# Databricks notebook source
orders = spark.table('workspace.bronze.orders')
customers = spark.table('workspace.bronze.customers')
order_items = spark.table("workspace.bronze.order_items")
products = spark.table("workspace.bronze.products")
sellers = spark.table("workspace.bronze.sellers")
payments = spark.table("workspace.bronze.payments")
reviews = spark.table("workspace.bronze.reviews")
category_translation = spark.table('workspace.bronze.category_translation')

print(orders.count(), customers.count(), order_items.count(), products.count(), sellers.count(), payments.count(), reviews.count(), category_translation.count())

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, trim, lower

ordersClean = orders.dropDuplicates(['order_id'])
ordersClean = (
    ordersClean
    .withColumn('order_purchase_timestamp', to_timestamp(col('order_purchase_timestamp')))
    .withColumn('order_delivered_customer_date', to_timestamp(col('order_delivered_customer_date')))
    .withColumn('order_estimated_delivery_date', to_timestamp(col('order_estimated_delivery_date')))
)
ordersClean = ordersClean.filter(col('order_purchase_timestamp').isNotNull())

print('before cleaning:', orders.count(), '\nafter cleaning:', ordersClean.count())

# COMMAND ----------

productsClean = (
    products.dropDuplicates(['product_id'])
    .withColumn('product_category_name', trim(lower(col('product_category_name'))))
)

print('before cleaning:', products.count(), '\nafter cleaning:', productsClean.count())

# COMMAND ----------

customersClean = customers.dropDuplicates(['customer_id'])

print('before cleaning:', customers.count(), '\nafter cleaning:', customersClean.count())

# COMMAND ----------

productsTranslated = (
    productsClean
    .join(category_translation, on='product_category_name', how='left')
    .select(
        'product_id',
        col('product_category_name_english').alias('category_eng'),
        'product_weight_g'
    )
)

order_itemsClean = order_items.dropDuplicates(['order_id', 'order_item_id'])

silverOrders = (
    ordersClean
    .join(order_itemsClean, on='order_id', how='inner')
    .join(productsTranslated, on='product_id', how='left')
    .join(customersClean, on='customer_id', how='left')
    .select(
        "order_id",
        "customer_id",
        "customer_state",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "order_status",
        "product_id",
        "category_eng",
        "price",
        "freight_value"
    )
)    

display(silverOrders)
print('rows after merge:', silverOrders.count())

# COMMAND ----------

from pyspark.sql.functions import datediff

silverOrders = (
    silverOrders
    .withColumn('delivery_days', datediff(col('order_delivered_customer_date'), col('order_purchase_timestamp')))
)
display(silverOrders.select('order_id','order_purchase_timestamp', 'order_delivered_customer_date', 'delivery_days'))

# COMMAND ----------

spark.sql('CREATE DATABASE IF NOT EXISTS workspace.silver')

(
    silverOrders.write
    .format('delta')
    .mode('overwrite')
    .saveAsTable('workspace.silver.orders_full')
)

print('saved workspace.silver.orders_full')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT category_eng, COUNT(*)
# MAGIC FROM workspace.silver.orders_full
# MAGIC WHERE category_eng IS NOT NULL
# MAGIC GROUP BY category_eng
# MAGIC ORDER BY COUNT(*) DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Gold: Revenue by Category
from pyspark.sql.functions import sum as spark_sum, avg, count, round as spark_round

gold_revenue_by_category = (
    silverOrders
    .filter(col('order_status')=='delivered')
    .groupBy('category_eng')
    .agg(
        spark_round(spark_sum('price'), 2).alias('total_revenue'),
        count('order_id').alias('orders_quantity'),
        spark_round(avg('price'), 2).alias('avg_order_value')
    )
    .orderBy(col('total_revenue').desc())
)

display(goldRevenueByCategory)

# COMMAND ----------

from pyspark.sql.functions import date_trunc

gold_revenue_by_month = (
    silverOrders
    .filter(col('order_status')=='delivered')
    .withColumn('order_month', date_trunc('month', col('order_purchase_timestamp')))
    .groupBy('order_month')
    .agg(
        spark_round(spark_sum('price'), 2).alias('total_revenue'),
        count('order_id').alias('orders_quantity')
    )
)

display(goldRevenueByMonth)

# COMMAND ----------

gold_delivery_performance = (
    silverOrders
    .filter(col('order_status')=='delivered')
    .filter(col('delivery_days').isNotNull())
    .groupBy('customer_state')
    .agg(
        spark_round(spark_sum('delivery_days'), 1).alias('avg_deliver_days'),
        count('order_id').alias('orders_quanity')
    )
    .orderBy(col('avg_deliver_days').desc())
)

display(gold_delivery_performance)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

state_window = Window.orderBy(col('total_revenue').desc())

gold_revenue_by_state = (
    silverOrders
    .filter(col('order_status')=='delivered')
    .groupBy('customer_state')
    .agg(spark_round(spark_sum('price'), 2).alias('total_revenue'))
    .withColumn('rank', rank().over(state_window))
    .orderBy('rank')
)

display(gold_revenue_by_state)

# COMMAND ----------

spark.sql('CREATE DATABASE IF NOT EXISTS workspace.gold')

tablesToSave = {
    'revenue_by_category': gold_revenue_by_category,
    "revenue_by_month": gold_revenue_by_month,
    "delivery_performance": gold_delivery_performance,
    "revenue_by_state": gold_revenue_by_state
}

for name, df in tablesToSave.items():
    df.write.format('delta').mode('overwrite').saveAsTable(f'workspace.gold.{name}')
    print(f'saved workspace.gold.{name}')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN workspace.silver;
# MAGIC SHOW TABLES IN workspace.gold;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.revenue_by_category
# MAGIC LIMIT 10;