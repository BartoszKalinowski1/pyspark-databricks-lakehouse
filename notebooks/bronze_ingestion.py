# Databricks notebook source
basePath = '/Volumes/workspace/bronze/data_raw/'

tables = {
    "orders": "olist_orders_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "category_translation": "product_category_name_translation.csv"
}

dataFrames = {}

for name, fileName in tables.items():
    df = spark.read.option('header', 'true').option('inferSchema', 'true').csv(basePath+fileName)
    dataFrames[name] = df
    print(f"{name}: {df.count()} rows, {len(df.columns)} columns.")

# COMMAND ----------

display(dataFrames['orders'])
dataFrames['orders'].printSchema()

# COMMAND ----------

spark.sql('CREATE DATABASE IF NOT EXISTS bronze')

for name, df in dataFrames.items():
    df.write.format('delta').mode('overwrite').saveAsTable(f'bronze.{name}')
    print(f'saved table bronze.{name}')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bronze;
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM bronze.orders
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT COUNT(*) 
# MAGIC FROM bronze.orders;