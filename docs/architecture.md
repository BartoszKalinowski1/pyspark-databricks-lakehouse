# Architecture Notes

This document goes deeper than the README overview — it covers table schemas, specific design trade-offs, and alternatives that were considered but not implemented, given the scope of this project.

## Layer contracts

### Bronze — `workspace.bronze.*`

Raw ingestion, one table per source CSV, no transformations beyond `inferSchema`. Grain matches the source file exactly (e.g. `bronze.orders` = 1 row per order, `bronze.order_items` = 1 row per order line item).

| Table | Grain | Row count (approx.) |
|---|---|---|
| `orders` | 1 row per order | ~99,441 |
| `customers` | 1 row per customer | ~99,441 |
| `order_items` | 1 row per order line item | ~112,650 |
| `products` | 1 row per product | ~32,951 |
| `sellers` | 1 row per seller | ~3,095 |
| `payments` | 1 row per payment installment | ~103,886 |
| `reviews` | 1 row per review | ~99,224 |
| `category_translation` | 1 row per category | ~71 |

### Silver — `workspace.silver.orders_full`

Grain: **1 row per order item** (not per order), because it's built by joining `orders` with `order_items`, which is a 1-to-many relationship. This is a deliberate choice: keeping the item-level grain preserves the ability to aggregate by product/category later, at the cost of `order`-level columns being repeated across rows for multi-item orders.

Applied transformations:
- Deduplication on primary keys (`order_id`, `customer_id`, `product_id`) before any join, to prevent join fan-out.
- Timestamp casting (`order_purchase_timestamp`, `order_delivered_customer_date`, `order_estimated_delivery_date`) from string to `timestamp`.
- Category name translated to English via a left join with `category_translation` (left join, not inner — some products have no listed category, and dropping them would silently lose revenue from the aggregates).
- Derived `delivery_days` column (`datediff` between purchase and delivery timestamps).

### Gold — `workspace.gold.*`

Each table answers one specific business question and is pre-aggregated so the dashboard queries return instantly without recomputing joins.

| Table | Grain | Business question answered |
|---|---|---|
| `revenue_by_category` | 1 row per category | Which product categories drive the most revenue? |
| `revenue_by_month` | 1 row per month | Is revenue growing, and where are the seasonal peaks? |
| `delivery_performance` | 1 row per state | Which regions have slow fulfillment? |
| `revenue_by_state` | 1 row per state, ranked | Where should sales/marketing focus regionally? |

All gold tables filter to `order_status == 'delivered'` only — cancelled or in-transit orders are excluded from revenue figures to avoid overstating realized sales.

## Design decisions and trade-offs

**Full overwrite instead of incremental/streaming ingestion.** Each layer is rebuilt from scratch on every run (`mode("overwrite")`). This is the right choice for a static, one-time dataset like Olist, and is simple to reason about. In a production setting with continuously arriving data, this would be replaced by an incremental pattern — Databricks Auto Loader for bronze ingestion, and `MERGE INTO` (upserts) for silver/gold instead of full rewrites.

**`saveAsTable` (managed tables) instead of external tables with explicit storage paths.** Managed tables let Unity Catalog handle the underlying storage location, which is simpler for a single-developer project. A team environment would more likely use external tables pointing at a specific cloud storage path, to keep storage lifecycle independent of the catalog and support multi-workspace access to the same files.

**Import mode in Power BI instead of DirectQuery.** With ~100k rows total, importing a static snapshot into Power BI's in-memory model is faster to query and simpler to build against. DirectQuery would be the right call if the gold tables were updated frequently and the dashboard needed to reflect near-real-time state — but it also pushes every visual interaction back to the SQL Warehouse as a live query, adding latency for a dataset this size.

**Serverless Compute instead of a manually configured cluster.** Databricks Free Edition's Serverless Compute removes cluster sizing/startup decisions entirely, which was the right trade for a fast build. In a real enterprise setting, cluster configuration (node type, autoscaling, spot instances) is itself a cost/performance lever worth tuning — something to be aware of but out of scope here.

## Alternatives considered but not implemented

- **dbt instead of/alongside PySpark for silver/gold transformations** — dbt's SQL-based, tested, version-controlled models are a strong fit for the silver/gold layers specifically (as used in the separate `etl-sales-pipeline` project). PySpark was chosen here instead so the project would directly demonstrate PySpark/DataFrame API skills.
- **Orchestration via Databricks Workflows** — the two notebooks are currently run manually in sequence. Wiring them into a scheduled Workflow (with the bronze notebook as an upstream dependency of the silver/gold notebook) was scoped out to prioritize the transformation logic itself within the available time.
- **Data quality checks between layers** — no automated validation currently confirms that, e.g., all `order_id`s in silver are still present after joins, or that gold aggregates reconcile to source totals. This was a conscious scope cut, listed as a next step rather than something silently skipped.

## Known limitations

- No handling for late-arriving or updated source records (the dataset is static, so this doesn't currently matter, but it's a gap versus a production pipeline).
- Geolocation data (`bronze` candidate table `geolocation`) was not incorporated into silver/gold — the dashboard uses `customer_state` directly rather than lat/long-based analysis.
- No automated tests on the PySpark transformation logic itself (e.g. no unit tests asserting row counts or null handling post-join).