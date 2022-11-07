# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

tgt_table_name__c_3 = 'daily_order_amounts'

# COMMAND ----------

# ToDo 、日別受注金額を保持したゴールドテーブルを作成してください。
slv_to_gld_sql = f"""
with src (

SELECT
  od.order_id,
  cast(od.order_delivered_carrier_date as date) AS order_delivered_carrier_date,
  
  oid.product_id,
  oid.price,
  oid.freight_value

  FROM
    olist_order_items_dataset oid
  
    INNER JOIN olist_orders_dataset od
    ON oid.order_id = od.order_id
  
  WHERE
    od.order_delivered_carrier_date IS NOT NULL
    AND od.order_status IN ("delivered", "shipped")
    
), agg (

SELECT
    order_delivered_carrier_date,
    SUM(price+freight_value) AS sales_amount --受注金額
  FROM
    src
  GROUP BY
    order_delivered_carrier_date      
)

SELECT
  *
  FROM
    agg
"""
df = spark.sql(slv_to_gld_sql)

# 一時ビューを作成
tmp_view_name = f'_tmp_{tgt_table_name__c_3}'
df.createOrReplaceTempView(tmp_view_name)

# ToDo CTAS（CREAT TABLE AS SLECT）により、`monthly_sales_counts`テーブルを作成
ctas_sql = f'''
create or replace table {tgt_table_name__c_3}
  using delta
  TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 1
  )
  AS 
  select 
    * 
    from 
      {tmp_view_name}
'''
spark.sql(ctas_sql)

