# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

src_table_name__3_4 = 'olist_orders_dataset'
tgt_table_name__3_4 = 'monthly_sales_counts'

# COMMAND ----------

# `olist_orders_dataset`テーブルから下記の処理を行ったデータフレーム（df）を作成してください。
## `order_delivered_carrier_date`列がNULLでないレコードを抽出してください。
## `order_delivered_carrier_date`列を`yyyyMM`（例:201801）形式に変換して、`sales_yearmonth`として定義してください。
## `order_yearmonth`列で集計を行い、`order_id`列の重複排除したカウント数を`sales_counts`列として定義してください。
slv_to_gld_sql = f"""
with src (

SELECT
  od.order_id,
  date_format(od.order_delivered_carrier_date, "yMM") AS sales_yearmonth
  
  FROM
    {src_table_name__3_4} od
    
  WHERE od.order_status IN ("delivered", "shipped")
  AND od.order_delivered_carrier_date IS NOT NULL
    
), agg (

SELECT
    sales_yearmonth,
    COUNT(DISTINCT order_id) AS sales_counts --受注件数
  FROM
    src
  GROUP BY
    sales_yearmonth
)

SELECT
  *
  FROM
    agg
"""
df = spark.sql(slv_to_gld_sql)

# 一時ビューを作成
tmp_view_name = f'_tmp_{tgt_table_name__3_4}'
df.createOrReplaceTempView(tmp_view_name)

# ToDo CTAS（CREAT TABLE AS SLECT）により、`monthly_sales_counts`テーブルを作成
ctas_sql = f'''
create or replace table {tgt_table_name__3_4}
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

# Vacuumを実行
spark.sql(f'''
VACUUM {tgt_table_name__3_4}
''')

# Analyze Tableを実行。`sales_yearmonth`列が対象。
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_4}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      sales_yearmonth
''')
