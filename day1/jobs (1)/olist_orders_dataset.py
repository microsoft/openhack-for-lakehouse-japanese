# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

src_table_name__3_3 = 'olist_orders_dataset_bronze'
tgt_table_name__3_3 = 'olist_orders_dataset'

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp,lit

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. ブロンズテーブルから主キー（`order_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''
with slv_records (
  SELECT
    order_id,
    MAX(_ingest_timestamp) AS max_ingest_timestamp
    
    FROM
      {src_table_name__3_3}
    
    GROUP BY
      order_id      
)

SELECT
  brz.`order_id`,
  brz.`customer_id`,
  brz.`order_status`,
  brz.`order_purchase_timestamp`::TIMESTAMP,
  brz.`order_approved_at`::TIMESTAMP,
  brz.`order_delivered_carrier_date`::TIMESTAMP,
  brz.`order_delivered_customer_date`::TIMESTAMP,
  brz.`order_estimated_delivery_date`::TIMESTAMP,
  _ingest_timestamp
  
  FROM
    {src_table_name__3_3} AS brz
  INNER JOIN 
    slv_records AS slv
    ON 
      brz.order_id =  slv.order_id
      AND brz._ingest_timestamp =  slv.max_ingest_timestamp
'''
df = spark.sql(brz_to_slv_sql)

# ToDo dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['order_id'])


# 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__3_3}'
df.createOrReplaceTempView(temp_view_name)


# ToDo 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行されるようにしてください。
spark.sql(f'''
MERGE INTO {tgt_table_name__3_3} AS tgt
  USING {temp_view_name} AS src
  
  ON tgt.order_id = src.order_id
  
  WHEN MATCHED
  AND tgt._ingest_timestamp < src._ingest_timestamp
    THEN UPDATE SET *
  WHEN NOT MATCHED
    THEN INSERT *
''')

# Vacuumを実行
spark.sql(f'''
VACUUM {tgt_table_name__3_3}
''')

# ToDo Analyze Tableを実行。`order_id`列が対象。
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_3}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      order_id
''')

