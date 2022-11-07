# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

tgt_table_name__c_1__bronze = 'olist_order_items_dataset_bronze'
tgt_table_name__c_1__silver = 'olist_order_items_dataset'

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp,lit

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. ブロンズテーブルから主キー（`order_id`、`order_item_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''
with slv_records (
  SELECT
    order_id,
    order_item_id::INT,
    MAX(_ingest_timestamp) AS max_ingest_timestamp
    
    FROM
      {tgt_table_name__c_1__bronze}
    
    GROUP BY
      order_id,
      order_item_id::INT
)

SELECT
  brz.`order_id`,
  brz.`order_item_id`::INT,
  brz.`product_id`,
  brz.`seller_id`,
  brz.`shipping_limit_date`,
  brz.`price`::DOUBLE,
  brz.`freight_value`::DOUBLE,
  _ingest_timestamp
  
  FROM
    {tgt_table_name__c_1__bronze} AS brz
  INNER JOIN 
    slv_records AS slv
    ON 
      brz.order_id =  slv.order_id
      AND brz.order_item_id = slv.order_item_id
      AND brz._ingest_timestamp =  slv.max_ingest_timestamp
'''
df = spark.sql(brz_to_slv_sql)

# ToDo dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['order_id', 'order_item_id'])


# 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__c_1__silver}'
df.createOrReplaceTempView(temp_view_name)


# ToDo 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行されるようにしてください。
spark.sql(f'''
MERGE INTO {tgt_table_name__c_1__silver} AS tgt
  USING {temp_view_name} AS src
  
  ON tgt.order_id = src.order_id
  AND tgt.order_item_id = src.order_item_id
  
  WHEN MATCHED
  AND tgt._ingest_timestamp < src._ingest_timestamp
    THEN UPDATE SET *
  WHEN NOT MATCHED
    THEN INSERT *
''')

