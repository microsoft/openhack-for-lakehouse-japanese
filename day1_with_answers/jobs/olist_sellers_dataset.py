# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

tgt_table_name__c_2__bronze = 'olist_sellers_dataset_bronze'
tgt_table_name__c_2__silver = 'olist_sellers_dataset'

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp,lit

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. ブロンズテーブルから主キー（`order_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''
with slv_records (
  SELECT
    seller_id,
    MAX(_ingest_timestamp) AS max_ingest_timestamp
    
    FROM
      {tgt_table_name__c_2__bronze}
    
    GROUP BY
      seller_id      
)

SELECT
  brz.`seller_id`,
  brz.seller.city AS seller_city,
  brz.seller.state AS seller_state,
  brz.seller.zip_code_prefix::INT AS seller_zip_code_prefix,
  brz._ingest_timestamp
  
  FROM
    {tgt_table_name__c_2__bronze} AS brz
  INNER JOIN 
    slv_records AS slv
    ON 
      brz.seller_id =  slv.seller_id
      AND brz._ingest_timestamp =  slv.max_ingest_timestamp
'''
df = spark.sql(brz_to_slv_sql)

# dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['seller_id'])


# 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__c_2__silver}'
df.createOrReplaceTempView(temp_view_name)


# ToDo 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施。
spark.sql(f'''
MERGE INTO {tgt_table_name__c_2__silver} AS tgt
  USING {temp_view_name} AS src
  
  ON tgt.seller_id = src.seller_id
  
  WHEN MATCHED
  AND tgt._ingest_timestamp < src._ingest_timestamp
    THEN UPDATE SET *
  WHEN NOT MATCHED
    THEN INSERT *
''')

