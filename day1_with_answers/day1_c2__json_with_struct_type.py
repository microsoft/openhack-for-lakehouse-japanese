# Databricks notebook source
# MAGIC %md
# MAGIC ## Challenge2. 階層（Struct）型のカラムを保持したJSONファイルの取り込み
# MAGIC Q1. STRUCT型を含むデータを処理してください。<br>
# MAGIC Q2. Bronzeテーブルへデータの書き込みを実施してください。<br>
# MAGIC Q3. Silverテーブルへデータの書き込みを実施してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="c_2"

# COMMAND ----------

# ソースファイルのパスを指定
src_file_path__c_2 = f"{src_file_dir__c_2}/*"

tgt_table_name__c_2__bronze = 'olist_sellers_dataset_bronze'
tgt_table_name__c_2__silver = 'olist_sellers_dataset'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. STRUCT型を含むデータを処理してください。
# MAGIC 
# MAGIC ＜参考記事＞
# MAGIC 
# MAGIC - [JSON Files - Spark 3.2.1 Documentation (apache.org)](https://spark.apache.org/docs/latest/sql-data-sources-json.html)
# MAGIC - [pyspark.sql.DataFrameReader.json — PySpark 3.2.1 documentation (apache.org)](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json)

# COMMAND ----------

# ToDO 変数`src_file_path__c_2`からjsonファイルをデータフレームに読み込む
df = (spark
        .read
        .format('json')
        .load(src_file_path__c_2)
     )

df.display()

# COMMAND ----------

# ToDo `seller`列にある`city`の項目のみを表示するデータフレームを表示してください。
df.select('seller.city').display()

# COMMAND ----------

# ToDO すべてのカラムのデータ型を文字列としたデータフレームを作成してください。
df = (spark
        .read
        .format('json')
        .option('primitivesAsString', True)
        .load(src_file_path__c_2)
     )

df.display()

# COMMAND ----------

# すべてのカラムのデータ型が文字列となっていることを確認
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. Bronzeテーブルへデータの書き込みを実施してください。
# MAGIC 
# MAGIC 参考リンク
# MAGIC - [Databricks（Spark）にてScalaの関数で取得可能なDDL文字列をPythonで取得する方法 - Qiita](https://qiita.com/manabian/items/4908f77a4da2c040cd6a)

# COMMAND ----------

# ToDo 変数`tgt_table_name__c_2__bronze`をテーブル名として、すべてのカラムを文字列で保持したBronzeテーブルを作成してください。
# `_datasource`列と`_ingest_timestamp`列も保持してください。
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__c_2__bronze}
(
  
  seller_id STRING,
  seller STRUCT <
    `city`: STRING, 
    `state`: STRING,
    `zip_code_prefix`: STRING
  >,
  _datasource STRING,
  _ingest_timestamp timestamp
)
USING delta
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 1
  )
""")

# COMMAND ----------

from  pyspark.sql.functions import input_file_name,current_timestamp

# ToDO すべてのカラムのデータ型を文字列としたデータフレームを作成してください。
df = (spark
        .read
        .format('json')
        .option('primitivesAsString', True)
        .load(src_file_path__c_2)
     )


# `_datasource`列と`_ingest_timestamp`列を追加
df = (df
        .withColumn("_datasource", input_file_name())
        .withColumn("_ingest_timestamp", current_timestamp())
     )

# ターゲットのテーブルへ`append`によりデータを書き込む
(df.write
   .format('delta')
   .mode('append')
   .option("mergeSchema", "true")
   .saveAsTable(tgt_table_name__c_2__bronze)
)

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__c_2__bronze))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. Silverテーブルへデータの書き込みを実施してください。
# MAGIC 
# MAGIC Silverテーブルでは、Struct型でデータを保持していないことに注意してください。

# COMMAND ----------

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__c_2__silver}
(  
  `seller_id` STRING,
  `seller_zip_code_prefix` INT,
  `seller_city` STRING,
  `seller_state` STRING,
  _ingest_timestamp timestamp
)
USING delta
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 1
  )
""")

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp,lit

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. ブロンズテーブルから主キー（`seller_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
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

# ToDo dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['seller_id'])

# ToDo 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施。
## 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__c_2__silver}'
df.createOrReplaceTempView(temp_view_name)

## Merge処理を実行
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


# COMMAND ----------

# データが書き込まれたことを確認
spark.table(tgt_table_name__c_2__silver).display()
