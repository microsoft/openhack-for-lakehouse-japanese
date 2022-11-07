# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

dbutils.widgets.text("src_file_path", "/FileStore/db_openhackason_2022/day1/team/src/c_2/*")
src_file_path__c_2 = dbutils.widgets.get("src_file_path")

# COMMAND ----------

# # ソースファイルのパスを指定
# src_file_path__c_2 = f"{src_file_dir__c_2}/*"

tgt_table_name__c_2__bronze = 'olist_sellers_dataset_bronze'
tgt_table_name__c_2__silver = 'olist_sellers_dataset'

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
     .saveAsTable(tgt_table_name__c_2__bronze)
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/db_openhackason_2022/day1/team/src/c2
