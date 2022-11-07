# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

dbutils.widgets.text("src_file_path", "/FileStore/db_openhackason_2022/day1/team/src/c_1/first/*")
src_file_path__c_1 = dbutils.widgets.get("src_file_path")

# COMMAND ----------

# # 初回データの配置データのファイルパスを指定
# src_file_path__c_1 = f"{src_file_path__c_1__first}"

# COMMAND ----------

tgt_table_name__c_1__bronze = 'olist_order_items_dataset_bronze'
tgt_table_name__c_1__silver = 'olist_order_items_dataset'

# COMMAND ----------

# ToDO `src_file_path__c_1`のデータを、`tgt_table_name__c_1__bronze`テーブルに書き込んでください。
from  pyspark.sql.functions import input_file_name,current_timestamp

df = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "False")
        .load(src_file_path__c_1)
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
     .saveAsTable(tgt_table_name__c_1__bronze)
)
