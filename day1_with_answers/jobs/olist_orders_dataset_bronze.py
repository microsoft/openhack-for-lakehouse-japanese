# Databricks notebook source
spark.sql(f"USE db_open_hackason_day1_team")

# COMMAND ----------

dbutils.widgets.text("src_file_path", "/FileStore/db_openhackason_2022/day1/team/src/3_2/second/*")
src_file_path__3_2_2 = dbutils.widgets.get("src_file_path")

# COMMAND ----------

# src_file_path__3_2_2 = f"{src_file_path__3_2_2__first}/*"

# COMMAND ----------

tgt_table_name__3_2 = 'olist_orders_dataset_bronze'

# COMMAND ----------

from  pyspark.sql.functions import input_file_name,current_timestamp

df = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "False")
        .load(src_file_path__3_2_2)
     )

# `_datasource`列と`_ingest_timestamp`列を追加
df = (df
          .withColumn("_datasource", input_file_name())
          .withColumn("_ingest_timestamp", current_timestamp())
     )

# ToDO ターゲットのテーブルへ`append`によりデータを書き込む
(df.write
     .format('delta')
     .mode('append')
     .saveAsTable(tgt_table_name__3_2)
)

# ToDo Vacuumを実行
spark.sql(f'''
VACUUM {tgt_table_name__3_2}
''')

# ToDo Analyze Tableを実行。`order_id`列、および、`_ingest_timestamp`列が対象
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_2}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      order_id,
      _ingest_timestamp
''')

