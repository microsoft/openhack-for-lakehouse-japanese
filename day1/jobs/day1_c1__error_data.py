# Databricks notebook source
# MAGIC %md
# MAGIC ## Challenge1. タイムトラベル機能による誤ったデータの取り込みを修正
# MAGIC Q1.`olist_order_items_dataset`データの取り込みを実施してください。<br>
# MAGIC Q2. 誤って取り込んだデータを削除してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="c_1"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1.`olist_order_items_dataset`データの取り込みを実施してください。

# COMMAND ----------

tgt_table_name__c_1__bronze = 'olist_order_items_dataset_bronze'
tgt_table_name__c_1__silver = 'olist_order_items_dataset'

# COMMAND ----------

# Bronzeテーブルを作成
spark.sql(f"""
DROP TABLE IF EXISTS {tgt_table_name__c_1__bronze}
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__c_1__bronze}
(
    `order_id` STRING,
    `order_item_id` STRING,
    `_ingest_timestamp` timestamp,
    `product_id` STRING,
    `seller_id` STRING,
    `shipping_limit_date` STRING,
    `price` STRING,
    `freight_value` STRING,
    `_datasource` STRING
)
USING delta
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = True, 
  delta.autoOptimize.autoCompact   = True,
  delta.dataSkippingNumIndexedCols = 3
)
""")

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__c_1__silver}
(
    `order_id` STRING,
    `order_item_id` INT,
    `product_id` STRING,
    `seller_id` STRING,
    `shipping_limit_date` STRING,
    `price` DOUBLE,
    `freight_value` DOUBLE,
    `_ingest_timestamp` timestamp
)
USING delta
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = True, 
  delta.autoOptimize.autoCompact   = True,
  delta.dataSkippingNumIndexedCols = 2
)
""")

# COMMAND ----------

# 初回データの配置データのファイルパスを指定
src_file_path__c_1 = f"{src_file_path__c_1__first}"

# COMMAND ----------

# ToDO `src_file_path__c_1`のデータを、`tgt_table_name__c_1__bronze`テーブルに書き込んでください。
from  pyspark.sql.functions import input_file_name,current_timestamp

df = (spark
        .read
        .format("csv")
        .option("header","true")
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
     .mode("append")
     .saveAsTable(tgt_table_name__c_1__bronze)
)

# COMMAND ----------

# 2回目データの配置データのファイルパスを指定
src_file_path__c_1 = f"{src_file_path__c_1__second}/*"

# COMMAND ----------

# ToDO `src_file_path__c_1`のデータを、`tgt_table_name__c_1__bronze`テーブルに再度書き込んでください。
from  pyspark.sql.functions import input_file_name,current_timestamp

df = (spark
        .read
        .format("csv")
        .option("header","true")
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
     .mode("append")
     .saveAsTable(tgt_table_name__c_1__bronze)
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. 誤って取り込んだデータを削除してください。

# COMMAND ----------

## ToDo 2回目の書き込み前に、タイムトラベル機能により戻してください。
# `order_id`列に`test`となっているテストレコードを追加されてしまったようです。
spark.table(tgt_table_name__c_1__bronze).filter('order_id = "test"').display()

# COMMAND ----------

## ToDo 2回目の書き込み前に、タイムトラベル機能により戻してください。
spark.sql(f'''RESTORE TABLE {tgt_table_name__c_1__bronze} TO VERSION AS OF 1''')

# COMMAND ----------

## Answer テーブルの履歴を確認
spark.sql(f'desc history {tgt_table_name__c_1__bronze}').display()

# COMMAND ----------

# `order_id`列に`test`となっているレコードが存在しないことを確認
spark.table(tgt_table_name__c_1__bronze).filter('order_id = "test"').display()

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp,lit

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. ブロンズテーブルから主キー（`order_id`、`order_item_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''select bronze2.* from (select order_id,order_item_id, max(_ingest_timestamp) as _ingest_timestamp from {tgt_table_name__c_1__bronze} group by order_id,order_item_id) bronze1
inner join (select * from {tgt_table_name__c_1__bronze}) bronze2 on (bronze1.order_id = bronze2.order_id and bronze1.order_item_id = bronze2.order_item_id and bronze1._ingest_timestamp = bronze2._ingest_timestamp)'''

df = spark.sql(brz_to_slv_sql)

# ToDo dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['order_id','order_item_id'])

# 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__c_1__silver}'
df.createOrReplaceTempView(temp_view_name)


# ToDo 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行されるようにしてください。
spark.sql(f'''
MERGE INTO {tgt_table_name__c_1__silver} as tgt
using {temp_view_name} as src
on tgt.order_id = src.order_id and tgt.order_item_id = src.order_item_id
when matched and tgt._ingest_timestamp < src._ingest_timestamp 
then update set *
when not matched then insert *
''')


# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__c_1__silver))

# COMMAND ----------

# 112,650レコードであることを確認
print(spark.table(tgt_table_name__c_1__silver).count())
