# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 03. メダリオンアーキテクチャ構築の実践 - Bronze - (目安 12:30~13:00 + 14:15~14:45)
# MAGIC ### 本ノートブックの目的：DatabricksにおけるBronze Tableの役割・取り扱いについて理解を深める
# MAGIC Q1. Bronzeテーブルのパイプラインを作成してください。<br>
# MAGIC Q2. 半構造化データをbronzeテーブルとして読み込んでください。<br>
# MAGIC Q3. deltaのタイムトラベル機能による誤ったデータの取り込みを修正してください。<br>
# MAGIC C1. その他ファイルをソースとしたブロンズテーブルへのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="3"

# COMMAND ----------

# MAGIC %md **Data Overview**
# MAGIC 
# MAGIC 店舗データを読み込み、プロファイリング、delta table化、メダリオンアーキテクチャーにそった形でダッシュボードと機械学習用に使えるデータに整形しましょう!
# MAGIC 
# MAGIC 今回利用するデータセットの関連図です。
# MAGIC 
# MAGIC <br>
# MAGIC <img src='https://github.com/skotani-db/databricks-hackathon-jp/raw/main/images/olist_data_relation.png' width='800' />
# MAGIC </br>
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC | データ名 | 内容 |
# MAGIC | - | - |
# MAGIC | olist_customers_dataset.csv | このデータセットには、顧客とその所在地に関する情報が含まれています。これを用いて、受注データセットに含まれる固有の顧客を特定したり、 受注の配送先を探したりします。私たちのシステムでは、各注文に一意な customerid が割り当てられています。つまり、同じ顧客でも注文によって異なる ID が与えられるということです。データセットに customerunique_id を設定する目的は、その店舗で再購入した顧客を識別できるようにするためです。そうでなければ、それぞれの注文に異なる顧客が関連付けられていることに気づくでしょう。 |
# MAGIC | olist_geolocation_dataset.csv | このデータセットには、ブラジルの郵便番号とその緯度経度座標の情報が含まれている。地図を作成したり、売り手と顧客の間の距離を調べるのに使用します。|
# MAGIC | olist_order_items_dataset.csv | このデータセットには、各注文の中で購入された商品に関するデータが含まれています。 |
# MAGIC | olist_order_payments_dataset.csv | このデータセットには、注文の支払いオプションに関するデータが含まれています。|
# MAGIC | olist_order_reviews_dataset.csv | このデータセットには、顧客によるレビューに関するデータが含まれている。顧客がOlist Storeで製品を購入すると、販売者にその注文を履行するよう通知される。顧客が製品を受け取ると、あるいは配送予定日になると、顧客は満足度アンケートをメールで受け取り、購入体験のメモやコメントを書き込むことができます。|
# MAGIC | olist_orders_dataset.csv | これがコアとなるデータセットです。各注文から他のすべての情報を見つけることができるかもしれません。 |
# MAGIC | olist_products_dataset.csv | このデータセットには、Olistが販売する製品に関するデータが含まれています。 |
# MAGIC | olist_sellers_dataset.json | このデータセットには、Olistで行われた注文を処理した販売者のデータが含まれています。販売者の所在地を調べたり、どの販売者が各商品を販売したかを特定するために使用します。 |
# MAGIC | product_category_name_translation.csv | productcategorynameを英語に翻訳します。 |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Bronzeテーブルのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 実践例

# COMMAND ----------

# CSV の中身をチェック
data = dbutils.fs.head(f"{datasource_dir}/olist_orders_dataset.csv", 700)
print(data)

# COMMAND ----------

src_file_path__3_1_1 = f"{datasource_dir}/olist_orders_dataset*.csv"
tgt_table_name__3_1_1 = "olist_orders_dataset_bronze"

# COMMAND ----------

# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_1_1}
(
    `order_id` STRING,
    `customer_id` STRING,
    `order_status` STRING,
    `order_purchase_timestamp` STRING,
    `order_approved_at` STRING,
    `order_delivered_carrier_date` STRING,
    `order_delivered_customer_date` STRING,
    `order_estimated_delivery_date` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

# COMMAND ----------

# 現在のテーブル定義を確認
spark.sql(f"""DESC EXTENDED {tgt_table_name__3_1_1}""").display()

# COMMAND ----------

# ソースからデータを読み込む
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_1_1)
)

# 監査列として`_datasource`列と`_ingest_timestamp`列を追加
df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

# ターゲットのテーブルへ`append`によりデータの書き込みを実施してください。
(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_1_1)
)

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_1_1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### ToDo **`olist_order_items_dataset.csv`** をソースとして **`olist_order_items_dataset_bronze`** テーブルへデータを書き込むパイプラインを作成してください。

# COMMAND ----------

tgt_table_name__3_1_2 = "olist_order_items_dataset_bronze"
src_file_path__3_1_2 = f"{datasource_dir}/olist_order_items_dataset*.csv"

# COMMAND ----------

# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_1_2}
(
    `order_id` STRING,
    `order_item_id` STRING,
    `product_id` STRING,
    `seller_id` STRING,
    `shipping_limit_date` STRING,
    `price` STRING,
    `freight_value` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

# COMMAND ----------

# 現在のテーブル定義を確認
spark.sql(f"""DESC EXTENDED {tgt_table_name__3_1_2}""").display()

# COMMAND ----------

# ToDo `src_file_path__3_1_2`変数をソースとしてデータを読み込む
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_1_2)
)

# ToDo 監査列として`_datasource`列と`_ingest_timestamp`列を追加
df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

# ToDo `tgt_table_name__3_1_2`変数のテーブルへ`append`によりデータの書き込みを実施してください。
(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_1_2)
)

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_1_2))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 本ノートブック以降で利用するテーブルへの書き込みを実施

# COMMAND ----------

# `olist_order_reviews_dataset_bronze` テーブルへ書き込み
tgt_table_name__3_1_3 = "olist_order_reviews_dataset_bronze"
tgt_table_schema__3_1_3 = """
`review_id` STRING,
`order_id` STRING,
`review_score` STRING,
`review_comment_title` STRING,
`review_comment_message` STRING,
`review_creation_date` STRING,
`review_answer_timestamp` STRING
"""
src_file_path__3_1_3 = f"{datasource_dir}/olist_order_reviews_dataset*.csv"

create_bronze_tbl(tgt_table_name__3_1_3, tgt_table_schema__3_1_3)
write_to_bronze_tbl(src_file_path__3_1_3, tgt_table_name__3_1_3)

spark.table(tgt_table_name__3_1_3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. 半構造化データをbronzeテーブルとして読み込む

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC json ファイルをソースとした spark データフレームの作成の実践
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC - [JSON Files - Spark 3.2.1 Documentation (apache.org)](https://spark.apache.org/docs/latest/sql-data-sources-json.html)
# MAGIC - [pyspark.sql.DataFrameReader.json — PySpark 3.2.1 documentation (apache.org)](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json)

# COMMAND ----------

# json ファイルの中身を確認
import pprint

sample_file_path = (
    "/databricks-datasets/learning-spark-v2/flights/summary-data/json/2010-summary.json"
)
json_data = dbutils.fs.head(sample_file_path)

pprint.pprint(json_data)

# COMMAND ----------

# `primitivesAsString` オプションを `True` にすることで文字列として読み込むことが可能
file_path = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
df = spark.read.format("json").option("primitivesAsString", True).load(file_path)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ToDo : 
# MAGIC 出品業者の情報である **`sellers_bronze`** テーブルはjson形式のファイルをロードして作成します。<br>
# MAGIC jsonは入れ子構造のデータ型です。jsonのスキーマはPySpark DataframeではSTRUCT型を用いて表現します。<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ToDo : json 形式に変換した **olist_sellers_dataset** をソースとした **`olist_sellers_dataset_bronze`** テーブルへのデータを書き込むパイプラインを作成してください。

# COMMAND ----------

tgt_table_name__3_2_1 = "olist_sellers_dataset_bronze"
src_file_path__3_2_1 = src_file_dir__c_2

# COMMAND ----------

spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_2_1}
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
"""
)

# COMMAND ----------

# ToDo すべてのカラムのデータ型を文字列としたデータフレームを作成してください
df = (
    spark.read.format("json").option("primitivesAsString", True).load(src_file_dir__c_2)
)
# 監査列として`_datasource`列と`_ingest_timestamp`列を追加
df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

# COMMAND ----------

# ターゲットのテーブルへデータフレームから`append`によりデータを書き込む
(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_2_1)
)

# COMMAND ----------

# データを確認
spark.table(tgt_table_name__3_2_1).display()

# COMMAND ----------

# ToDo  `seller`列にある`city`の項目のみを表示するデータを表示してください。
spark.sql(
    f"""
SELECT
  seller.city
  FROM 
    {tgt_table_name__3_2_1}
"""
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. deltaのタイムトラベル機能による誤ったデータの取り込みを修正

# COMMAND ----------

# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE olist_customers_dataset_bronze
(
    customer_id string,
    customer_unique_id string,
    customer_zip_code_prefix string,
    customer_city string,
    customer_state string,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

# COMMAND ----------

# DBTITLE 1,ダミーデータをcustomers_bronzeへ書き込む
# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   olist_customers_dataset_bronze
# MAGIC VALUES
# MAGIC   (
# MAGIC     'test',
# MAGIC     'test',
# MAGIC     1060032,
# MAGIC     'roppongi',
# MAGIC     'JP',
# MAGIC     'abc',
# MAGIC     CAST('2020-02-01' AS timestamp)
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,dataが書き込まれたことを確認
# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   olist_customers_dataset_bronze
# MAGIC WHERE
# MAGIC   customer_id = 'test'

# COMMAND ----------

# DBTITLE 1,レコードがWRITEされた履歴を確認し、その直前のバージョン番号を控える
# MAGIC %sql DESC HISTORY olist_customers_dataset_bronze

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- ToDo : testレコードがWRITEされていないバージョンに戻す
# MAGIC -- https://qiita.com/taka_yayoi/items/3b2095825a7e48b86f69
# MAGIC -- <<FILL IN>>
# MAGIC RESTORE TABLE olist_customers_dataset_bronze TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- testレコードがないテーブルのバージョンへ戻ったことを確認する
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   olist_customers_dataset_bronze
# MAGIC WHERE
# MAGIC   customer_id = 'test'

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1. その他ファイルをソースとした Bronze テーブルへのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ToDo 未取り込みである下記のファイルをソースとした Bronze テーブルへのパイプラインを作成してください。
# MAGIC 
# MAGIC - olist_customers_dataset.csv
# MAGIC - olist_geolocation_dataset.csv
# MAGIC - olist_order_payments_dataset.csv
# MAGIC - olist_products_dataset.csv
# MAGIC - product_category_name_translation.csv

# COMMAND ----------

# ファイルを確認
display(dbutils.fs.ls(datasource_dir))

# COMMAND ----------

# ToDo `olist_customers_dataset_bronze` テーブルへ書き込み
tgt_table_name__3_c_1 = "olist_customers_dataset_bronze"
src_file_path__3_c_1 = f"{datasource_dir}/olist_customers_dataset*.csv"

# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_c_1}
(
    customer_id string,
    customer_unique_id string,
    customer_zip_code_prefix string,
    customer_city string,
    customer_state string,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_c_1)
)

df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_c_1)
)

spark.table(tgt_table_name__3_c_1).display()

# COMMAND ----------

# Todo `olist_geolocation_dataset_bronze` テーブルへ書き込み
tgt_table_name__3_c_2 = "olist_geolocation_dataset_bronze"
src_file_path__3_c_2 = f"{datasource_dir}/olist_geolocation_dataset*.csv"


# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_c_2}
(
    `geolocation_zip_code_prefix` STRING,
    `geolocation_lat` STRING,
    `geolocation_lng` STRING,
    `geolocation_city` STRING,
    `geolocation_state` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_c_2)
)

df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_c_2)
)

spark.table(tgt_table_name__3_c_2).display()

# COMMAND ----------

# Todo `olist_order_payments_dataset_bronze` テーブルへ書き込み
tgt_table_name__3_c_3 = "olist_order_payments_dataset_bronze"
src_file_path__3_c_3 = f"{datasource_dir}/olist_order_payments_dataset*.csv"


# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_c_3}
(
    `order_id` STRING,
    `payment_sequential` STRING,
    `payment_type` STRING,
    `payment_installments` STRING,
    `payment_value` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_c_3)
)

df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_c_3)
)

spark.table(tgt_table_name__3_c_3).display()

# COMMAND ----------

# ToDo `olist_products_dataset_bronze` テーブルへ書き込み
tgt_table_name__3_c_4 = "olist_products_dataset_bronze"
tgt_table_schema__3_c_4 = """
"""
src_file_path__3_c_4 = f"{datasource_dir}/olist_products_dataset*.csv"


# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_c_4}
(
    `product_id` STRING,
    `product_category_name` STRING,
    `product_name_lenght` STRING,
    `product_description_lenght` STRING,
    `product_photos_qty` STRING,
    `product_weight_g` STRING,
    `product_length_cm` STRING,
    `product_height_cm` STRING,
    `product_width_cm` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_c_4)
)

df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_c_4)
)

spark.table(tgt_table_name__3_c_4).display()

# COMMAND ----------

# ToDo `product_category_name_translation_bronze` テーブルへ書き込み
tgt_table_name__3_c_5 = "product_category_name_translation_bronze"
src_file_path__3_c_5 = f"{datasource_dir}/product_category_name_translation*.csv"


# Bronzeテーブルを作成
spark.sql(
    f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_c_5}
(
    `product_category_name` STRING,
    `product_category_name_english` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
"""
)

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "False")
    .load(src_file_path__3_c_5)
)

df = (
    df.select("*", "_metadata")
    .withColumn("_datasource", df["_metadata.file_path"])
    .withColumn("_ingest_timestamp", df["_metadata.file_modification_time"])
    .drop("_metadata")
)

(
    df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(tgt_table_name__3_c_5)
)

spark.table(tgt_table_name__3_c_5).display()
