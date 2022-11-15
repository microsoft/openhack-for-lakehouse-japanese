# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 04. メダリオンアーキテクチャ構築の実践 - Silver Table - (目安 14:45~15:10)
# MAGIC ### 本ノートブックの目的：DatabricksにおけるSilverテーブルの役割・取り扱いについて理解を深める
# MAGIC Q1. Silver テーブルのパイプラインを作成してください <br>
# MAGIC Q2. 階層（Struct）型のカラムを保持した Bronze テーブルから Silver テーブルへ書き込むパイプラインを作成してください<br>
# MAGIC C1. その他 Bronze テーブルをソースとして Silver テーブルへのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="4"

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
# MAGIC ソースごとの主キーについては、 Kaggle にて確認してください。
# MAGIC 
# MAGIC - [Brazilian E-Commerce Public Dataset by Olist | Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Silver テーブルのパイプラインを作成してください

# COMMAND ----------

# MAGIC %md
# MAGIC #### 実践例

# COMMAND ----------

src_table_name__4_1_1 = 'olist_orders_dataset_bronze'
tgt_table_name__4_1_1 = 'olist_orders_dataset_silver'

# COMMAND ----------

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__4_1_1}
(
   `order_id` STRING,
    `customer_id` STRING,
    `order_status` STRING,
    `order_purchase_timestamp` TIMESTAMP,
    `order_approved_at` TIMESTAMP,
    `order_delivered_carrier_date` TIMESTAMP,
    `order_delivered_customer_date` TIMESTAMP,
    `order_estimated_delivery_date` TIMESTAMP,
    _ingest_timestamp timestamp
)
USING delta
""")

# COMMAND ----------

# 下記の処理を実行したデータフレーム（df）を作成
## 1. ブロンズテーブルから主キー（`order_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''
with slv_records (
SELECT
  order_id,
  MAX(_ingest_timestamp) AS max_ingest_timestamp
  FROM
    {src_table_name__4_1_1}
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
    {src_table_name__4_1_1} AS brz
  INNER JOIN 
    slv_records AS slv
    ON 
      brz.order_id =  slv.order_id
      AND brz._ingest_timestamp =  slv.max_ingest_timestamp
'''
df = spark.sql(brz_to_slv_sql)

# dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['order_id'])

# 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行。
## 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__4_1_1}'
df.createOrReplaceTempView(temp_view_name)


## Merge処理を実行
spark.sql(f'''
MERGE INTO {tgt_table_name__4_1_1} AS tgt
  USING {temp_view_name} AS src
  
  ON tgt.order_id = src.order_id
  
  WHEN MATCHED
  AND tgt._ingest_timestamp < src._ingest_timestamp
    THEN UPDATE SET *
  WHEN NOT MATCHED
    THEN INSERT *
''')

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__4_1_1))

# COMMAND ----------

# MAGIC %md
# MAGIC #### ToDo **`olist_order_items_dataset_bronze`** から **`olist_order_items_dataset_sliver`** へデータを書き込むパイプラインを作成してください。

# COMMAND ----------

src_table_name__4_1_2 = 'olist_order_items_dataset_bronze'
tgt_table_name__4_1_2 = 'olist_order_items_dataset_silver'

# COMMAND ----------

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__4_1_2}
(
    `order_id` STRING,
    `order_item_id` INT,
    `product_id` STRING,
    `seller_id` STRING,
    `shipping_limit_date` STRING,
    `price` DOUBLE,
    `freight_value` DOUBLE,
    _ingest_timestamp timestamp
)
USING delta
""")

# COMMAND ----------

# ToDo olist_order_items_dataset_bronze から olist_order_items_dataset_sliver へデータを書き込むパイプラインを作成してください。
<<FILL-IN>>

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__4_1_2))

# COMMAND ----------

# 複数回書き込みを実施しても、98,666 レコードとなることを確認
print(spark.table(tgt_table_name__4_1_2).count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 本ノートブック以降で利用するテーブルへの書き込みを実施

# COMMAND ----------

# `olist_order_reviews_dataset_bronze` テーブルへ書き込み
tgt_table_name__4_1_3 = 'olist_order_reviews_dataset_silver'
src_table_name__4_1_3 = 'olist_order_reviews_dataset_bronze'

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__4_1_3}
(
    `review_id` STRING,
    `order_id` STRING,
    `review_score` INT,
    `review_comment_title` STRING,
    `review_comment_message` STRING,
    `review_creation_date` TIMESTAMP,
    `review_answer_timestamp` TIMESTAMP,
    _ingest_timestamp timestamp
)
USING delta
""")

# 下記の処理を実行したデータフレーム（df）を作成
## 1. ブロンズテーブルから主キーごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''
with slv_records (
SELECT
  `review_id`,
  `order_id`,
  MAX(_ingest_timestamp) AS max_ingest_timestamp
  FROM
    {src_table_name__4_1_3}
  GROUP BY
    `review_id`,
    `order_id`
)
SELECT
  brz.`review_id`,
  brz.`order_id`,
  brz.`review_score`::INT,
  brz.`review_comment_title`,
  brz.`review_comment_message`,
  brz.`review_creation_date`::TIMESTAMP,
  brz.`review_answer_timestamp`::TIMESTAMP,
  brz._ingest_timestamp
  
  FROM
    {src_table_name__4_1_3} AS brz
  INNER JOIN 
    slv_records AS slv
    ON 
      brz.review_id =  slv.review_id
      AND brz.order_id =  slv.order_id
      AND brz._ingest_timestamp =  slv.max_ingest_timestamp
'''
df = spark.sql(brz_to_slv_sql)

# dropDuplicates関数にて、主キーの一意性を保証。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['review_id', 'order_id'])

# 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施してください。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行されるようにしてください。
## 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__4_1_3}'
df.createOrReplaceTempView(temp_view_name)


## Merge処理を実行
spark.sql(f'''
MERGE INTO {tgt_table_name__4_1_3} AS tgt
  USING {temp_view_name} AS src
  
  ON
    tgt.review_id =  src.review_id
    AND tgt.order_id =  src.order_id
  
  WHEN MATCHED
  AND tgt._ingest_timestamp < src._ingest_timestamp
    THEN UPDATE SET *
  WHEN NOT MATCHED
    THEN INSERT *
''')

spark.table(tgt_table_name__4_1_3).display()
print(spark.table(tgt_table_name__4_1_3).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. 階層（Struct）型のカラムを保持した Bronze テーブルから Silver テーブルへ書き込むパイプラインを作成してください

# COMMAND ----------

src_table_name__4_2_1 = 'olist_sellers_dataset_bronze'
tgt_table_name__4_2_1 = 'olist_sellers_dataset_silver'

# COMMAND ----------

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__4_2_1}
(
  seller_id STRING,
  zip_code_prefix INT,
  city STRING,  
  state STRING,
  _ingest_timestamp timestamp
)
USING delta
""")

# COMMAND ----------

# ToDo olist_sellers_dataset_bronze から olist_sellers_dataset_silver へデータを書き込むパイプラインを作成してください。
<<FILL-IN>>

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__4_2_1))

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__4_2_1))

# COMMAND ----------

# 複数回書き込みを実施しても、3,095 レコードとなることを確認
print(spark.table(tgt_table_name__4_2_1).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## C1. その他 Bronze テーブルをソースとした Silver テーブルへのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### 実践例 主キーが存在しない場合の実践例

# COMMAND ----------

# `olist_geolocation_dataset_silver` テーブルへ書き込み
tgt_table_name__4_c_2 = 'olist_geolocation_dataset_silver'
src_table_name__4_c_2 = 'olist_geolocation_dataset_bronze'

# 下記の処理を実行したデータフレーム（df）を作成
## 1. `_ingest_timestamp`列の最大日のレコードを抽出して、データ型を変更
brz_to_slv_sql = f'''
SELECT
  brz.`geolocation_zip_code_prefix`::INT,
  brz.`geolocation_lat`::INT,
  brz.`geolocation_lng`::INT,
  brz.`geolocation_city`,
  brz.`geolocation_state`,
  brz._ingest_timestamp
  
  FROM
    {src_table_name__4_c_2} AS brz
  WHERE 
    brz._ingest_timestamp = (
      SELECT
        MAX(_ingest_timestamp) AS max_ingest_timestamp
        FROM
          {src_table_name__4_c_2}
    )
'''
df = spark.sql(brz_to_slv_sql)

# dropDuplicates関数にて、重複排除
df = df.drop_duplicates()

# 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施してください。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行されるようにしてください。
## 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__4_c_2}'
df.createOrReplaceTempView(temp_view_name)


## Merge処理を実行
spark.sql(f'''
CREATE OR REPLACE TABLE {tgt_table_name__4_c_2} 
AS
  SELECT
    *
  FROM
    {temp_view_name}
''')

spark.table(tgt_table_name__4_c_2).display()
print(spark.table(tgt_table_name__4_c_2).count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### ToDo 未実施の Bronze テーブルをソースとして Silver テーブルへのパイプラインを作成してください。
# MAGIC 
# MAGIC - olist_customers_dataset_bronze
# MAGIC - olist_geolocation_dataset_bronze
# MAGIC - olist_order_payments_dataset_bronze
# MAGIC - olist_products_dataset_bronze
# MAGIC 
# MAGIC 下記については、主キー列がない場合の対応を実施してください。
# MAGIC 
# MAGIC - product_category_name_translation_bronze

# COMMAND ----------

# ToDO `olist_customers_dataset_bronze` テーブルへ書き込み
<<FILL-IN>>

# COMMAND ----------

# ToDO `olist_customers_dataset_bronze` テーブルへ書き込み
<<FILL-IN>>

# COMMAND ----------

# ToDO `olist_customers_dataset_bronze` テーブルへ書き込み
<<FILL-IN>>

# COMMAND ----------

# ToDO `olist_customers_dataset_bronze` テーブルへ書き込み
<<FILL-IN>>
