# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 03. メダリオンアーキテクチャ構築の実践 - Bronze - (目安 11:30~12:00 + 13:45~14:15)
# MAGIC ### 本ノートブックの目的：DatabricksにおけるBronze Tableの役割・取り扱いについて理解を深める
# MAGIC Q1. Sparkテーブルにおけるテーブルプロパティ、および、事後処理の検討してください。<br>
# MAGIC Q2. Bronzeテーブルのパイプラインを作成してください。<br>
# MAGIC Q3. 半構造化データをbronzeテーブルとして読み込んでください。<br>
# MAGIC Q4. deltaのタイムトラベル機能による誤ったデータの取り込みを修正してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="3"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Sparkテーブルにおけるテーブルプロパティ、および、事後処理の検討してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ToDO 下記のドキュメントを参考に、設定すべきテーブルプロパティ、および、データエンジニアリング実施後に行うべき処理を、それぞれ3つ以上記載してください。
# MAGIC 
# MAGIC - [ファイル管理を使用してパフォーマンスを最適化する](https://docs.databricks.com/delta/file-mgmt.html)
# MAGIC - [自動最適化](https://docs.databricks.com/optimizations/auto-optimize.html)
# MAGIC - [ANALYZE TABLE](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-aux-analyze-table.html)
# MAGIC - [VACUUM](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html)
# MAGIC - [What's the best practice on running ANALYZE on Delta Tables for query performance optimization? (databricks.com)](https://community.databricks.com/s/question/0D53f00001GHVicCAH/whats-the-best-practice-on-running-analyze-on-delta-tables-for-query-performance-optimization)

# COMMAND ----------

# MAGIC %md
# MAGIC 設定を検討すべきテーブルプロパティ
# MAGIC - << FILL IN >>
# MAGIC 
# MAGIC データエンジニアリング実施後に行うべき処理
# MAGIC - << FILL IN >>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. Bronzeテーブルのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %md ### Data Overview
# MAGIC 店舗データを読み込み、プロファイリング、delta table化、メダリオンアーキテクチャーにそった形でダッシュボードと機械学習用に使えるデータに整形しましょう!
# MAGIC 
# MAGIC 今回利用するデータセットの関連図です。
# MAGIC 
# MAGIC <br>
# MAGIC <img src='https://github.com/naoyaabe-db/aws-databricks-hackathon-jp-20221006/raw/main/images/olist_data_relation.png' width='800' />
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
# MAGIC ToDo : 
# MAGIC  **`customers_bronze`** テーブルを以下のステップで作成します。<br>
# MAGIC  1. `src_path`と`schema`を用いてspark dataframeとしてcsvデータを読み込む
# MAGIC  1. 1.で作成したspark dataframeはSQLの構文で直接参照できないため　**`tmp_customers`** という名前のtemporary viewを作成する
# MAGIC  1. CREATE OR REPLACE TABLE AS構文によって2.で作成したviewを参照し　**`customers_bronze`** テーブルを作成する
# MAGIC 
# MAGIC  - [VIEW/TEMPORARY VIEW](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-view.html)
# MAGIC  - [spark dataframeからtemporary viewを作成する](https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.createOrReplaceTempView.html)
# MAGIC  - [DatabricksにてCTAS（CREATE TABLE AS SELECT）を利用する方法](https://qiita.com/manabian/items/6c960c1544c53977a316)

# COMMAND ----------

schema = '''
            `customer_id` STRING,
            `customer_unique_id` STRING,
            `customer_zip_code_prefix` INT,
            `customer_city` STRING,
            `customer_state` STRING
        '''

src_path = f"{datasource_dir}/olist_customers_dataset.csv"

df = (spark
          <<FILL-IN>>
     )

# COMMAND ----------

# temporary viewを作成する
df.<<FILL-IN>>

# COMMAND ----------

# MAGIC %sql SELECT * FROM tmp_customers;

# COMMAND ----------

# MAGIC %sql DESC EXTENDED tmp_customers;

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC -- CTASでolist_customers_dataset_bronzeを作成する
# MAGIC -- optimizeWrite、および、autoCompactをTrueに設定
# MAGIC -- https://docs.databricks.com/optimizations/auto-optimize.html
# MAGIC 
# MAGIC /* TODO　FILL-IN */

# COMMAND ----------

# MAGIC %sql DESC EXTENDED customers_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC CHECK : tables_infoを使って、各csvデータから同様のステップで複数のdeltaテーブルを一括で作成します<br>
# MAGIC 1. spark.readでcsvを読み込みデータフレームへ格納する関数 **`create_dataframe`** を作成します<br>
# MAGIC 1. データフレームをTemporaryViewへ変換し、CRAS（CREATE OR REPLACE TABLE AS構文）でテーブル作成する関数 **`save_as_delta_table`** を作成します<br>
# MAGIC 1. 上記2ステップをtables_infoの各テーブルへ実行する関数 **`create_tables`** を作成します

# COMMAND ----------

# DBTITLE 1,作成するテーブルの情報
tables_info = [
    
    # olist_geolocation_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "geolocation_bronze",
        "src_path"      : f"{datasource_dir}/olist_geolocation_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `geolocation_zip_code_prefix` INT,
            `geolocation_lat` DOUBLE,
            `geolocation_lng` DOUBLE,
            `geolocation_city` STRING,
            `geolocation_state` STRING
        ''',
    },
    
    # olist_order_items_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "order_items_bronze",
        "src_path"      : f"{datasource_dir}/olist_order_items_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `order_id` STRING,
            `order_item_id` INT,
            `product_id` STRING,
            `seller_id` STRING,
            `shipping_limit_date` STRING,
            `price` DOUBLE,
            `freight_value` DOUBLE
        ''',
    },
    
    # olist_order_payments_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "order_payments_bronze",
        "src_path"      : f"{datasource_dir}/olist_order_payments_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `order_id` STRING,
            `payment_sequential` INT,
            `payment_type` STRING,
            `payment_installments` INT,
            `payment_value` DOUBLE
        ''',
    },
    
    # olist_order_reviews_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "order_reviews_bronze",
        "src_path"      : f"{datasource_dir}/olist_order_reviews_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
    `review_id` STRING,
    `order_id` STRING,
    `review_score` INT,
    `review_comment_title` STRING,
    `review_comment_message` STRING,
    `review_creation_date` TIMESTAMP,
    `review_answer_timestamp` TIMESTAMP
        ''',
    },
    
    # olist_orders_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "orders_bronze",
        "src_path"      : f"{datasource_dir}/olist_orders_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `order_id` STRING,
            `customer_id` STRING,
            `order_status` STRING,
            `order_purchase_timestamp` TIMESTAMP,
            `order_approved_at` TIMESTAMP,
            `order_delivered_carrier_date` TIMESTAMP,
            `order_delivered_customer_date` TIMESTAMP,
            `order_estimated_delivery_date` TIMESTAMP    
        ''',
    },
    
    # olist_products_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "products_bronze",
        "src_path"      : f"{datasource_dir}/olist_products_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `product_id` STRING,
            `product_category_name` STRING,
            `product_name_lenght` INT,
            `product_description_lenght` INT,
            `product_photos_qty` INT,
            `product_weight_g` INT,
            `product_length_cm` INT,
            `product_height_cm` INT,
            `product_width_cm` INT
        ''',
    },
  
    # product_category_name_translation
    {
        "database_name" : f"{database}",
        "table_name"    : "product_category_name_translation_bronze",
        "src_path"      : f"{datasource_dir}/product_category_name_translation.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `product_category_name` STRING,
            `product_category_name_english` STRING
        ''',
    },
]

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp

def crate_dataframe(
    schema,
    src_path,
    file_format,
    header=True,    
):
    """データフレームをリターン
    """
    df = (spark
              .read
              .format(file_format)
              .schema(schema)
              .option('header', header)
              .load(src_path)
         )

    return df

# COMMAND ----------

def save_as_delta_table(
    src_df,
    database_name,
    table_name,
):
    """CTAS（Create As A Table）によりテーブルを作成
    """
    # 一時ビューを作成
    tmp_view_name = f'_tmp_{table_name}'
    src_df.createOrReplaceTempView(tmp_view_name)
    
    # CTAS（CREAT TABLE AS SLECT）により、テーブルを作成。
    spark.sql(f"""
        CREATE OR REPLACE TABLE {database_name}.{table_name}
          USING delta
          AS 
          SELECT 
            * 
            FROM 
              {tmp_view_name}
      
    """
    )

# COMMAND ----------

def create_tables(tables_info):
    """テーブル情報の辞書型リストに基づき、データフレームを作成して、テーブルを作成
    """
    print("---Started to create tables---")
    for l in tables_info:
        src_df = crate_dataframe(
            src_path    = l["src_path"],
            schema      = l["schema"],
            file_format = l["file_format"],
        )
        
        # `_ingest_timestamp`列を追加
        # _ingest_timestammpがいらないので、day2では利用しない
        # src_df = src_df.withColumn("_ingest_timestamp", current_timestamp())
        
        save_as_delta_table(
            src_df         = src_df,
            database_name  = l["database_name"],
            table_name     = l["table_name"],
        )
        
        print(f'`{l["table_name"]}`table is created.')
    print("---Ended to create tables---")

# COMMAND ----------

# DBTITLE 1,csvファイルからTableを一括で作成
create_tables(tables_info)

# COMMAND ----------

# DBTITLE 1,作成したテーブルの定義を確認（いろいろなテーブルで確認してみる）
# MAGIC %sql
# MAGIC DESC EXTENDED customers_bronze;

# COMMAND ----------

# DBTITLE 1,テーブル設定を一括で変更
def alter_TBLPROPERTIES(tables_info):
  print("---Start to alter TBLPROPERTIES.---")
  for l in tables_info:
    table_name = l["table_name"]
    spark.sql(f'''
    ALTER TABLE {table_name}  
      SET TBLPROPERTIES (
        delta.autoOptimize.optimizeWrite = True, 
        delta.autoOptimize.autoCompact   = True
      )
    ''')
    print(f"`{table_name}` is altered.")
  print("---End to alter TBLPROPERTIES.---")

alter_TBLPROPERTIES(tables_info)

# COMMAND ----------

# DBTITLE 1,変更したテーブルの設定を確認（いろいろなテーブルで確認してみる）
# MAGIC %sql
# MAGIC DESC EXTENDED customers_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3.半構造化データをbronzeテーブルとして読み込んでください。

# COMMAND ----------

# MAGIC %md
# MAGIC ToDo : 
# MAGIC 出品業者の情報である **`sellers_bronze`** テーブルはjson形式のファイルをロードして作成します。<br>
# MAGIC jsonは入れ子構造のデータ型です。jsonのスキーマはPySpark DataframeではSTRUCT型を用いて表現します。<br>
# MAGIC 
# MAGIC 参考リンク<br>
# MAGIC - [JSON Files - Spark 3.2.1 Documentation (apache.org)](https://spark.apache.org/docs/latest/sql-data-sources-json.html)
# MAGIC - [pyspark.sql.DataFrameReader.json — PySpark 3.2.1 documentation (apache.org)](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html#pyspark.sql.DataFrameReader.json)
# MAGIC - [Databricks（Spark）にてScalaの関数で取得可能なDDL文字列をPythonで取得する方法 - Qiita](https://qiita.com/manabian/items/4908f77a4da2c040cd6a)

# COMMAND ----------

# すべてのカラムのデータ型を文字列としたデータフレームを作成します
df = (spark
        .read
        .format('json')
        .option('primitivesAsString', True)
        .load(f"{datasource_dir}/olist_sellers_dataset.json")
     )

# データフレームのスキーマを表示します
json_data = df.schema.json()
schema = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(json_data).toDDL()
schema

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ToDo `sellers_bronze`をテーブル名として、すべてのカラムを文字列で保持したBronzeテーブルを作成してください。
# MAGIC -- `optimizeWrite`、および、`autoCompact`を`True`に設定
# MAGIC -- 最初の1列を統計情報の取得対象に設定
# MAGIC CREATE OR REPLACE TABLE sellers_bronze
# MAGIC (
# MAGIC   
# MAGIC   <<FILL-IN>>
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC     <<FILL-IN>>
# MAGIC   )

# COMMAND ----------

# ターゲットのテーブルへデータフレームから`append`によりデータを書き込む
(df.write
   .format('delta')
   .mode('append')
   .option("mergeSchema", "true")
   .saveAsTable("sellers_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 確認、seller列が構造体になっている
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   sellers_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4. deltaのタイムトラベル機能による誤ったデータの取り込みを修正してください。

# COMMAND ----------

# DBTITLE 1,ダミーデータをcustomers_bronzeへ書き込む
# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO customers_bronze VALUES
# MAGIC     ('test', 'test', 1060032, 'roppongi', 'JP');

# COMMAND ----------

# DBTITLE 1,dataが書き込まれたことを確認
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM customers_bronze
# MAGIC WHERE customer_id = 'test'

# COMMAND ----------

# DBTITLE 1,レコードがWRITEされた履歴を確認し、その直前のバージョン番号を控える
# MAGIC %sql
# MAGIC 
# MAGIC DESC HISTORY customers_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ToDo : testレコードがWRITEされていないバージョンに戻す
# MAGIC -- https://qiita.com/taka_yayoi/items/3b2095825a7e48b86f69
# MAGIC <<FILL IN>>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- testレコードがないテーブルのバージョンへ戻ったことを確認する
# MAGIC SELECT * FROM customers_bronze
# MAGIC WHERE customer_id = 'test'
