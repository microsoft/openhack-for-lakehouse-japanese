# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 想定のディレクトリ構成
# MAGIC 
# MAGIC ```
# MAGIC /dbfs/FileStore
# MAGIC ├── db_openhackason_2022
# MAGIC │   ├── datasource      <- kaggleにて提供されているCSVファイルを配置
# MAGIC │   ├── day2/{user_name}
# MAGIC │   │   ├── database    <- Day2で利用するデータベースのディレクトリ
# MAGIC ```
# MAGIC 
# MAGIC ※ 事前に`dbfs:/FileStore/db_openhackason_2022/datasource`ディレクトリに下記のデータセットを配置する必要あります。
# MAGIC - [Brazilian E-Commerce Public Dataset by Olist | Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv)

# COMMAND ----------

# MAGIC %run ../day2_00__config

# COMMAND ----------

# データベース名を変数に指定
database_name = "db_open_hackason"

dbutils.widgets.text("mode", "init")
mode = dbutils.widgets.get("mode")

dbutils.widgets.text("database_name", database_name)

database = f"{dbutils.widgets.get('database_name')}_day2_{user_name}"


# Day1で利用する作業領域のディレクトリ
data_path       = f'/FileStore/db_openhackason_2022/day2/{user_name}'

# Kaggleからダウンロードしたファイルを配置するディレクトリ
datasource_dir = f'/FileStore/db_openhackason_2022/datasource'


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
          TBLPROPERTIES (
            delta.autoOptimize.optimizeWrite = True, 
            delta.autoOptimize.autoCompact   = True
          )
          AS 
          SELECT 
            * 
            FROM 
              {tmp_view_name}
      
    """
    )

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
        #src_df = src_df.withColumn("_ingest_timestamp", current_timestamp())
        
        save_as_delta_table(
            src_df         = src_df,
            database_name  = l["database_name"],
            table_name     = l["table_name"],
        )
        
        print(f'`{l["table_name"]}`table is created.')
    print("---Ended to create tables---")


# COMMAND ----------

# 作成するテーブル情報
tables_info = [
    # olist_customers_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "olist_customers_dataset",
        "src_path"      : f"{datasource_dir}/olist_customers_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `customer_id` STRING,
            `customer_unique_id` STRING,
            `customer_zip_code_prefix` INT,
            `customer_city` STRING,
            `customer_state` STRING
        ''',
    },
    
    # olist_geolocation_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "olist_geolocation_dataset",
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
        "table_name"    : "olist_order_items_dataset",
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
        "table_name"    : "olist_order_payments_dataset",
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
        "table_name"    : "olist_order_reviews_dataset",
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
        "table_name"    : "olist_orders_dataset",
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
        "table_name"    : "olist_products_dataset",
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
    
    # olist_sellers_dataset
    {
        "database_name" : f"{database}",
        "table_name"    : "olist_sellers_dataset",
        "src_path"      : f"{datasource_dir}/olist_sellers_dataset.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `seller_id` STRING,
            `seller_zip_code_prefix` INT,
            `seller_city` STRING,
            `seller_state` STRING
        ''',
    },
    
    # product_category_name_translation
    {
        "database_name" : f"{database}",
        "table_name"    : "product_category_name_translation",
        "src_path"      : f"{datasource_dir}/product_category_name_translation.csv",
        "file_format"   : "csv",
        "schema"        : '''
            `product_category_name` STRING,
            `product_category_name_english` STRING
        ''',
    },
]

# COMMAND ----------

if mode == "init":

    # データベースの準備
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database} LOCATION "{data_path}/database/{database}"')
    
    # テーブルの作成
    create_tables(tables_info)

# データベースのデフォルトをセット
spark.sql(f"USE {database}")

# 利用するディレクトリやデータベース等の情報を表示
print(f"data_path : {data_path}")
print(f"database  : {spark.catalog.currentDatabase()}")


if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(data_path, True)
