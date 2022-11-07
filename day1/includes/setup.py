# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 想定のディレクトリ構成
# MAGIC 
# MAGIC ```
# MAGIC /dbfs/FileStore
# MAGIC ├── db_hackathon4lakehouse_2022
# MAGIC │   ├── datasource      <- kaggleにて提供されているCSVファイルを配置
# MAGIC │   ├── {user_name}
# MAGIC │   │   ├── src         <- Day1で利用するソースファイルを配置
# MAGIC │   │   ├── database    <- Day1で利用するデータベースのディレクトリ
# MAGIC │   │   ├── auto_loader <- Auto Loader機能で利用するディレクトリ
# MAGIC ```
# MAGIC 
# MAGIC ※ 事前に`dbfs:/FileStore/db_hackathon4lakehouse_2022/datasource`ディレクトリに下記のデータセットを配置する必要あります。
# MAGIC - [Brazilian E-Commerce Public Dataset by Olist | Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv)

# COMMAND ----------

# MAGIC %run ../day1_00__config

# COMMAND ----------

# データベース名を変数に指定
database_name = "db_hackathon4lakehouse"

dbutils.widgets.text("mode", "cleanup")
mode = dbutils.widgets.get("mode")

dbutils.widgets.text("database_name", database_name)

database = f"{database_name}_{user_name}"

# Day1で利用する作業領域のディレクトリ
data_path       = f'/FileStore/db_hackathon4lakehouse_2022/{user_name}'

# Kaggleからダウンロードしたファイルを配置するディレクトリ
datasource_dir = f'/FileStore/db_hackathon4lakehouse_2022/datasource'

# COMMAND ----------

# ファイルを配置する関数を定義
def put_files(output_dir,loop_num=1):
    import os
    import csv
    import datetime

    output_dir_for_pyapi = f'/dbfs{output_dir}'    
    os.makedirs(output_dir_for_pyapi, exist_ok=True)
    
    i = 0
    while i < loop_num:
        current_datetime = datetime.datetime.now()
        datetime_num_str = current_datetime.strftime('%Y%m%d%H%M%S')
        datetime_iso_str = current_datetime.isoformat()

        with open(f'{output_dir_for_pyapi}/{datetime_num_str}.csv', 'w+') as f:
            writer = csv.writer(f)
            writer.writerow([datetime_iso_str])
        
        i += 1
        
def put_csv_files(file_dir, csv_data):
    import inspect
    import os

    file_dir_for_py = f'/dbfs{file_dir}' 

    os.makedirs(file_dir_for_py, exist_ok=True)

    with open(f'{file_dir_for_py}/test.csv', 'w') as f:
        f.write(inspect.cleandoc(csv_data))    

def print__c4__file_path():
    # olist_order_items_dataset_bronze
    print(f'olist_orders_dataset_bronze      : {data_path}/src/3_2/second/*')
    
    # olist_orders_dataset_bronze
    print(f'olist_order_items_dataset_bronze : {data_path}/src/c_1/first/*')
    
    # olist_sellers_dataset_bronze
    print(f'olist_sellers_dataset_bronze     : {data_path}/src/c_2/*')
    
def print__c4__db_name():    
    # 現在のデータベース
    print(f'Current Database                 : {spark.catalog.currentDatabase()}')


# COMMAND ----------

if mode == "init":

    # データベースの準備
    print(f"data_path : {data_path}")
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database} LOCATION "{data_path}/database/{database}"')


# データベースのデフォルトをセット
spark.sql(f"USE {database}")
print(f"database  : {spark.catalog.currentDatabase()}")

if mode != '2_2':
    # 2-2以外の手順では、パーティション数のデフォルト'200'を指定
    spark.conf.set("spark.sql.shuffle.partitions", 200)    
    
if mode == '2_2':
    # 2-2の手順では、ストリーミング処理を行うため、現在のsparkコア数をパーティション数として指定
    spark_core = spark.sparkContext.defaultParallelism
    spark.conf.set("spark.sql.shuffle.partitions", spark_core)

if mode == '3_2':
    # 初回データの配置
    src_file_path__3_2__first = f'{data_path}/src/3_2/first'
    csv_data = '''
    order_id,customer_id,order_status
    e481f51cbdc54678b7cc49136f2d6af7,9ef432eb6251297304e76186b10a928d,delivered
    53cdb2fc8bc7dce0b6741e2150273451,b0830fb4747a6c6d20dea0b8c802d7ef,delivered
    47770eb9100c2d0c44946d9cf07ec65d,41ce2a54c0b03bf3443c3d931a367089,delivered
    '''
    dbutils.fs.rm(src_file_path__3_2__first, True)
    put_csv_files(src_file_path__3_2__first, csv_data)
    
    # 2回目データの配置
    origin_file_path__3_2 = f'{datasource_dir}/olist_orders_dataset.csv'
    src_file_path__3_2__second = f'{data_path}/src/3_2/second/olist_orders_dataset.csv'
    dbutils.fs.cp(origin_file_path__3_2, src_file_path__3_2__second, True)

if mode == 'c_1':
    # 初回目データの配置
    origin_file_path__c_1 = f'{datasource_dir}/olist_order_items_dataset.csv'
    src_file_path__c_1__first = f'{data_path}/src/c_1/first/olist_order_items_dataset.csv'
    dbutils.fs.cp(origin_file_path__c_1, src_file_path__c_1__first, True)
    
    # 2回目データの配置
    src_file_path__c_1__second = f'{data_path}/src/c_1/second'
    csv_data = '''
    order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value
    test,1,test_product,test_id,2001-01-01 00:00:00,12.3,12.3
    test,2,test_product,test_id,2001-01-01 00:00:00,12.3,12.3
    test,3,test_product,test_id,2001-01-01 00:00:00,12.3,12.3
    '''
    dbutils.fs.rm(src_file_path__c_1__second, True)
    put_csv_files(src_file_path__c_1__second, csv_data)

if mode == 'c_2':
    import os
    # dlt用のディレクトリ作成
    orders_dir = f"{data_path}/dlt/orders/"
    order_items_dir = f"{data_path}/dlt/order_items/"
    
    os.makedirs(orders_dir, exist_ok=True)
    os.makedirs(order_items_dir, exist_ok=True)
    
    # csvデータの配置
    source_path_orders = f"{datasource_dir}/olist_orders_dataset.csv"
    source_path_order_items = f"{datasource_dir}/olist_order_items_dataset.csv"
    
    dbutils.fs.cp(source_path_orders, orders_dir+"olist_orders_dataset.csv")
    dbutils.fs.cp(source_path_order_items, order_items_dir+"olist_order_items_dataset.csv")

if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(data_path, True)

# COMMAND ----------



# COMMAND ----------


