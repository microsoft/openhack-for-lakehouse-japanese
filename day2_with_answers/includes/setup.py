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
# MAGIC │   │   ├── database    <- Day2で利用するデータベースのディレクトリ
# MAGIC ```
# MAGIC 
# MAGIC ※ 事前に`dbfs:/FileStore/db_hackathon4lakehouse_2022/datasource`ディレクトリに下記のデータセットを配置する必要あります。
# MAGIC - [Brazilian E-Commerce Public Dataset by Olist | Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce?select=olist_customers_dataset.csv)

# COMMAND ----------

# MAGIC %run ../day2_00__config

# COMMAND ----------

# データベース名を変数に指定
database_name = "db_hackathon4lakehouse"

dbutils.widgets.text("mode", "init")
mode = dbutils.widgets.get("mode")

dbutils.widgets.text("database_name", database_name)

database = f"{dbutils.widgets.get('database_name')}_day2_{user_name}"


# Day1で利用する作業領域のディレクトリ
data_path       = f'/FileStore/db_hackathon4lakehouse_2022/{user_name}'

# Kaggleからダウンロードしたファイルを配置するディレクトリ
datasource_dir = f'/FileStore/db_hackathon4lakehouse_2022/datasource'

# COMMAND ----------

if mode == "init":

    # データベースの準備
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    spark.sql(f'CREATE DATABASE IF NOT EXISTS {database} LOCATION "{data_path}/database/{database}"')

# データベースのデフォルトをセット
spark.sql(f"USE {database}")

# 利用するディレクトリやデータベース等の情報を表示
print(f"data_path : {data_path}")
print(f"database  : {spark.catalog.currentDatabase()}")


if mode == "cleanup":
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(data_path, True)
