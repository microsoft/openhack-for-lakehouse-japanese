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

# MAGIC %run ../day3_00__config

# COMMAND ----------

# データベース名を変数に指定
database_name = "db_open_hackason"

dbutils.widgets.text("mode", "init")
mode = dbutils.widgets.get("mode")

dbutils.widgets.text("database_name", database_name)

database = f"{dbutils.widgets.get('database_name')}_day2_{user_name}"

username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Day1で利用する作業領域のディレクトリ
data_path       = f'/FileStore/db_openhackason_2022/day2/{user_name}'

# Kaggleからダウンロードしたファイルを配置するディレクトリ
#datasource_dir = f'/FileStore/db_openhackason_2022/datasource'
datasource_dir = 'wasbs://mshack@sajpstorage.blob.core.windows.net/dataset/'
