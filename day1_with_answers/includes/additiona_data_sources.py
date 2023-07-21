# Databricks notebook source
# `datasource_dir` 変数が定義されていることが前提
try:
    additionla_datasource_dir
except:
    assert False, '`additionla_datasource_dir`変数を定義してください。'

# COMMAND ----------

# Get data source variables.

# COMMAND ----------

# MAGIC %run ./includes/2018_olist_item_sales

# COMMAND ----------

# MAGIC %run ./includes/2019_olist_item_sales

# COMMAND ----------

# MAGIC %run ./includes/2020_olist_item_sales

# COMMAND ----------

# MAGIC %run ./includes/2021_olist_item_sales

# COMMAND ----------

# MAGIC %run ./includes/20180830_olist_item_sales

# COMMAND ----------

import inspect

class AddtionalDataSources:
    def __init__(self):
        self.additonal_data_sources_info = {        
            '2018_olist_item_sales': {
                'src': src__2018_olist_item_sales,
                'out_path': f'{additionla_datasource_dir}/2018_olist_item_sales.csv',
            },
            '2019_olist_item_sales': {
                'src': src__2019_olist_item_sales,
                'out_path': f'{additionla_datasource_dir}/2019_olist_item_sales.csv',
            },
            '2020_olist_item_sales': {
                'src': src__2020_olist_item_sales,
                'out_path': f'{additionla_datasource_dir}/2020_olist_item_sales.csv',
            },
            '2021_olist_item_sales': {
                'src': src__2021_olist_item_sales,
                'out_path': f'{additionla_datasource_dir}/2021_olist_item_sales.csv',
            },
            '20180830_olist_item_sales': {
                'src': src__20180830_olist_item_sales,
                'out_path': f'{additionla_datasource_dir}/20180830_olist_item_sales.csv',
            },
        }

    def put_additional_data_source(self):
        print(f'-- Adiitional files are creating. --')
        for file,value in self.additonal_data_sources_info.items():
            out_path = value['out_path']
            cleaned_data = inspect.cleandoc(value['src'])
            dbutils.fs.put(out_path, cleaned_data, True)
