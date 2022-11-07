-- Databricks notebook source
-- MAGIC %md # Hack Day 1
-- MAGIC ## 06. メダリオンアーキテクチャ構築の実践 - Gold Table - (目安 15:10~15:15)
-- MAGIC ### 本ノートブックの目的：DatabricksにおけるGold Tableの役割・取り扱いについて理解を深める
-- MAGIC Q1. Gold Tableのパイプラインを作成してください

-- COMMAND ----------

-- MAGIC %run ./includes/setup $mode="3"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q1. Gold Tableのパイプラインを作成してください

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ToDo : 
-- MAGIC - **`order_info_silver`** を`date`で集計し、`product_sales`をSUMし新規のカラム`sales`を作成する
-- MAGIC - `purchase_date`, `sales`をカラムとする売上の時系列データの新規テーブル **`sales_history_gold`** を作成してください
-- MAGIC -  **`sales_history_gold`** を `purchase_date`をキーに昇順で並び替えてください

-- COMMAND ----------

-- TODO
<<FILL-IN>>
