-- Databricks notebook source
-- MAGIC %md # Hack Day 1
-- MAGIC ## 05. メダリオンアーキテクチャ構築の実践 - Silver Table - (目安 14:45~15:10)
-- MAGIC ### 本ノートブックの目的：DatabricksにおけるSilverテーブルの役割・取り扱いについて理解を深める
-- MAGIC Q1. Silver Tableのパイプラインを作成してください

-- COMMAND ----------

-- MAGIC %run ./includes/setup $mode="3"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q1. Silver Tableのパイプラインを作成してください

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ToDo :  **`order_items_bronze`** をソースとして
-- MAGIC - `price` および `freight_value`を足し合わせ、productごとの値段`product_sales`を計算する　<br>
-- MAGIC -  既存のカラムと`product_sales`をカラムとする新規のテーブル **`order_items_silver`** としてテーブル作成する

-- COMMAND ----------

-- TODO
<<FILL-IN>>

-- COMMAND ----------

SELECT * FROM order_items_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ToDo :   **`orders_bronze`** に以下の操作をします
-- MAGIC - `order_purchase_timestamp`をyyyy-MM-ddの形式にフォーマットし、`purchase_date`という列名に変更。DATE型にCASTする
-- MAGIC - `order_status`列が`delivered`と`shipped`のレコードを抽出
-- MAGIC - `order_delivered_carrier_date`列がNULLでないレコードを抽出
-- MAGIC - 既存のカラムと`purchase_date`をカラムとする新規のテーブル **`orders_silver`** としてテーブル作成する

-- COMMAND ----------

--TODO
<<FILL-IN>>

-- COMMAND ----------

SELECT * FROM orders_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ToDo :
-- MAGIC -  **`orders_silver`** に **`order_items_silver`** を`order_id`を結合キーとして左側結合する
-- MAGIC -  `purchase_date`, `seller_id`, `order_id`, `product_id`, `product_sales`をカラムとする新規のテーブル **`order_info_silver`** としてテーブル作成する

-- COMMAND ----------

-- FILL-IN
<<FILL-IN>>

-- COMMAND ----------

SELECT * FROM order_info_silver LIMIT 5;
