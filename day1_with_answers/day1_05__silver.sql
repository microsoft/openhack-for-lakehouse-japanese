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
CREATE OR REPLACE TABLE order_items_silver AS (
  SELECT
    *,
    price + freight_value product_sales
  FROM
    order_items_bronze
)

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
CREATE OR REPLACE TABLE orders_silver AS (
  SELECT
    *,
    CAST(
      date_format(order_purchase_timestamp, 'yyyy-MM-dd') AS DATE
    ) purchase_date
  FROM
    orders_bronze
  WHERE
    (
      order_status = 'delivered'
      OR order_status = 'shipped'
    )
    AND order_delivered_carrier_date IS NOT NULL
)

-- COMMAND ----------

SELECT * FROM orders_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ToDo :
-- MAGIC -  **`orders_silver`** に **`order_items_silver`** を`order_id`を結合キーとして左側結合する
-- MAGIC -  `purchase_date`, `seller_id`, `order_id`, `product_id`, `product_sales`をカラムとする新規のテーブル **`order_info_silver`** としてテーブル作成する

-- COMMAND ----------

-- FILL-IN
CREATE
OR REPLACE TABLE order_info_silver AS (
  SELECT
    a.order_id,
    seller_id,
    product_id,
    purchase_date,
    product_sales
  FROM
    orders_silver a
    JOIN order_items_silver b ON a.order_id = b.order_id
);

-- COMMAND ----------

SELECT * FROM order_info_silver LIMIT 5;
