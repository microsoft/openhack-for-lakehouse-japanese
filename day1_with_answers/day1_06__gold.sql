-- Databricks notebook source
-- MAGIC %md # Hack Day 1
-- MAGIC ## 06. メダリオンアーキテクチャ構築の実践 - Gold Table - (目安 15:10~15:15)
-- MAGIC ### 本ノートブックの目的：DatabricksにおけるGold Tableの役割・取り扱いについて理解を深める
-- MAGIC Q1. Gold Tableのパイプラインを作成してください

-- COMMAND ----------

-- MAGIC %run ./includes/setup $mode="6"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q1. Gold Tableのパイプラインを作成してください

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 実践例

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC - **`orders_silver`** に **`order_items_silver`** を`order_id`を結合キーとして左側結合する
-- MAGIC - `order_purchase_timestamp`をDATE型に CAST して、`purchase_date`　列として追加
-- MAGIC - `order_status`列が`delivered`と`shipped`のレコードを抽出
-- MAGIC - `order_delivered_carrier_date`列がNULLでないレコードを抽出
-- MAGIC - `purchase_date`, `seller_id`, `order_id`, `product_id`, `product_sales`をカラムとする新規のテーブル **`order_info_gold`** としてテーブル作成する

-- COMMAND ----------

-- MAGIC %sql CREATE
-- MAGIC OR REPLACE TABLE order_info_silver AS
-- MAGIC SELECT
-- MAGIC   a.order_id,
-- MAGIC   seller_id,
-- MAGIC   product_id,
-- MAGIC   CAST(order_purchase_timestamp AS DATE) AS purchase_date,
-- MAGIC   price + freight_value AS product_sales
-- MAGIC FROM
-- MAGIC   olist_orders_dataset_silver a
-- MAGIC   LEFT OUTER JOIN olist_order_items_dataset_silver b ON a.order_id = b.order_id
-- MAGIC WHERE
-- MAGIC   a.order_status IN ('delivered', 'shipped')
-- MAGIC   AND a.order_delivered_carrier_date IS NOT NULL;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   order_info_silver
-- MAGIC LIMIT
-- MAGIC   5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ToDo `sales_history_gold` を作成してください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ToDo
-- MAGIC - **`order_info_silver`** を`date`で集計し、`product_sales`をSUMし新規のカラム`sales`を作成する
-- MAGIC -  **`sales_history_gold`** を `purchase_date`をキーに昇順で並び替えてください
-- MAGIC - `purchase_date`, `sales`をカラムとする売上の時系列データの新規テーブル **`sales_history_gold`** を作成してください

-- COMMAND ----------

-- FILL-IN
CREATE
OR REPLACE TABLE sales_history_gold AS (
  SELECT
    purchase_date,
    SUM(product_sales) sales
  FROM
    order_info_silver
  GROUP BY
    purchase_date
  ORDER BY
    purchase_date asc
);

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   *
-- MAGIC FROM
-- MAGIC   sales_history_gold
