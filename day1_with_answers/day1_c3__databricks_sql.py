# Databricks notebook source
# MAGIC %md
# MAGIC ## Challenge3. 日別受注金額データの可視化 (Databricks SQL)
# MAGIC Q1. Databricks SQLにてクエリを作成してください。<br>
# MAGIC Q2. Databricks SQLにて、日別受注金額に関する下記の項目を保持したDashboardを作成してください。<br>
# MAGIC Q3. 日別受注金額を保持したゴールドテーブル（daily_order_amounts）を作成してください。<br>
# MAGIC Q4. Databricks SQLにて、受注件数のクエリのソースをゴールドテーブルに変更してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="c_3"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Databricks SQLにてクエリを作成してください。
# MAGIC 
# MAGIC Databricks SQLにて、下記のクエリを作成してください。
# MAGIC   - `olist_orders_dataset`テーブルと`olist_order_items_dataset`テーブルを結合
# MAGIC   - `order_status`列が`delivered`と`shipped`のレコードを抽出
# MAGIC   - `order_delivered_carrier_date`列がNULLでないレコードを抽出
# MAGIC   - `order_delivered_carrier_date`列を日付型に変換
# MAGIC   - `order_delivered_carrier_date`列ごとに、`price`列と`freight_value`列の合計をSUM関数
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC -   [クエリ - Azure Databricks - Databricks SQL | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/sql/user/queries/)
# MAGIC -   [クエリ タスク - Azure Databricks - Databricks SQL | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/sql/user/queries/queries)
# MAGIC -   [クエリ フィルター - Azure Databricks - Databricks SQL | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/sql/user/queries/query-filters)

# COMMAND ----------

# 現在のデータベースを改めて確認
spark.catalog.currentDatabase()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ToDo Databricks SQLにて、下記のクエリを作成してください。
# MAGIC --USE db_open_hackason_day1_team;
# MAGIC with src (
# MAGIC 
# MAGIC SELECT
# MAGIC   od.order_id,
# MAGIC   cast(od.order_delivered_carrier_date as date) AS order_delivered_carrier_date,
# MAGIC   
# MAGIC   oid.product_id,
# MAGIC   oid.price,
# MAGIC   oid.freight_value
# MAGIC 
# MAGIC   FROM
# MAGIC     olist_order_items_dataset oid
# MAGIC   
# MAGIC     INNER JOIN olist_orders_dataset od
# MAGIC     ON oid.order_id = od.order_id
# MAGIC   
# MAGIC   WHERE
# MAGIC     od.order_delivered_carrier_date IS NOT NULL
# MAGIC     AND od.order_status IN ("delivered", "shipped")
# MAGIC     
# MAGIC ), agg (
# MAGIC 
# MAGIC SELECT
# MAGIC     order_delivered_carrier_date,
# MAGIC     SUM(price+freight_value) AS sales_amount --受注金額
# MAGIC   FROM
# MAGIC     src
# MAGIC   GROUP BY
# MAGIC     order_delivered_carrier_date      
# MAGIC )
# MAGIC 
# MAGIC SELECT
# MAGIC   *
# MAGIC   FROM
# MAGIC     agg
# MAGIC   -- 下記は、Databricks SQL上でのみ有効とする
# MAGIC   -- WHERE order_delivered_carrier_date > '{{ Date Range.start }}'
# MAGIC   --     AND order_delivered_carrier_date < '{{ Date Range.end }}'       

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. Databricks SQLにて、日別受注金額に関する下記の項目を保持したDashboardを作成してください。
# MAGIC 
# MAGIC 下記のビジュアルを保持したダッシュボードを作成してください。
# MAGIC 
# MAGIC   - 日付の期間で絞るフィルター
# MAGIC   - 受注金額の総額を表示するカウンター
# MAGIC   - 日付をX軸にした折れ線グラフ
# MAGIC   - クエリ結果のテーブル
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC -   [Databricks SQL による視覚化 - Azure Databricks - Databricks SQL | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/sql/user/visualizations/)
# MAGIC -   [視覚化タスク - Azure Databricks - Databricks SQL | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/sql/user/visualizations/visualizations)
# MAGIC -   [Databricks SQL ダッシュボード - Azure Databricks - Databricks SQL | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/sql/user/dashboards/)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. 日別受注金額を保持したゴールドテーブル（daily_order_amounts）を作成してください。

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Answer SQLのみで実行する場合
# MAGIC -- USE db_open_hackason_day1_team;
# MAGIC CREATE OR REPLACE TABLE daily_order_amounts 
# MAGIC using delta TBLPROPERTIES (
# MAGIC   delta.autoOptimize.optimizeWrite = True,
# MAGIC   delta.autoOptimize.autoCompact = True,
# MAGIC   delta.dataSkippingNumIndexedCols = 1
# MAGIC ) AS 
# MAGIC with src (
# MAGIC SELECT
# MAGIC   od.order_id,
# MAGIC   cast(od.order_delivered_carrier_date as date) AS order_delivered_carrier_date,
# MAGIC   oid.product_id,
# MAGIC   oid.price,
# MAGIC   oid.freight_value
# MAGIC   FROM
# MAGIC     olist_order_items_dataset oid
# MAGIC     INNER JOIN olist_orders_dataset od ON oid.order_id = od.order_id
# MAGIC   WHERE
# MAGIC     od.order_delivered_carrier_date IS NOT NULL
# MAGIC     AND od.order_status IN ("delivered", "shipped")
# MAGIC ), agg (
# MAGIC SELECT
# MAGIC   order_delivered_carrier_date,
# MAGIC   SUM(price + freight_value) AS sales_amount --受注金額
# MAGIC   FROM
# MAGIC     src
# MAGIC   GROUP BY
# MAGIC     order_delivered_carrier_date
# MAGIC )
# MAGIC 
# MAGIC select
# MAGIC   *
# MAGIC   from
# MAGIC     agg

# COMMAND ----------

# Answer データフレームで実施する場合
tgt_table_name__c_3 = "daily_order_amounts"


slv_to_gld_sql = f"""
with src (

SELECT
  od.order_id,
  cast(od.order_delivered_carrier_date as date) AS order_delivered_carrier_date,
  
  oid.product_id,
  oid.price,
  oid.freight_value

  FROM
    olist_order_items_dataset oid
  
    INNER JOIN olist_orders_dataset od
    ON oid.order_id = od.order_id
  
  WHERE
    od.order_delivered_carrier_date IS NOT NULL
    AND od.order_status IN ("delivered", "shipped")
    
), agg (

SELECT
    order_delivered_carrier_date,
    SUM(price+freight_value) AS sales_amount --受注金額
  FROM
    src
  GROUP BY
    order_delivered_carrier_date      
)

SELECT
  *
  FROM
    agg
"""
df = spark.sql(slv_to_gld_sql)

# 一時ビューを作成
tmp_view_name = f'_tmp_{tgt_table_name__c_3}'
df.createOrReplaceTempView(tmp_view_name)

# ToDo CTAS（CREAT TABLE AS SLECT）により、`monthly_sales_counts`テーブルを作成
ctas_sql = f'''
create or replace table {tgt_table_name__c_3}
  using delta
  TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 1
  )
  AS 
  select 
    * 
    from 
      {tmp_view_name}
'''
spark.sql(ctas_sql)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4.Databricks SQLにて、受注件数のクエリのソースをゴールドテーブルに変更してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Answer
# MAGIC --USE db_open_hackason_day1_team;
# MAGIC SELECT
# MAGIC   *
# MAGIC   FROM
# MAGIC     daily_order_amounts
# MAGIC     -- 下記は、Databricks SQL上でのみ有効とする
# MAGIC     -- WHERE order_delivered_carrier_date > '{{ Date Range.start }}'
# MAGIC     --     AND order_delivered_carrier_date < '{{ Date Range.end }}'
