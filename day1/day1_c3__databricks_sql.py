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

# MAGIC %md
# MAGIC ### Q4.Databricks SQLにて、受注件数のクエリのソースをゴールドテーブルに変更してください。
