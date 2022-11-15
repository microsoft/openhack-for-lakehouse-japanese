-- Databricks notebook source
-- MAGIC %md # Hack Day 1
-- MAGIC ## Challenge1. 日別受注金額データの可視化 (Databricks SQL)  (目安 15:15~16:15)
-- MAGIC Q1. Databricks SQLにてクエリを作成してください。<br>
-- MAGIC Q2. Databricks SQLにて、日別受注金額に関する下記の項目を保持したDashboardを作成してください。<br>

-- COMMAND ----------

-- MAGIC %run ./includes/setup $mode="c_3"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # 現在のデータベースを改めて確認
-- MAGIC print(spark.sql('SELECT current_database()').first()[0])

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q1. Databricks SQLにてクエリを作成してください。
-- MAGIC - **`sales_history_gold`** のクエリをEditorへ入力し、Run ALLで実行結果を確認する
-- MAGIC - 実行結果下のタブ **`Edit visualization`** から折れ線グラフを選択する

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c1__databricks_sql/dbsql-third.png' width='1000' />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c1__databricks_sql/dbsql-fourth.png' width='1000' />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Q2. Databricks SQLにて、日別受注金額に関する下記の項目を保持したDashboardを作成してください。
-- MAGIC 
-- MAGIC 下記のビジュアルを保持したダッシュボードを作成してください。
-- MAGIC 
-- MAGIC   - 日付の期間で絞るフィルター
-- MAGIC   - 受注金額の総額を表示するカウンター
-- MAGIC   - 日付をX軸にした折れ線グラフ
-- MAGIC   - クエリ結果のテーブル
-- MAGIC 
-- MAGIC 参考リンク
-- MAGIC 
-- MAGIC -   [Databricks SQL による視覚化](https://docs.databricks.com/notebooks/visualizations/index.html)
-- MAGIC -   [視覚化タスク](https://docs.databricks.com/sql/user/visualizations/visualizations.html)
-- MAGIC -   [Databricks SQL ダッシュボード ](https://docs.databricks.com/sql/user/dashboards/index.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c1__databricks_sql/dbsql-second.png' width='800' />

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c1__databricks_sql/dbsql-fifth.png' width='800' />
