# Databricks notebook source
# MAGIC %md
# MAGIC ## Challenge4. Databricks Jobsによるジョブオーケストレーションの構築
# MAGIC 
# MAGIC Q1. Notebook ユーティリティ (dbutils)を利用して、ノートブックを実行してください。<br>
# MAGIC Q2. Databricks Jobsに登録してください。<br>
# MAGIC Q3. Databricks Jobsから呼び出すデータエンジニアリングのノートブックを作成してください。<br>
# MAGIC Q4. Databricks Jobsに登録してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="c_4"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Notebook ユーティリティ (dbutils)を利用して、ノートブックを実行してください。
# MAGIC 
# MAGIC 下記の作業を実施してください。
# MAGIC 
# MAGIC 1.`jobs`フォルダを作成後、新規ノートブックを作成してください。
# MAGIC 
# MAGIC 2.下記のコードを転記してください。
# MAGIC 
# MAGIC ```python
# MAGIC word = dbutils.widgets.get('word')
# MAGIC 
# MAGIC displayed_words = f'Hello {word}'
# MAGIC 
# MAGIC print(displayed_words)
# MAGIC ```
# MAGIC 
# MAGIC 3.Notebook ユーティリティ (dbutils)により、変数`output_word`がリターン値となるように設定してください。
# MAGIC 
# MAGIC 参考リンク
# MAGIC - [ノートブックのワークフロー - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/notebooks/notebook-workflows)

# COMMAND ----------

# ToDO 引数として`word`に`world`を指定して、ノートブックの実行結果を変数`output_word`に格納してください。
output_word = dbutils.notebook.run('./jobs/jobs_test', 3600, {"word" : "world"})

# COMMAND ----------

# `Hello world`と表示されることを確認
print(output_word)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. Databricks Jobsに登録してください。
# MAGIC 
# MAGIC Q1で作成したノートブックをDatabricks Jobsに登録して、実行してください。
# MAGIC 
# MAGIC 参考リンク
# MAGIC - [ジョブ - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. Databricks Jobsから呼び出すデータエンジニアリングのノートブックを作成してください。
# MAGIC 
# MAGIC 本手順までに作成したデータエンジニアリングのコードを保持したノートブックを作成してください。
# MAGIC 
# MAGIC すべてのノートブックにて、下記の対応をしてください。
# MAGIC - 本トレーニングで利用しているデータベース名を、Current Databaseとして指定してください。
# MAGIC 
# MAGIC Bronzeテーブルのノートブックにて、下記の対応をしてください。
# MAGIC - ソースを引数で指定し、テーブルごとのソースパスの値をデフォルトに指定してください。

# COMMAND ----------

# 本トレーニングで利用しているデータベース名
print__c4__db_name()

# COMMAND ----------

# テーブルごとのソースパス
print__c4__file_path()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4. Databricks Jobsに登録してください。
# MAGIC 
# MAGIC Q3で作成したノートブックをDatabricks Jobsに登録して、実行してください。
