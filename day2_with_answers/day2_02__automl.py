# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC 
# MAGIC ## 02. AutoMLによる売上予測モデル作成 (目安 13:45~14:45)
# MAGIC ### 本ノートブックの目的：AutoMLを使ったモデル開発について理解を深める
# MAGIC 
# MAGIC このノートブックでは、01で作ったFeature Store上のデータを使ってベストなモデルを作成します。

# COMMAND ----------

# MAGIC %md ## Q1. 下記を参考にAutomlを使って、売上予測モデルを作成してください

# COMMAND ----------

# MAGIC %md-sandbox ### DatabricksのAuto MLと`item_sales`テーブルの使用
# MAGIC 
# MAGIC <img style="float: right" width="600" src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-first.png'>
# MAGIC 
# MAGIC Auto MLは、「Machine Learning(機械学習)」メニュースペースで利用できます。<br>
# MAGIC (Machine Learning メニューを選択し、ホーム画面で AutoMLを選択してください)
# MAGIC 
# MAGIC 新規にAuto-ML実験を開始し、先ほど作成した特徴量テーブル(`item_sales`)を選択するだけで良いのです。
# MAGIC 
# MAGIC ML Problem typeは、今回は`Forcasting`です。
# MAGIC prediction targetは`y`カラムです。
# MAGIC Time columnは`ds`カラムです。
# MAGIC 
# MAGIC AutoMLのMetricや実行時間とトライアル回数については、Advance Menuで選択できます。
# MAGIC 
# MAGIC Forecast horizon and frequency(予測対象の期間)は`60` 日とします
# MAGIC 
# MAGIC Startをクリックすると、あとはDatabricksがやってくれます。
# MAGIC 
# MAGIC この作業はUIで行いますが[python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)による操作も可能です。

# COMMAND ----------

# MAGIC %md ## 実験の経過や結果については、MLflow のExperiment画面で確認可能です。
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-second.png' />

# COMMAND ----------

# MAGIC %md ## 注意事項
# MAGIC 
# MAGIC - AutoMLは、シングルノード上で実験が行われるため、メモリサイズが小さいと学習できるデータセット数が小さくなります。そのためメモリ搭載の多いインスタンスを選択してください。 
# MAGIC - 上図(MLflow Experiment UI)の Alertタブを確認ください。
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/tutoml_alert.png' />

# COMMAND ----------

# MAGIC %md ## AutoMLが完了した後は

# COMMAND ----------

# MAGIC %md 
# MAGIC 表示された結果にチェックボックスをつけて、compareを押してください
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-compare.png' />

# COMMAND ----------

# MAGIC %md ## Q2. モデルごとのmetrics比較を行なってください

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/mlflow_comapre.jpg' />

# COMMAND ----------

# MAGIC %md ## Q3. ベストノートブックからのチューニングを行なってください

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-best.png' />

# COMMAND ----------

# MAGIC %md ## notebookのclone

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-rerun.png' />

# COMMAND ----------

# MAGIC %md ## パラメータを変更
# MAGIC 赤枠の箇所を変更・追記して、notebookの上位にあるRun Allをクリックしてください

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/country_holidays2.jpg' />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/mlflow_logparam.jpg' />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-rerun.png' />

# COMMAND ----------

# MAGIC %md ## パラメータ変更した状態でのmlflowを使ったmodel比較
# MAGIC 画面menuからExperiments -> automlで作成したExperimentsをクリック

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-modelcompare.png' />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__automl/automl-result.png' />
