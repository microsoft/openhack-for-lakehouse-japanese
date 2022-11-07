# Databricks notebook source
# MAGIC %md # 03 AutoMLによるモデル作成
# MAGIC 
# MAGIC このノートブックでは、02で作ったFeature Store上のデータを使ってベストなモデルを作成します。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/2_automl.png' width='800' />

# COMMAND ----------

# MAGIC %md ## Q1. 下記を参考にAutomlを使って、売上予測モデルを作成してください

# COMMAND ----------

# MAGIC %md-sandbox ### DatabricksのAuto MLとorderitem_joinDFデータセットの使用
# MAGIC 
# MAGIC <img style="float: right" width="600" src='https://sajpstorage.blob.core.windows.net/mshack/images/automl_first.jpg'>
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
# MAGIC Forecast horizon and frequencyは`136`
# MAGIC 
# MAGIC Startをクリックすると、あとはDatabricksがやってくれます。
# MAGIC 
# MAGIC この作業はUIで行いますが[python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1)による操作も可能です。

# COMMAND ----------

# MAGIC %md ## 実験の経過や結果については、MLflow のExperiment画面で確認可能です。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/autoML_results.jpg' />

# COMMAND ----------

# MAGIC %md ## 注意事項
# MAGIC 
# MAGIC - AutoMLは、シングルノード上で実験が行われるため、メモリサイズが小さいと学習できるデータセット数が小さくなります。そのためメモリ搭載の多いインスタンスを選択してください。 
# MAGIC - 上図(MLflow Experiment UI)の Alertタブを確認ください。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/tutoml_alert.png' />

# COMMAND ----------

# MAGIC %md ## AutoMLが完了した後は

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/automl_compare.jpg' />

# COMMAND ----------

# MAGIC %md ## モデルごとのmetrics比較

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/mlflow_comapre.jpg' />

# COMMAND ----------

# MAGIC %md ## ベストノートブックからのチューニング

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/automl_best_notebook.jpg' />

# COMMAND ----------

# MAGIC %md ## notebookのclone

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/clone_best_notebooks.jpg?' />

# COMMAND ----------

# MAGIC %md ## パラーメータを変更
# MAGIC 赤枠の箇所を変更・追記して、notebookの上位にあるRun Allをクリックしてください

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/country_holidays2.jpg' />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/mlflow_logparam.jpg' />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/run_all.jpg' />

# COMMAND ----------

# MAGIC %md ## パラメータ変更した状態でのmlflowを使ったmodel比較
# MAGIC 画面menuからExperiments -> automlで作成したExperimentsをクリック

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/re_compare_mlflow.jpg' />

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/parameter_change.jpg' />
