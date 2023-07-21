# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC
# MAGIC ## 03. 自動パイプラインで作成したモデルのレビューとガバナンス　(目安 15:00~16:45)
# MAGIC ### 本ノートブックの目的：自動パイプラインで作成した ML モデルの管理方法について理解を深める
# MAGIC
# MAGIC Q1. `dbutils.notebook.run`実行時のパラメータの確認<br>
# MAGIC Q2. MLflow Experinmet をソースとして Spark Dataframe の作成<br>
# MAGIC Q3. 推論結果を比較<br>
# MAGIC Q4. モデルへ新しい ML モデルを登録<br>

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2"

# COMMAND ----------

# モデル
model_name = f"sales_prediction__by_{user_name}"
tgt_stage = "Production"
existed_model_uri = f"models:/{model_name}/{tgt_stage}"

# COMMAND ----------

# タイムスタンプ型の列を持つデータフレームを作成する関数を定義
import datetime
import pandas as pd


def create_datetime_df(
    df_type="pandas",
    col_name="purchase_date",
    base_timestamp_str="2018-09-01 00:00:00",
    tgt_days=10,
):
    base_ts = datetime.datetime.strptime(base_timestamp_str, "%Y-%m-%d %H:%M:%S")

    src_ts_date = []
    for day in range(tgt_days):
        add_ts = base_ts + datetime.timedelta(days=day)
        src_ts_date.append(add_ts)

    if df_type.lower() == "pandas":
        src_df = pd.DataFrame(data={col_name: src_ts_date})
    elif df_type.lower() == "spark":
        src_df = spark.createDataFrame([{col_name: ts} for ts in src_ts_date])

    return src_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 自動パイプラインの実行

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. `dbutils.notebook.run`実行時のパラメータの確認**
# MAGIC
# MAGIC `dbutils.notebook.run`メソッドによりノートブックを実行し、Databricks Workflows にて実行時のパラメータを確認してください。`dbutils.notebook.run`実行後に表示されるリンク（例：`Notebook job #873696807316876`）を選択することで、実行中の Databricks Workflows のジョブに移動します。

# COMMAND ----------

# `day2_02__ml_pipeline`ノートブックの実行
nb_path = "day2_02__ml_pipeline"
timeout = 0
nb_paras = {
    "stage": "Production",
}
nb_results = dbutils.notebook.run(
    nb_path,
    timeout,
    nb_paras,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 自動パイプラインにて作成した ML モデルを確認

# COMMAND ----------

# ノートブックの実行結果を表示
import json

nb_results_dict = json.loads(nb_results)
print(nb_results_dict)

# COMMAND ----------

# リターン値をそれぞれ変数にセット
experiment_id_key_name = "experiment_id"
experiment_id = nb_results_dict[experiment_id_key_name]
print(f"experiment_id: {experiment_id}")

best_run_id_key_name = "best_run_id"
run_id = nb_results_dict[best_run_id_key_name]
print(f"run_id: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Q3. MLflow Experinmet をソースとして Spark Dataframe の作成**
# MAGIC
# MAGIC 次のコードを参考にして、`best_run_id`のみの情報を表示する Spark Dataframe を作成して、`display`メソッドで表示してください。
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.format("mlflow-experiment").load("3270527066281272")
# MAGIC display(df)
# MAGIC ```
# MAGIC
# MAGIC 引用元：[MLflow 実験 - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/external-data/mlflow-experiment)

# COMMAND ----------

# ToDo MLflow experiment に関する情報を表示
mlflow_df = spark.read.format("mlflow-experiment").load(experiment_id)
mlflow_df = mlflow_df.filter(f"run_id = '{run_id}'")
display(mlflow_df)

# COMMAND ----------

# Load the model
import mlflow.pyfunc

loaded_model = mlflow.pyfunc.load_model(f"runs:/{run_id}/model")

# メタデータを表示
print(loaded_model.metadata)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 新旧 ML モデルの比較

# COMMAND ----------

tgt_table = "sales_history_gold"
actual_col = "sales"
predict_col_with_new = "yhat_with_new_model"
predict_col_with_old = "yhat_with_old_model"

# COMMAND ----------

# 評価指標を計算する関数を定義
from pyspark.sql.functions import sum, median, expr


def calc_mdape(
    src_spark_df,
    predict_cols,
    actual_col,
):
    mdape = src_spark_df.select(
        median(expr(f"abs(({actual_col} - {predict_cols}) / {actual_col})"))
    ).first()[0]
    return mdape


def calc_smape(
    src_spark_df,
    predict_col,
    actual_col,
):
    cnt = src_spark_df.count()
    col_values = src_spark_df.select(
        sum(
            expr(
                f"2 * abs({predict_col} - {actual_col}) / (abs({predict_col}) + abs({actual_col}))"
            )
        )
    ).first()[0]
    smape = 100 / cnt * col_values
    return smape

# COMMAND ----------

# Spark Dataframe を作成
src_sdf = spark.table(tgt_table)
display(src_sdf)

# COMMAND ----------

# 推論を実施したデータフレームを作成
import mlflow
from pyspark.sql.functions import struct, col

# 新モデル
new_logged_model = f"runs:/{run_id}/model"
new_model = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=new_logged_model,
    result_type="double",
)
src_sdf = src_sdf.withColumn(
    predict_col_with_new, new_model(struct(*map(col, src_sdf.columns)))
)

# 旧モデル
old_model = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=existed_model_uri,
    result_type="double",
)
src_sdf = src_sdf.withColumn(
    predict_col_with_old, old_model(struct(*map(col, src_sdf.columns)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q3. 推論結果を比較する**
# MAGIC
# MAGIC `display`メソッドにて、実績と２つの推論結果を比較するグラフを作成してください。

# COMMAND ----------

# ToDo
display(src_sdf)

# COMMAND ----------

# SMAPE (Symmetric mean absolute percentage error) を比較
smape_of_new_model = calc_smape(
    src_sdf,
    predict_col_with_new,
    actual_col,
)

smape_of_old_model = calc_smape(
    src_sdf,
    predict_col_with_old,
    actual_col,
)

print(f"smape_of_new_model: {smape_of_new_model}")
print(f"smape_of_old_model: {smape_of_old_model}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 新しく作成した ML モデルを Models に登録

# COMMAND ----------

# モデル名を表示
print(model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q4. モデルへ新しい ML モデルを登録**
# MAGIC
# MAGIC Python コードにて、次の作業を実施してください。
# MAGIC
# MAGIC 1. 今回作成した RUN をモデルに登録
# MAGIC 2. 登録した Version の Stage を`Staging`に設定
# MAGIC 2. モデルにて 登録した Version の Stage が`Staging`になっていることを確認

# COMMAND ----------

runid_to_register = run_id
tgt_stage = "Staging"

# COMMAND ----------

# ToDo 今回作成した RUN をモデルに登録
import mlflow

result = mlflow.register_model(
    f"runs:/{runid_to_register}/model",
    model_name,
)

# COMMAND ----------

# 登録したバージョンを変数にセット
tgt_version = result.version
print(tgt_version)

# COMMAND ----------

# ToDo 登録した Version の Stage を Staging に変更
from mlflow.tracking import MlflowClient

client = MlflowClient()

trans_result = client.transition_model_version_stage(
    name=model_name,
    version=tgt_version,
    stage=tgt_stage,
)

print(trans_result)

# COMMAND ----------


