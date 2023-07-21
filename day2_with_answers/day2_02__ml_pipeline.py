# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC
# MAGIC ## 02. ML パイプラインによる自動化 (目安 12:30~13:00 + 14:00~15:00)
# MAGIC ### 本ノートブックの目的：モデル再トレーニングの自動化について理解を深める
# MAGIC
# MAGIC Q1. `sales_history_gold`の処理を記述<br>
# MAGIC Q2. Python API にて時系列予測を実施するコードを記述<br>
# MAGIC Q3. `experiment_id`と`best_run_id`をリターンするように記述<br>
# MAGIC Q4. Datbricks Wokflows による実行

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC `7. モデル再トレーニングの自動化`を行うために、１つ前ののノートブックに基づき ML パイプラインによる自動化を実践します。`2. データ準備と特徴量エンジニアリング`と`3. モデルのトレーニングとチューニング`を、Databricks Workflow から実行できるようなノートブックの作成を行ってください。
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_02__ml_pipeline/mlops-components.png'/>
# MAGIC
# MAGIC 引用元：[MLOps とは (databricks.com)](https://www.databricks.com/jp/glossary/mlops)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Widgets による変数値の切り替え
# MAGIC
# MAGIC ステージごとに異なる変数値を定義する場合には、Databricks Widgets を利用することでパイプライン実行時に切り替えことができます。本ノートブックでは、次のステージ（デプロイ環境）を想定しています。
# MAGIC
# MAGIC 1. Development
# MAGIC 2. Staging
# MAGIC 3. Production
# MAGIC
# MAGIC Databricks Workflows からジョブを実行する場合には`Parameters`にてパラメータを設定できます。`dbutils.notebook.run`でジョブを実行する場合には、次のようなコードにて、`arguments`引数に辞書型変数を渡すことでパラメータを設定できます。
# MAGIC
# MAGIC ```python
# MAGIC nb_results = dbutils.notebook.run(
# MAGIC     'day2_02__ml_pipeline',
# MAGIC     0,
# MAGIC     {"stage":"Production"},
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC パラメータは、Databricks Widgets の値を取得する`dbutils.widgets.get`メソッドにより取得できます。
# MAGIC
# MAGIC ```python
# MAGIC dbutils.widgets.get("stage")
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Databricks ウィジェット - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/widgets)
# MAGIC - [Databricks ノートブックを別のノートブックから実行する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/notebook-workflows)
# MAGIC

# COMMAND ----------

# widget を設定
dbutils.widgets.text("stage", "staging")

# COMMAND ----------

# widget の値を変数にセット
stage = dbutils.widgets.get("stage")
print(f"stage: {stage}")

# COMMAND ----------

# stage に応じた変数をセット
if stage.lower() == "production":
    tgt_tbl_name = "sales_history_gold"
if stage.lower() == "staging":
    tgt_tbl_name = "sales_history_gold__staging"
if stage.lower() == "development":
    tgt_tbl_name = "sales_history_gold__2017"

print(f"tgt_tbl_name: {tgt_tbl_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## `2. データ準備と特徴量エンジニアリング`の自動化

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. データ準備

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. `sales_history_gold`の処理を記述**
# MAGIC
# MAGIC １つ前のノートブックに記載されている`sales_history_gold`の処理時に利用する関数を記述してください。

# COMMAND ----------

# ToDo データ準備に関する関数を定義
def create_order_info_gold_df():
    sql = """
    SELECT
        CAST(purchase_date AS date),
        CAST(SUM(product_sales) AS int) sales
    FROM
        order_info_gold
    GROUP BY
        CAST(purchase_date AS date)
    ORDER BY
        CAST(purchase_date AS date) asc
    """
    src_df = spark.sql(sql)
    return src_df


def overwrite_to_spark_tbl(
    tgt_df,
    tgt_tbl_name,
    overwriteSchema_value="true",
):
    return (
        tgt_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", overwriteSchema_value)
        .saveAsTable(tgt_tbl_name)
    )


# COMMAND ----------

# データの読み込みと書き込みに関する関数を実行
src_df = create_order_info_gold_df()

overwrite_to_spark_tbl(
    src_df,
    tgt_tbl_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. データの前処理

# COMMAND ----------

# 今回は省略

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. 特徴量エンジニアリング

# COMMAND ----------

# 今回は省略

# COMMAND ----------

# MAGIC %md
# MAGIC ## `3. モデルのトレーニングとチューニング`の自動化

# COMMAND ----------

# `current_schema`の取得
current_schema = spark.catalog.currentDatabase()
print(current_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2. Python API にて時系列予測を実施するコードを記述**

# COMMAND ----------

# ToDo
import databricks.automl
import logging
import datetime

src_dataset = f"{current_schema}.{tgt_tbl_name}"

ts_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
experiment_name = f"sales_sales_history_gold-{ts_str}"
print(f"experiment_name: {experiment_name}")

# Disable informational messages from fbprophet
logging.getLogger("py4j").setLevel(logging.WARNING)

summary = databricks.automl.forecast(
    src_dataset,
    country_code="BR",
    target_col="sales",
    time_col="purchase_date",
    horizon=90,
    frequency="d",
    primary_metric="smape",
    experiment_name=experiment_name,
    timeout_minutes=30,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブック実行後のリターン値を設定
# MAGIC

# COMMAND ----------

# experiment_id と最も精度が高い Mlflows の run_id を取得
experiment_id = summary.experiment.experiment_id
best_run_id = summary.best_trial.mlflow_run_id

# COMMAND ----------

# MAGIC %md
# MAGIC **Q3. `experiment_id`と`best_run_id`をリターンするように記述**
# MAGIC
# MAGIC 次のコードを参考に、ノートブック実行後のリターン値に関するコードの記述してくください。
# MAGIC
# MAGIC ```python
# MAGIC import json
# MAGIC dbutils.notebook.exit(json.dumps({
# MAGIC   "status": experiment_name,
# MAGIC   "table": "my_data"
# MAGIC }))
# MAGIC ```
# MAGIC 引用元：[Databricks ノートブックを別のノートブックから実行する - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/notebooks/notebook-workflows#pass-structured-data)

# COMMAND ----------

# ToDo
import json

dbutils.notebook.exit(
    json.dumps(
        {
            "experiment_id": experiment_id,
            "best_run_id": best_run_id,
        }
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Wokflows による実行後

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Q4. Datbricks Wokflows による実行**
# MAGIC
# MAGIC 本ノートブックを Datbricks Wokflows により実行してください。
# MAGIC
# MAGIC 1. 右上にある`Schedule`を選択
# MAGIC 2. `schedule`を`Manual`に、`Cluster`を現在利用しているクラスターに設定して、`Create`を選択
# MAGIC 3. 左側のメニューにある`Workflows`を選択
# MAGIC 4. `Job`タブにて、本ノートブック名のジョブを選択
# MAGIC 4. `Run Now`を選択して、ジョブが実行されたことを確認
# MAGIC 5. ジョブ完了後、ジョブの実行結果を確認

# COMMAND ----------


