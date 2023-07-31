# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC ## 01. MLflow & Databricks AutoML を使った ML パイプラインの作成 (目安 10:30~12:30)
# MAGIC ### 本ノートブックの目的：MLflow を用いた MLOps への理解を深める
# MAGIC
# MAGIC Q1. display メソッドによる EDA<br>
# MAGIC Q2. ydata-profiling による EDA<br>
# MAGIC Q3. bamboolib による EDA<br>
# MAGIC Q4. Databricks AutoML によるトレーニングの実行<br>
# MAGIC Q5. MLflow Tracking への理解を深める<br>
# MAGIC Q6. MLflow によるモデルの管理<br>
# MAGIC Q7. Pandas Dataframe による推論

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Databricks が提唱している MLOps の構成要素をベースに MLOps を実践してください。1 から 6 までの方法論を本ノートブックで、`7. モデル再トレーニングの自動化`を次のノートブックである`day2_02__ml_pipeline`で、それぞれ実施するようになっています。<br>
# MAGIC <br>
# MAGIC **MLOps の構成要素**
# MAGIC
# MAGIC 1. 探索的データ解析（EDA）
# MAGIC 2. データ準備と特徴量エンジニアリング
# MAGIC 3. モデルのトレーニングとチューニング
# MAGIC 4. モデルのレビューとガバナンス
# MAGIC 5. モデル推論とサービング
# MAGIC 6. モデル監視
# MAGIC 7. モデル再トレーニングの自動化
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <img src='https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day2_01__mlops_intro/mlops-components.png'/>
# MAGIC
# MAGIC 引用元：[MLOps とは (databricks.com)](https://www.databricks.com/jp/glossary/mlops)

# COMMAND ----------

# MAGIC %pip install ydata-profiling -q

# COMMAND ----------

# MAGIC %pip install bamboolib -q

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 事前準備

# COMMAND ----------

tgt_tbl_name = "sales_history_gold__2017"

# COMMAND ----------

# データ準備に関する関数を定義
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

# 関数によりデータフレームを作成
src_df = create_order_info_gold_df()

# 検証用データとして`2018-01-1`以前のデータを抽出
src_df = src_df.filter("purchase_date < CAST('2018-01-1' AS Date)")

# 本ノートブックで利用するテーブルへ書き込み
overwrite_to_spark_tbl(
    src_df,
    tgt_tbl_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 探索的データ解析（EDA）
# MAGIC
# MAGIC Databricks にて３つの EDA の実施方法を実践します。それぞれの機能（ライブラリ）には次のような特徴があります。
# MAGIC
# MAGIC 1. `display`では、データフレームの可視化を簡単に実施可能
# MAGIC 2. `ydata-profiling`では、Spark Dataframe にてデータを網羅的な確認が実施可能
# MAGIC 3. `bamboolib`では、ローコードでデータ探索を実施可能

# COMMAND ----------

# Spark Dataframe の作成
spark_df = spark.table(tgt_tbl_name)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q1. display メソッドによる EDA**
# MAGIC
# MAGIC display メソッドの実行結果に基づき、次の作業を実施してください。
# MAGIC
# MAGIC - `Visualization`にて、X軸に`puchase_date`を、Y軸に`sales`を設定した Line chart の作成
# MAGIC - `Data Profile`の実行
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Visualizations in Databricks notebooks - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/visualizations/)

# COMMAND ----------

# ToDo display メソッドの実行後、Visualization を作成
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q2. ydata-profiling による EDA**
# MAGIC
# MAGIC [Pandas-Profiling Now Supports Apache Spark | Databricks Blog](https://www.databricks.com/blog/2023/04/03/pandas-profiling-now-supports-apache-spark.html) の記事に基づき、`ydata-profiling`を実行してください。
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [ydata-profiling/examples/features/spark_example.py at master · ydataai/ydata-profiling · GitHub](https://github.com/ydataai/ydata-profiling/blob/master/examples/features/spark_example.py)

# COMMAND ----------

from ydata_profiling import ProfileReport

# COMMAND ----------

# ToDo ydata-profiling を実行
report = ProfileReport(
    spark_df,
    infer_dtypes=False,
    interactions=None,
    missing_diagrams=None,
    correlations={
        "auto": {"calculate": False},
        "pearson": {"calculate": True},
        "spearman": {"calculate": True},
    },
)
report_html = report.to_html()
displayHTML(report_html)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q3. bamboolib による EDA**
# MAGIC
# MAGIC `bamboolib UI`にて次の作業を実施してください。
# MAGIC
# MAGIC 1. `bamboolib`をインポート後に Pandas Dataframe を表示して bamboolib UI へ切り替え
# MAGIC     1. Spark Dataframe から変換した Pandas Dataframe を実行
# MAGIC     2. `Show bamboolib UI`を選択
# MAGIC 2. `purchase_date`列を`Object`型から`datetime`型に変換
# MAGIC     1. `Search actions`にて`To Datetime`を選択
# MAGIC     2. `To Datetime`ウィンドウにて、`Change data type of column`に`purchase_date`を選択後、`Execute`を選択
# MAGIC     3. 下部に表示されているコードに、`pdf['purchase_date'] = pd.to_datetime ...`というコードが追加されたことを確認
# MAGIC 3. グラフを作成
# MAGIC     1. `Create plot`を選択して、`Plot Creator`タブが追加されることを確認
# MAGIC     2. `Figure type`にて`Line plot`を選択
# MAGIC     3. `Search properties`にて`xAxis`を選び、`purchase_date`を選択
# MAGIC     4. `Search properties`にて`yAxis`を選び、`sales`を選択
# MAGIC     5. グラフが表示されることを確認
# MAGIC 4. 生成されたコードを貼り付けて実行
# MAGIC     1. `Data`タブのコードを貼り付けて実行
# MAGIC     2. `Plot Creator`タブのコードを貼り付けて実行
# MAGIC     3. `bamboolib UI` と同様のグラフとなることを確認

# COMMAND ----------

# Spark Datafram を Pandas Dataframe に変換
pdf = spark_df.toPandas()

# カラム名とデータ型を表示（`purchase_date`列が`object`型であることを確認）
print(pdf.dtypes)

# データフレームを表示
pdf

# COMMAND ----------

# ToDo bamboolib を利用
import bamboolib as bam
pdf

# COMMAND ----------

# ToDo `Data`タブのコードを貼り付けて実行
import pandas as pd; import numpy as np
# Step: Change data type of purchase_date to Datetime
pdf['purchase_date'] = pd.to_datetime(pdf['purchase_date'], infer_datetime_format=True)

# COMMAND ----------

# ToDo `Plot Creator`タブのコードを貼り付けて実行
import plotly.express as px
fig = px.line(pdf.sort_values(by=['purchase_date'], ascending=[True]), x='purchase_date', y='sales')
fig

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. データ準備と特徴量エンジニアリング

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. データ準備

# COMMAND ----------

# `事前準備`にて実施済みであるため、省略
# src_df = create_order_info_gold_df()

# overwrite_to_spark_tbl(
#     src_df,
#     tgt_tbl_name,
# )

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
# MAGIC ## 3. モデルのトレーニングとチューニング
# MAGIC
# MAGIC モデルのトレーニングとチューニングを実施する際には主に次の項目を検討する必要があり、Databricks AutoML の利用によりそれらの要件を満たす再利用可能なノートブックにより ML モデルのトレーニングを実施できます。
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 1. ML モデルのトレーニング
# MAGIC 2. モデル選択とハイパーパラメータの最適化
# MAGIC 3. ML モデルのトレーニング管理（MLflow へのトラッキング等）

# COMMAND ----------

# MAGIC %md
# MAGIC Q4. Databricks AutoML によるトレーニングの実行
# MAGIC
# MAGIC `Databricks AutoML`にて次の作業を実施してください。
# MAGIC
# MAGIC 1. Databricks AutoML を実行
# MAGIC     1. 左側のメニューにある`Experiments`を選択
# MAGIC     2. 右上の`Create AutoML Experiment`を選択
# MAGIC     3. 下記にある Databricks AutoML 実行時のパラメーター表の値を設定
# MAGIC     4. `Start AutoML`を選択
# MAGIC 2. experiment の実行確認
# MAGIC     1. `Refresh`を選択して、処理が進捗状況を確認
# MAGIC     2. Workspace の`/Users/{user_id}/databricks_automl`フォルダにノートブックが生成されていることを確認
# MAGIC     3. 実行した experiment に戻り、`AutoML Evaluation`の項目値が`complete`となることを確認
# MAGIC 3. Databricks AutoML による EDA 結果を確認
# MAGIC     1. 実行した experiment にて、`View data exploration notebook`を選択して、EDA が実行されているノートブックに移動
# MAGIC     2. EDA が実行されていることを確認
# MAGIC 4. Databricks AutoML による trial（ML モデルのトレーニング結果）を確認
# MAGIC     1. 実行した experiment にて、`val_smape`を基準に最も精度が高い`Run Name`の項目を確認
# MAGIC     2. `View data exploration notebook`を選択して、最も精度が高い`Run Name`のノートブックに移動することを確認
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC **Databricks AutoML 実行時のパラメーター表**
# MAGIC
# MAGIC | #    | 項目                           | 値                                                         |
# MAGIC | ---- | ------------------------------ | ---------------------------------------------------------- |
# MAGIC | 1    | Cluster                        | 現在利用している Databricks Runtime for Machine Learning のクラスターを指定 |
# MAGIC | 2    | ML problem type                | Forecasting                                                |
# MAGIC | 3    | Input training dataset *1      | {current_schema}.sales_history_gold_2017                   |
# MAGIC | 4    | Prediction target              | sales                                                      |
# MAGIC | 5    | Time column                    | purchase_date                                              |
# MAGIC | 6    | Forecast horizon and frequency | 90                                                         |
# MAGIC | 7    | Country Holidays *2            | Brazil                                                     |
# MAGIC | 8    | Timeout (minutes) *2           | 30                                                     |
# MAGIC
# MAGIC
# MAGIC
# MAGIC *1 `current_schema`については、次のセルの出力結果に書き換える必要があります。
# MAGIC
# MAGIC *2 `Advanced Configuration (optional)`を選択することで表示される項目です。

# COMMAND ----------

# `current_schema`の確認
current_schema = spark.catalog.currentDatabase()
print(current_schema)

# COMMAND ----------

# Python API にて Databricks AutoML を実行する場合には次のコードを実行
# import databricks.automl
# import logging
# import datetime

# src_dataset = f"{current_schema}.{tgt_tbl_name}"

# ts_str = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
# experiment_name = f"sales_sales_history_gold-{ts_str}"
# print(f"experiment_name: {experiment_name}")

# # Disable informational messages from fbprophet
# logging.getLogger("py4j").setLevel(logging.WARNING)

# summary = databricks.automl.forecast(
#     src_dataset,
#     country_code="BR",
#     target_col="sales",
#     time_col="purchase_date",
#     horizon=90,
#     frequency="d",
#     primary_metric="smape",
#     experiment_name=experiment_name,
#     timeout_minutes=30,
# )

# COMMAND ----------

# Databricks AutoML の実行結果を表示
# print(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q5. MLflow Tracking への理解を深める**
# MAGIC
# MAGIC 次の手順の実施により、Databricks AutoML にて生成されたノートブックにより MLflow へのトラッキングする項目を変更してください。
# MAGIC
# MAGIC 1. Databricks AutoML により最も精度が高いノートブックを現在のフォルダにクローン
# MAGIC 2. ノートブックにて、MLflow Tracking を実施しているセルを特定
# MAGIC 3. [mlflow.set_tag](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.set_tag)メソッドにて、`executed_by`をキーに`databricks_automl`を値に設定することで tag としてトラッキング
# MAGIC 4. ノートブックにて、グラフをプロットしているセルを特定
# MAGIC 5. [mlflow.log_figure](https://mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_figure)メソッドにて、作成されている画像をトラッキング
# MAGIC 6. ノートブックをすべて実行
# MAGIC 7. 前の手順で作成した experiment から本手順の Run Name を選択
# MAGIC 8. トラッキング結果にて、次のメソッドにより、記録された項目を確認
# MAGIC     - mlflow.start_run
# MAGIC     - mlflow.set_tag
# MAGIC     - mlflow.log_param
# MAGIC     - mlflow.log_metrics
# MAGIC     - mlflow_prophet_log_model、あるいは、mlflow_arima_log_model
# MAGIC     - mlflow.log_figure
# MAGIC
# MAGIC MLflow へのトラッキングを実施したことがない場合には、本手順を実施する前に、次のノートブックをインポートして実行することを検討してください。
# MAGIC
# MAGIC - [MLflowLoggingAPIPythonQuickstart - Databricks (microsoft.com)](https://learn.microsoft.com/ja-jp/azure/databricks/_extras/notebooks/source/mlflow/mlflow-logging-api-quick-start-python.html)
# MAGIC
# MAGIC
# MAGIC `mlflow.set_tag`のサンプルコードを、次に示します。
# MAGIC
# MAGIC ```python
# MAGIC mlflow.set_tag("executed_by", "databricks_automl")
# MAGIC ```
# MAGIC
# MAGIC `mlflow.log_figure`のサンプルコードを、次に示します。画像が複数ある場合には、異なるファイル名にする必要があります。
# MAGIC
# MAGIC ```python
# MAGIC with mlflow.start_run(run_id=mlflow_run.info.run_id):
# MAGIC     mlflow.log_figure(fig, "figures/figure_01.png")
# MAGIC ```
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Python のクイック スタート - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/mlflow/quick-start-python)
# MAGIC - [Logging functions](https://mlflow.org/docs/latest/tracking.html#logging-functions)
# MAGIC

# COMMAND ----------

# ToDo Q5 の実施中に手動実行した際の RUN_ID を設定
manual_run_id = ""

# COMMAND ----------

# MLflow 上の画像をダウンロード
if manual_run_id != "":
    import mlflow
    from IPython.display import Image

    fig_path = mlflow.artifacts.download_artifacts(
        f"runs:/{manual_run_id}/figures/figure_01.png",
    )
else:
    pass

# COMMAND ----------

# 画像を表示する場合にはコメントアウトを解除して実行
# display(Image(fig_path))

# COMMAND ----------

# ダウンロードした画像を削除
if manual_run_id != "":
    import os

    os.remove(fig_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. モデルのレビューとガバナンス

# COMMAND ----------

# MAGIC %md
# MAGIC **Q6. MLflow によるモデルの管理**
# MAGIC
# MAGIC MLflow によるモデルの管理を行うため、次の作業を実施してください。
# MAGIC
# MAGIC 1. モデルの作成
# MAGIC     1. 左側のメニューにある`Models`を選択
# MAGIC     2. `Create Model`を選択
# MAGIC     3. `Model name`に次のセルの実行結果（例：`sales_prediction__by_team`）を貼り付て、`Create`を選択
# MAGIC     4. モデルが作成されたことを確認
# MAGIC 2. experiment の登録
# MAGIC     1. 左側のメニューにある`Experiments`を選択
# MAGIC     2. `Q3. モデルのトレーニングとチューニングの実践`にて作成した experiment を表示
# MAGIC     3. `val_smape`を基準に最も精度が高い`Run Name`の項目を選択
# MAGIC     4. `Register Model`を選択
# MAGIC     5. `Model`にて作成済みモデルを選択して、`Register`を選択
# MAGIC     6. 作成済みモデルに移動して、`Versions`に項目が追加されていることを確認
# MAGIC 3. モデルのステータスの変更
# MAGIC     1. 左側のメニューにある`Modesl`を選択
# MAGIC     2. 作成済みモデルを選択
# MAGIC     3. `Version 1`を選択
# MAGIC     4. `Stage`を選択後、`Transition to → Production`を選択
# MAGIC     5. `Stage Transition`ウィンドウにて、`OK`を選択
# MAGIC     6. 作成済みモデルのウィンドウに戻り、`Version 1`の`Stage`が`Prodution`となっていることを確認

# COMMAND ----------

# モデル名を表示
model_name = f"sales_prediction__by_{user_name}"
print(model_name)

# COMMAND ----------

# `モデルの作成`を、コードで実行する場合には下記を実行
# from mlflow.tracking import MlflowClient

# client = MlflowClient()
# client.create_registered_model(model_name)

# COMMAND ----------

# `experiment の登録`を、コードで実行する場合には下記を実行
# import mlflow

# runid_to_register = "36c58003f3304bc988f447a7bd8db926"

# result = mlflow.register_model(
#     f"runs:/{runid_to_register}/model",
#     model_name,
# )

# COMMAND ----------

# `モデルのステータスの変更`を、コードで実行する場合には下記を実行
# from mlflow.tracking import MlflowClient
# client = MlflowClient()

# tgt_version = 1

# client.transition_model_version_stage(
#     name=model_name,
#     version=tgt_version,
#     stage="Production"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. モデル推論とサービング
# MAGIC
# MAGIC 2018年01月01日から 10 日間の期間のデータをもつデータフレームに基づき、`Production`ステージの ML モデルに基づいた推論結果を確認します。
# MAGIC
# MAGIC Model 上の ML モデルを利用する場合には、`pyfunc`メソッドにより推論する方法があり、次のようなコードを記述します。
# MAGIC
# MAGIC ```python
# MAGIC import mlflow.pyfunc
# MAGIC
# MAGIC model_name = "sk-learn-random-forest-reg-model"
# MAGIC model_version = 1
# MAGIC
# MAGIC model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{model_version}")
# MAGIC
# MAGIC model.predict(data)
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC import mlflow.pyfunc
# MAGIC
# MAGIC model_name = "sk-learn-random-forest-reg-model"
# MAGIC stage = "Staging"
# MAGIC
# MAGIC model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/{stage}")
# MAGIC
# MAGIC model.predict(data)
# MAGIC ```
# MAGIC
# MAGIC 引用元：[MLflow Model Registry — MLflow 2.4.1 documentation](https://mlflow.org/docs/latest/model-registry.html#fetching-an-mlflow-model-from-the-model-registry)
# MAGIC
# MAGIC
# MAGIC experiment 上の  ML モデルを`pyfunc`メソッドにより利用する場合には、次のようなコードを記述します。
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC from pyspark.sql.functions import struct, col
# MAGIC logged_model = 'runs:/4168e779719d4bbd97182b26186f6d32/model'
# MAGIC
# MAGIC # Load model as a Spark UDF. Override result_type if the model does not return double values.
# MAGIC loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')
# MAGIC
# MAGIC # Predict on a Spark DataFrame.
# MAGIC df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC logged_model = 'runs:/4168e779719d4bbd97182b26186f6d32/model'
# MAGIC
# MAGIC # Load model as a PyFuncModel.
# MAGIC loaded_model = mlflow.pyfunc.load_model(logged_model)
# MAGIC
# MAGIC # Predict on a Pandas DataFrame.
# MAGIC import pandas as pd
# MAGIC loaded_model.predict(pd.DataFrame(data))
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

tgt_stage = "Production"
tgt_model = f"models:/{model_name}/{tgt_stage}"

# COMMAND ----------

# タイムスタンプ型の列を持つデータフレームを作成する関数を定義
import datetime
import pandas as pd


def create_datetime_df(
    df_type="pandas",
    col_name="purchase_date",
    base_timestamp_str="2018-01-01 00:00:00",
    tgt_days=10,
):
    base_ts = datetime.datetime.strptime(base_timestamp_str, "%Y-%m-%d %H:%M:%S")

    src_ts_date = []
    for day in range(tgt_days):
        add_ts = base_ts + datetime.timedelta(days=day)
        src_ts_date.append(add_ts)

    if df_type.lower() == "pandas":
        src_pdf = pd.DataFrame(data={col_name: src_ts_date})
    elif df_type.lower() == "spark":
        src_pdf = spark.createDataFrame([{col_name: ts} for ts in src_ts_date])

    return src_pdf

# COMMAND ----------

# Spark Dataframe を作成
src_sdf_05 = create_datetime_df(
    df_type="spark",
    col_name="purchase_date",
    base_timestamp_str="2018-01-01 00:00:00",
    tgt_days=10,
)

display(src_sdf_05)

# COMMAND ----------

# 推論の実施とその結果のデータフレームを表示
import mlflow
from pyspark.sql.functions import struct, col

loaded_model = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=tgt_model,
    result_type="double",
)
src_sdf_05 = src_sdf_05.withColumn(
    "predictions", loaded_model(struct(*map(col, src_sdf_05.columns)))
)

display(src_sdf_05)

# COMMAND ----------

# MAGIC %md
# MAGIC **Q7. Pandas Dataframe による推論**
# MAGIC
# MAGIC Pandas Dataframe にて推論を実施してください。

# COMMAND ----------

# Pandas Dataframe を作成
src_pdf_05 = create_datetime_df(
    df_type="pandas",
    col_name="purchase_date",
    base_timestamp_str="2018-01-01 00:00:00",
    tgt_days=10,
)

display(src_pdf_05)

# COMMAND ----------

# ToDo
import pandas as pd

loaded_model = mlflow.pyfunc.load_model(tgt_model)

predicted_pdf_05 = loaded_model.predict(src_pdf_05)
result_pdf_05 = pd.merge(
    src_pdf_05, predicted_pdf_05, left_index=True, right_index=True
)
display(result_pdf_05)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. モデル監視の実践
# MAGIC
# MAGIC 2018年1月1日から2018年3月31日までの期間の実データにて、予測結果と実績を比較します。

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

tgt_tbl_name_06 = "sales_history_gold__20181Q"

# COMMAND ----------

model_name = f"sales_prediction__by_{user_name}"
tgt_stage = "Production"
tgt_model = f"models:/{model_name}/{tgt_stage}"

# COMMAND ----------

# 2018年1月1日から2018年3月31日までのデータを保持したテーブルを作成
spark.sql(
    f"""
    CREATE
    OR REPLACE TABLE {tgt_tbl_name_06} AS (
    SELECT
        CAST(purchase_date AS timestamp) AS purchase_date,
        SUM(product_sales) sales
    FROM
        order_info_gold
    WHERE 
        purchase_date >= CAST('2018-01-1' AS Date)
        AND purchase_date < CAST('2018-04-1' AS Date)
    GROUP BY
        CAST(purchase_date AS timestamp)
    ORDER BY
        CAST(purchase_date AS timestamp) asc
    )
    """
)

# COMMAND ----------

# 推論結果列を保持したデータフレームを表示
import mlflow
from pyspark.sql.functions import struct, col

src_sdf_06 = spark.table(tgt_tbl_name_06)
cols_06 = src_sdf_06.columns

loaded_model = mlflow.pyfunc.spark_udf(
    spark,
    model_uri=tgt_model,
    result_type="double",
)
result_df_06 = src_sdf_06.withColumn("yhat", loaded_model(struct(*map(col, cols_06))))

# COMMAND ----------

# `sales`列と`yhat`（予測値）列のデータを分析
display(result_df_06)

# COMMAND ----------

# SMAPE (Symmetric mean absolute percentage error) を表示
smape_of_new_model = calc_smape(
    result_df_06,
    "sales",
    "yhat",
)

print(f"smape: {smape_of_new_model}")

# COMMAND ----------


