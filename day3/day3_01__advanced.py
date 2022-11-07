# Databricks notebook source
# MAGIC %md # Hack Day 3
# MAGIC 
# MAGIC 売上予測データを追加した状態ので機械学習を行います。

# COMMAND ----------

# MAGIC %pip install fbprophet

# COMMAND ----------

# MAGIC %run ./includes/read_variable

# COMMAND ----------

# DBTITLE 1,Define valiables
# Username を取得
#username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')

# データベース名を生成
db_name = database
spark.sql(f"USE {db_name}")

# データベースを表示
print(f"database_name: {db_name}")

# COMMAND ----------

# MAGIC %md ## Q1. Dataload復習
# MAGIC - 昨日作成したdelta table(gd_orderitem_sales)から、Dataframe : orderitem_joinDFとしてreadを行なってください
# MAGIC 
# MAGIC - orderitemDF_add_allとして、sourcepath配下にある20180830_olist_item_sales.csvをreadしてください
# MAGIC - orderitemDF_add2018として、sourcepath配下にある2018_olist_item_sales.csvをreadしてください
# MAGIC - orderitemDF_add2019として、sourcepath配下にある2019_olist_item_sales.csvをreadしてください
# MAGIC 
# MAGIC - orderitemDF_add2020として、sourcepath配下にある2020_olist_item_sales.csvをreadしてください
# MAGIC 
# MAGIC - orderitemDF_add2021として、sourcepath配下にある2021_olist_item_sales.csvをreadしてください

# COMMAND ----------

# DBTITLE 1,既存データの再Load
# Delta Tableからの読み込み

sourcePath = 'wasbs://mshack@sajpstorage.blob.core.windows.net/dataset/'

orderitem_joinDF = <<FILL IN>>

orderitemDF_add_all = <<FILL IN>>

orderitemDF_add2018 = <<FILL IN>>

orderitemDF_add2019 = <<FILL IN>>

orderitemDF_add2020 = <<FILL IN>>

orderitemDF_add2021 = <<FILL IN>>

# COMMAND ----------

# MAGIC %md ## Q2. Schema Check
# MAGIC orderitem_joinDFとorderitemDF_add2018(2019|2020|2021|all)のschematypeをチェックしてください。

# COMMAND ----------

# DBTITLE 1,Schema Chek
<<FILL IN>>

# COMMAND ----------

# MAGIC %md ## Q3. Cast 
# MAGIC orderitemDF_add2018/2019/2020/2021/allのds schemaをdate形式にcastしてください。

# COMMAND ----------

# DBTITLE 1,orderitemDF_add2018のds schemaをdateにcast
orderitemDF_add_all = << FILL IN >>
orderitemDF_add2018_cast = <<FILL IN>>
orderitemDF_add2019_cast = <<FILL IN>>
orderitemDF_add2020_cast = <<FILL IN>>
orderitemDF_add2021_cast = <<FILL IN>>

# COMMAND ----------

# DBTITLE 1,Schema Chek
orderitemDF_add_all_cast.printSchema()
orderitemDF_add2018_cast.printSchema()
orderitemDF_add2019_cast.printSchema()
orderitemDF_add2020_cast.printSchema()
orderitemDF_add2021_cast.printSchema()

# COMMAND ----------

# MAGIC %md ## Q4. DataFrame変換
# MAGIC orderitemDF_add_all_castをpandas dataframeに変換してください

# COMMAND ----------

# prophetで扱うために、dataframeをpandas dataframeに変換
import pandas as pd

pd_orderitem_join_all_union = << FILL IN >>

# COMMAND ----------

# MAGIC %md ## Q5. Model呼び出し
# MAGIC 昨日作成した予測モデルを呼び出して、pd_orderitem_join_allに適用し、visualizeしてください。
# MAGIC 
# MAGIC https://docs.databricks.com/applications/mlflow/model-registry-example.html#load-versions-of-the-registered-model-using-the-api

# COMMAND ----------

import mlflow.pyfunc

model_name = << FILL IN >>  # ご自分のmodel nameに変更ください
model_version = 'production'     # model_version = 'production' ## <= このようにproduction/stagingも指定可能

# Load model as a PyFuncModel.
loaded_model = << FILL IN >>

# Predict on a Pandas DataFrame.
import pandas as pd

forecast_pd_all = << FILL IN >>

# COMMAND ----------

# DBTITLE 1,実際の売上との比較は、、、
display(pd_orderitem_join_all)

# COMMAND ----------

# MAGIC %md ## Q6. 予測の解離
# MAGIC COVID19のタイミング(2019-12)から、売上が倍増しているような動きのようです。既存のモデルでは対応できないようです。
# MAGIC 2018年から2020年までのデータを結合して、2021年以降のデータをprophetを使って予測してみてください。
# MAGIC 
# MAGIC - 条件1:Prophetでmodel生成する際に、mlflowを使ってください
# MAGIC - 条件2:mlflowで作成した実験モデルを利用して、2021年のデータを予測してください。
# MAGIC     pandasからの呼び出しは、experimentsから対象experimentsのArtifacts->modelの箇所にPredict on a Pandas DataFrameがありますので、参考にしてください。

# COMMAND ----------

<< FILL IN >>

# COMMAND ----------

# DBTITLE 1,再度、全データを適用してみて解離がどうなるかを確認
import mlflow
logged_model = 'runs:/1d6ce383c8fa4c4895a6784a85659c36/model'

# Load model as a PyFuncModel.
loaded_model = mlflow.prophet.load_model(logged_model)

# Predict on a Pandas DataFrame.
import pandas as pd
forecast_pd_all_retry = loaded_model.predict(pd.DataFrame(pd_orderitem_join_all))


# COMMAND ----------

plot.plot_plotly(model, forecast_pd_all_retry)

# COMMAND ----------

model.plot_components(forecast_pd_all_retry)

# COMMAND ----------

from fbprophet.plot import add_changepoints_to_plot


fig = model.plot(forecast_pd_all_retry)
a = add_changepoints_to_plot(fig.gca(), model, forecast_pd_all_retry)

# COMMAND ----------

# MAGIC %md ## Challenge2 モデルドリフト
# MAGIC 
# MAGIC 今回のように、適切にデータを適用しないモデルを運用している場合、modelで予測した値と実際の値が解離することがあります。
# MAGIC モデルモニタリングは、最近のホットな話題です。一度導入した機械学習モデルは、様々な要因で時間とともに劣化していきます。これには以下が含まれますが、これらに限定されません。
# MAGIC - __データの品質__ （例：データパイプラインの中断、スキーマの変更など
# MAGIC - __フィーチャードリフト__ （例：モデルに渡されたフィーチャの根本的な分布の変化）。
# MAGIC - __ラベルのドリフト__ （例：予測されたラベルの時間的またはモデルのバージョン間での分布の違い）。
# MAGIC - __予測のドリフト__ (例：TN, TP, FP, FNなどのグランドトゥルースと比較したモデルのパフォーマンスメトリクスの違いなど。)
# MAGIC 
# MAGIC 今回は、用意したデータを使ってモデルドリフトをやってみましょう。
# MAGIC 手法、ツールは問いません。Evidentlyなどのライブラリや、シンプルに外れ値を検知するパターンでもOKです。
# MAGIC mlflowからの呼び出しではなく、再度modelを生成してもOKです。
