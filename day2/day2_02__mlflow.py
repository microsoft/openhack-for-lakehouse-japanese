# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC 
# MAGIC 実際にRawデータから加工してモデル学習＆デプロイまで構築するデモになります。以下のようなパイプラインを想定しております。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/overall.png' width='1200'/>

# COMMAND ----------

# MAGIC %md # Sales Forcasting
# MAGIC 
# MAGIC 加工したデータを使って、売上データの予測を手動で行います
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/1_createDelta.png' width='800' />

# COMMAND ----------

# MAGIC %pip install fbprophet

# COMMAND ----------

# MAGIC %run ./includes/read_variable

# COMMAND ----------

# DBTITLE 1,Define valiables
# 下準備
import re


# データベース名を生成
db_name = database
spark.sql(f"USE {db_name}")

# データベースを表示
print(f"database_name: {db_name}")

# COMMAND ----------

# DBTITLE 1,sv_order_itemsとsv_ordersをdeltaから読み出し
# order item
orderitemDF = spark.read \
          .format('delta') \
          .load("/FileStore/db_openhackason_2022/day2/team/database/db_open_hackason_day2_team/sv_order_items")

# orders
ordersDF = spark.read \
          .format("delta") \
          .load("/FileStore/db_openhackason_2022/day2/team/database/db_open_hackason_day2_team/sv_orders")

# COMMAND ----------

# MAGIC %md ### 注文数の時系列の推移

# COMMAND ----------

# DBTITLE 1,orderDFの型を確認
ordersDF.printSchema()

# COMMAND ----------

# DBTITLE 1,display()でデータの中身をチェック
# データの確認
display(ordersDF)

# COMMAND ----------

# DBTITLE 1,select()で必要なカラムのみを抽出
# timestampの加工
# order_statusがdelivered と shppedのみを抽出

from pyspark.sql.functions import *


display(ordersDF.select(date_format(col('order_purchase_timestamp'),'yyyy-MM-dd').alias('time').cast('date')))

# COMMAND ----------

# MAGIC %md ##Q1. 日付ごとの集計
# MAGIC 
# MAGIC 1.　ordersDFからorder_purchase_timestampと、orders_idの数を抽出して、gold tableを生成してください。<br>
# MAGIC その際、日付ごとで集計を行いたいので、order_purchase_timestampからはYY-MM-DDの形式で、timeをいう名前の列に変更してください。
# MAGIC <br>
# MAGIC <br>
# MAGIC 2. 　1で作成したDataframeをgoldテーブルとしてdelta table化してください テーブル名は("gd_orders_time")
# MAGIC <br>
# MAGIC <br>
# MAGIC 3.　 SQLで、時系列での売上数をグラフ化してください。
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

# 日毎の集計にするために、timeseriesを加工してgold tableを作成する
from pyspark.sql.functions import *

order_times = << FILL IN >>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 時系列でorderの状況を可視化
# MAGIC << FILL IN >>

# COMMAND ----------

# DBTITLE 1,printSchema()で、Dataframeの型を確認
order_times.printSchema()

# COMMAND ----------

# DBTITLE 1,order_timesのデータ確認
display(order_times)

# COMMAND ----------

# MAGIC %md ## 売上の可視化
# MAGIC 
# MAGIC ordersDFにはorder_idの情報しかなく、itemの売上数を計算するには、orderitemDFにあるpriceとfreight_valueを組み合わせる必要があります。

# COMMAND ----------

# DBTITLE 1,orderitemのDataFrameを確認
display(orderitemDF)

# COMMAND ----------

# DBTITLE 1,printSchema()で、Dataframeの型を確認
orderitemDF.printSchema()

# COMMAND ----------

# MAGIC %md ## Q2. データの加工
# MAGIC 
# MAGIC 
# MAGIC 1.　drop_orderitemDFとordersDF_nullをorder_idでjoinしてください。その際、必要なカラムは
# MAGIC - order_purchase_timestamp (日付で集計したいので、YY-MM-DDの形式で、dateという列名にしてください)
# MAGIC - price (型がstringのため、floatに.cast()を使って変換してください。)
# MAGIC - freight_value　(型がstringのため、floatに.cast()を使って変換してください。)
# MAGIC - sum_price (priceとfright_valueを合計した金額)
# MAGIC 
# MAGIC <br>
# MAGIC <br>
# MAGIC 2.  1で作成したDataframeをsilverテーブルとしてdelta table化してください テーブル名は(f"{db_name}.sv_orderitem_sales")
# MAGIC <br>
# MAGIC <br>
# MAGIC 3.　 SQLで、時系列での売上金額をグラフ化してください。
# MAGIC <br>
# MAGIC <br>

# COMMAND ----------

orderitem_joinDF = << FILL IN >>



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- 売上状況の可視化

# COMMAND ----------

# MAGIC %md ## 移動平均法

# COMMAND ----------

# MAGIC %md ##Q3. データのfiltering
# MAGIC 
# MAGIC - sales2017_DFとして、2017年のみをfilterして、dataframeを生成してください
# MAGIC - sales2018_DFとして、2018年のみをfilterして、dataframeを生成してください

# COMMAND ----------

# DBTITLE 1,2017年と2018年の売上データをみてみる
# 2017年の売上データを抽出する
from pyspark.sql.functions import *


sales2017_DF = << FILL IN >>
                  

# 2018年の売上データを抽出
# 問題にしてもいいかも
sales2018_DF = << FILL IN >>
                  

# COMMAND ----------

# MAGIC %md ## Q4. temporary tebleの作成
# MAGIC 
# MAGIC SQLで確認するために、一時テーブルの生成を行なってください

# COMMAND ----------

# DBTITLE 1,temporaryテーブルの生成
# SQLで確認するために、一時テーブルの生成
<< FILL IN >>

# COMMAND ----------

# DBTITLE 1,2017年の売り上げ推移
# MAGIC %sql
# MAGIC select ds, y from 2017_sales group by ds,y order by ds

# COMMAND ----------

# DBTITLE 1,2018年の売上推移
# MAGIC %sql
# MAGIC select ds, y from 2018_sales group by ds,y order by ds

# COMMAND ----------

# DBTITLE 1,移動平均のためのデータを生成
from pyspark.sql.window import Window
from pyspark.sql import functions as func


days = lambda i: i * 86400

sales2017_DF = sales2017_DF.select(col('ds').cast('timestamp'), col('y'))
sales2018_DF = sales2018_DF.select(col('ds').cast('timestamp'), col('y'))


windowSpec = Window.orderBy(func.col("ds").cast('long')).rangeBetween(-days(6), 0)
sales2017_SAM_DF = sales2017_DF.withColumn('rolling_seven_day_average', func.avg("y").over(windowSpec)) 
sales2018_SAM_DF = sales2018_DF.withColumn('rolling_seven_day_average', func.avg("y").over(windowSpec)) 

# COMMAND ----------

# DBTITLE 1,2017年の売上を移動平均としてみてみる
display(sales2017_SAM_DF)

# COMMAND ----------

# DBTITLE 1,2018年の売上を移動平均としてみてみる
# 2018年の移動平均
display(sales2018_SAM_DF)

# COMMAND ----------

# MAGIC %md ## 将来の売上予測
# MAGIC 
# MAGIC 作業中
# MAGIC prophetインストールすると、DFが消えるのでcluster側に最初からinstall

# COMMAND ----------

# DBTITLE 1,prophetで扱うために、spark dataframeからPandasに変換
# prophetで扱うために、dataframeをpandas dataframeに変換
import pandas as pd


pd_orderitem_joinDF = orderitem_joinDF.select("*").toPandas()

# COMMAND ----------

# DBTITLE 1,display()で確認
display(pd_orderitem_joinDF)

# COMMAND ----------

# MAGIC %md ## Q5. prophetのmodel作成
# MAGIC pd_orderitem_joinDFを使って、ProphetでModelを生成して、売上予測を実行してください。
# MAGIC また、実行結果をvisualizeしてください。
# MAGIC 
# MAGIC 
# MAGIC https://facebook.github.io/prophet/docs/quick_start.html#python-api

# COMMAND ----------

# DBTITLE 1,Prophetでmodel適用
from fbprophet import Prophet

<< FILL IN >>

# COMMAND ----------

# MAGIC %md ## Q6. mlflowで管理
# MAGIC 
# MAGIC - 下記を参考にmlflowを使って、実験をmlflowに登録してください。
# MAGIC 
# MAGIC https://databricks.com/notebooks/recitibikenycdraft/time-series.html

# COMMAND ----------

# MAGIC %md
# MAGIC Databricksは[MLflow](https://mlflow.org/)を用いて、自動的に全てのランを追跡します。MLflowのUIを用いて、構築したモデルを比較することができます。下のセルの実行後、画面右上にある**Experiment**ボタンを押してみてください。
# MAGIC 
# MAGIC <a href="https://www.mlflow.org/docs/latest/index.html"><img width=100 src="https://www.mlflow.org/docs/latest/_static/MLflow-logo-final-black.png" title="MLflow Documentation — MLflow 1.15.0 documentation"></a>
# MAGIC 
# MAGIC 参考情報：
# MAGIC - [PythonによるDatabricks MLflowクイックスタートガイド \- Qiita](https://qiita.com/taka_yayoi/items/dd81ac0da656bf883a34)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <head>
# MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
# MAGIC   <style>
# MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
# MAGIC   </style>
# MAGIC </head>
# MAGIC 
# MAGIC <div style='float:right; width:30%;'>
# MAGIC   <img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style='padding-right:300px;' width='100%'>
# MAGIC </div>
# MAGIC 
# MAGIC <h3>MLflowにて実験結果を保存する</h3>  
# MAGIC ❶ 機械学習に特化したロギング用のAPI。<br>
# MAGIC ❷ 学習を行う環境やライブラリーに依存すること無くトラッキング可能。<br>
# MAGIC ❸ データサイエンスのコードを実行すること、即ち `runs` した情報を全てトラッキング。<br>
# MAGIC ❹ この `runs` の結果が `experiments` として集約されて情報化。<br>
# MAGIC ❺ MLflow サーバではこれらの情報を管理
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <head>
# MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
# MAGIC   <style>
# MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
# MAGIC   </style>
# MAGIC </head>
# MAGIC <h3>各実行結果は下記を含む</h3>  
# MAGIC ❶ <strong>`パラメータ`</strong>: キーバリューペアーとしてインプットのパラメータを格納。例えば、ランダムフォレストモデルの木の数など。<br>
# MAGIC ❷ <strong>`メトリックス`</strong>: 評価指標の情報を格納。例えば、RMSEやROC曲線の情報など。<br>
# MAGIC ❸ <strong>`アーティファクト`</strong>: 任意形式のアウトプットを格納。モデルやプロット等の成果物。<br>
# MAGIC ❹ <strong>`ソース`</strong>: 実験を実行したコードを格納。<br>
# MAGIC </div>
# MAGIC <!--<p>実験のトラッキングは、Python, R, Java 等のライブラリを利用して行われます。また、CLI や REST でもトラッキング可能です。</p>-->
# MAGIC <p>ワークフローを通じて、モデルを開発段階からproduction(実運用段階)に昇格させ、RESTサーバでモデルを提供することも可能です。</p>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### ログの取得方法は以下の通りとなります。
# MAGIC <head>
# MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
# MAGIC   <style>
# MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
# MAGIC   </style>
# MAGIC </head>
# MAGIC ❶ まず最初に [mlflow.start_run()](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.start_run) を実行<br> 
# MAGIC ❷ ブロック内でモデル学習を実行<br>
# MAGIC ❸ モデルをロギングするために [mlflow.spark.log_model()](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.log_model) を実行<br> 
# MAGIC ❹ ハイパーパラメーターをロギングするために [mlflow.log_param()](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_param) を実行<br> 
# MAGIC ❺ モデルメトリックスをロギングするために [mlflow.log_metric()](https://www.mlflow.org/docs/latest/python_api/mlflow.html#mlflow.log_metric) を実行<br>
# MAGIC 
# MAGIC きめ細かい指定をするためには上のメソッドを使用しますが、デフォルトのパラメーター、メトリクスをロギングするので十分であれば、`mlflow.spark.autolog`によるオートロギングを利用できます。
# MAGIC 
# MAGIC [mlflow.spark.autolog](https://www.mlflow.org/docs/latest/python_api/mlflow.spark.html#mlflow.spark.autolog)

# COMMAND ----------

# DBTITLE 1,mlflowで管理
# mlflowのインポート
import mlflow
import mlflow.spark
import json
from prophet import serialize
from prophet.diagnostics import cross_validation, performance_metrics


ARTIFACT_PATH = "model"
experiment_name = f"/Users/{username_raw}/databricks_automl/project-olist-sales-forcast-2nd"
mlflow.set_experiment(experiment_name)

<< FILL IN >>

# COMMAND ----------

#予測
plot.plot_plotly(model, ml_flow_forecast)

# COMMAND ----------

# MAGIC %md ## mlflowで作成したExperimentsをクリックします
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/mlflow_experiments2.jpg' />

# COMMAND ----------

# MAGIC %md ## mlflowから作成したモデルをModel registryに登録
# MAGIC <br>
# MAGIC </br>
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/regist_model.jpg' />
# MAGIC <br>
# MAGIC </br>
# MAGIC 
# MAGIC **ご自身のお名前をいれたmodel名にしてください** 
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/register_model.jpg' />
# MAGIC <br>
# MAGIC </br>
# MAGIC 
# MAGIC **赤枠をクリックしてください** 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/regist_model2.jpg' />
# MAGIC <br>
# MAGIC </br>
# MAGIC 
# MAGIC **Transit to productionをクリックします** 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/transit_to_prod.jpg' />
# MAGIC <br>
# MAGIC </br>
# MAGIC **この作業を実施することで、DatabricksのModel Registryに登録が行われ、mlflowのAPIやsparkから呼び出すことが可能になります。modelの確認はサイドバーからでも確認可能です**

# COMMAND ----------

# MAGIC %md ## 複数作成したmlflowでの確認
# MAGIC 
# MAGIC **通常は複数モデルをがあるため、mlflowを使って各modelのパラメータ比較などを行うのが一般的です。**
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/automl_compare.jpg' />

# COMMAND ----------

# MAGIC %md-sandbox ## Feature Storeに保存
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC 機能の準備ができたら、Databricks Feature Storeに保存します。
# MAGIC その際、フィーチャーストアはDelta Lakeのテーブルでバックアップされます。
# MAGIC これにより、組織全体で機能の発見と再利用が可能になり、チームの効率が向上します。
# MAGIC 
# MAGIC フィーチャーストアは、デプロイメントにトレーサビリティとガバナンスをもたらし、どのモデルがどの機能のセットに依存しているかを把握することができます。
# MAGIC UIを使用してフィーチャーストアにアクセスするには、"Machine Learning "メニューを使用していることを確認してください。

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

dbName = f'olist_db_{user_name}'   # Database name
featureTableName = 'item_sales'     # Table name

spark.sql(f'create database if not exists {dbName}')

#olist_feature_table = fs.create_feature_table( ## 
olistfeature_table = fs.create_table(
  name=f'{dbName}.{featureTableName}',
  primary_keys='ds',
  schema=orderitem_joinDF.schema,
  description='これらの特徴は、olist csvのorderDFとorderitemDFをマージして、prophet用に加工したデータになります'
)

fs.write_table(df=orderitem_joinDF, name=f'{dbName}.{featureTableName}', mode='overwrite')

# COMMAND ----------

# MAGIC %md ## challenge1 ハイパーパラメーターチューニング
# MAGIC 
# MAGIC 交差検証のパラメータを変更してみたりして、結果がどう変わるかなど試してみてください。
# MAGIC 
# MAGIC https://facebook.github.io/prophet/docs/diagnostics.html#cross-validation
# MAGIC 
# MAGIC https://qiita.com/tchih11/items/42fc0d52a1486ba64b5d
