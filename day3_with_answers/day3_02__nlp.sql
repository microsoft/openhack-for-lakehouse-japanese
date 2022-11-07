-- Databricks notebook source
-- MAGIC %md # Hack Day 3
-- MAGIC 
-- MAGIC ストリーミングデータを使ってワードクラウドを作ります。

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <img style="margin-top:25px;" src="https://jixjiadatabricks.blob.core.windows.net/images/databricks-logo-small-new.png" width="140">
-- MAGIC <hr>
-- MAGIC ## 内容
-- MAGIC - ここではDeltaアーキテクチャを使ってストリームデータの処理を構築しつつ、Spark Structured Streamingの扱い方を見ていきます。<br>
-- MAGIC - また、バッチ処理と同様、ストリーミングデータに対する集計や加工などを段階的に実装していきますが、DeltaアーキテクチャではストリームのETL処理が非常に簡単に実現できます。<br>
-- MAGIC 
-- MAGIC 
-- MAGIC ### Deltaアーキテクチャとは
-- MAGIC - Deltaアーキテクチャとは、Delta Lakeを使ったストリーム処理とバッチ処理を融合させた新しいビッグデータ処理のアーキテクチャです。<br>
-- MAGIC - このアーキテクチャの最大の特徴は、動的なデータと静的なデータを同様に扱え、分散ストレージ（即ち、データレイク）上のオブジェクトに対して直接ETL・ELTが行える事です。<br>
-- MAGIC - この特徴により、データの速度（ストリーム or バッチ）や性質（構造化 or 半構造化）によって別々で処理する必要がなくなり、ビッグデータパイプラインの設計が著しく簡単になります。<br><br>
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/images/delta_architecture_demo.gif" width="1200px">
-- MAGIC 
-- MAGIC ## 何故従来のLambdaアーキテクチャではいけないのか？
-- MAGIC 従来のラムダアーキテクチャには幾つかの難点があります：     
-- MAGIC 1. バッチとストリームを別々で処理、パイプラインの設計が複雑     
-- MAGIC 2. データの品質管理と整合性を保つのがが困難である。データ追加後のメタデータREFRESHなどバッチとストリームデータの同期は容易ではない。    
-- MAGIC 3. ダイレクトなDML操作（更新や削除など）は不可なので、 既存データの更新にはテーブルそのものを丸ごと書き直す必要がある    
-- MAGIC <div><img src="https://jixjiadatabricks.blob.core.windows.net/mokmok/lambda_challenge.gif" width="1000px"/></div>
-- MAGIC        
-- MAGIC <br>
-- MAGIC ## Spark Structured Streamingとは
-- MAGIC Sparkではバッチ処理に非常に近い方法でストリーミングデータの処理が可能です。ストリームデータをマイクロバッチで小分けして、集計や処理を施します：
-- MAGIC <div style="position:relative; left:-50px">
-- MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/mokmok/streaming-microbatch-animated.gif" width="1200px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md ### ストリーミングデータの準備

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import re
-- MAGIC from pyspark.sql.types import * 
-- MAGIC 
-- MAGIC # Username を取得
-- MAGIC username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
-- MAGIC # Username の英数字以外を除去し、全て小文字化。Username をファイルパスやデータベース名の一部で使用可能にするため。
-- MAGIC username = re.sub('[^A-Za-z0-9]+', '', username_raw).lower()
-- MAGIC 
-- MAGIC # データベース名を生成:SQL Analyticsから参照します。
-- MAGIC db_name = f"delta_demo_{username}"
-- MAGIC 
-- MAGIC # データベースの準備
-- MAGIC spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
-- MAGIC spark.sql(f"USE {db_name}")
-- MAGIC 
-- MAGIC # データベースを表示
-- MAGIC print(f"database_name: {db_name}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #homeDir = '/home/streaming/wikipedia/' + username
-- MAGIC homeDir = '/tmp/streaming/' + username + '/'
-- MAGIC bronzePath = homeDir + "bronze.delta"
-- MAGIC bronzeCkpt = homeDir + "bronze.checkpoint"
-- MAGIC silverPath = homeDir + "silver.delta"
-- MAGIC silverCkpt = homeDir + "silver.checkpoint"
-- MAGIC goldPath = homeDir + "gold.delta"
-- MAGIC goldCkpt = homeDir + "gold.checkpoint"
-- MAGIC 
-- MAGIC # 保存先をリセット
-- MAGIC print(homeDir)
-- MAGIC dbutils.fs.rm(homeDir, True)
-- MAGIC dbutils.fs.rm(bronzePath , True)
-- MAGIC dbutils.fs.rm(silverPath , True)
-- MAGIC dbutils.fs.rm(goldPath , True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import from_json, col,count, window, col
-- MAGIC 
-- MAGIC #　JSONデータのスキーマ定義
-- MAGIC schema = StructType([
-- MAGIC   StructField("channel", StringType(), True),
-- MAGIC   StructField("comment", StringType(), True),
-- MAGIC   StructField("delta", IntegerType(), True),
-- MAGIC   StructField("flag", StringType(), True),
-- MAGIC   StructField("geocoding", StructType([
-- MAGIC     StructField("city", StringType(), True),
-- MAGIC     StructField("country", StringType(), True),
-- MAGIC     StructField("countryCode2", StringType(), True),
-- MAGIC     StructField("countryCode3", StringType(), True),
-- MAGIC     StructField("stateProvince", StringType(), True),
-- MAGIC     StructField("latitude", DoubleType(), True),
-- MAGIC     StructField("longitude", DoubleType(), True),
-- MAGIC   ]), True),
-- MAGIC   StructField("isAnonymous", BooleanType(), True),
-- MAGIC   StructField("isNewPage", BooleanType(), True),
-- MAGIC   StructField("isRobot", BooleanType(), True),
-- MAGIC   StructField("isUnpatrolled", BooleanType(), True),
-- MAGIC   StructField("namespace", StringType(), True),         
-- MAGIC   StructField("page", StringType(), True),              
-- MAGIC   StructField("pageURL", StringType(), True),           
-- MAGIC   StructField("timestamp", StringType(), True),        
-- MAGIC   StructField("url", StringType(), True),
-- MAGIC   StructField("user", StringType(), True),              
-- MAGIC   StructField("userURL", StringType(), True),
-- MAGIC   StructField("wikipediaURL", StringType(), True),
-- MAGIC   StructField("wikipedia", StringType(), True),
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %md ### Q1.Rawデータ(Bronzeテーブル)の準備
-- MAGIC Kafkaから読み込んだデータをデータフレームにロードし、そのデータフレームからテーブルを作成してください。

-- COMMAND ----------

Drop table if exists bronze;

-- COMMAND ----------

-- DBTITLE 0,Kafkaから読み込んだデータをデータフレームにロードし、そのデータフレームからテーブルを作成
-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.rm(bronzeCkpt, True)
-- MAGIC 
-- MAGIC #　JSONストリームを解析し、Deltaフォーマットで保存
-- MAGIC (spark
-- MAGIC   .readStream
-- MAGIC   .format('kafka')                          # Kafkaをソースと指定
-- MAGIC   .option('kafka.bootstrap.servers', 
-- MAGIC           'server2.databricks.training:9092') # Kafkaサーバ:ポートを指定
-- MAGIC   .option('subscribe', 'en')                # enというKafkaトピックからサブスクライブする
-- MAGIC   .load()
-- MAGIC   .withColumn('json', from_json(col('value').cast('string'), schema))   # Kafkaのバイナリデータを文字列に変換し、from_json()でJSONをパース
-- MAGIC   .select(col("json.*"))                    # JSONの子要素だけを取り出す
-- MAGIC   .writeStream                              # writeStream()でストリームを書き出す
-- MAGIC   .format('delta')                          # Deltaとして保存
-- MAGIC   .option('checkpointLocation', bronzeCkpt) # チェックポイント保存先を指定
-- MAGIC   .outputMode('append')                     # マイクロバッチの結果をAppendで追加
-- MAGIC   .queryName('Bronze Stream')               # ストリームに名前を付ける（推奨）
-- MAGIC   .table("bronze")                          # 書き込み先のテーブルが存在しなければ上で定義したスキーマ情報でテーブルを作成、テーブルが存在していればデータ追加
-- MAGIC                                             # あらかじめCREATE TABLE文で作成しておくこともできますが、今回はデータフレームのテーブル情報からテーブルを作成
-- MAGIC   #.start(bronzePath)                        # start()でストリーム処理を開始 (アクション)
-- MAGIC  
-- MAGIC )

-- COMMAND ----------

describe extended bronze;

-- COMMAND ----------

select * from bronze;

-- COMMAND ----------

-- MAGIC %md ### Q2.加工済みデータ(Silverテーブル)の準備
-- MAGIC Bronzeテーブルからデータを読み込み+加工処理し、Silverテーブルに継続的に書きこんでください。

-- COMMAND ----------

Drop table if exists silver;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import unix_timestamp, col
-- MAGIC 
-- MAGIC # 念のためディレクトリも削除しておく
-- MAGIC #dbutils.fs.rm(table_path + 'silver', True)
-- MAGIC dbutils.fs.rm(silverCkpt, True)
-- MAGIC 
-- MAGIC # ブロンズデータに対し次の処理を施す：
-- MAGIC # (1)  クエリに使うカラムを選定 (wikipedia, namespace, user, pageURL)
-- MAGIC # (2) 「geocoding」カラムのJSON子要素(CountryCode3)を取り出す
-- MAGIC # (3) 「timestamp」を解析し、Timestampフォーマットに直す
-- MAGIC # (4) 「CountryCode」がNullのデータを取り除く
-- MAGIC # (5)  ストリーム処理の結果をDeltaフォーマットでsilverPathへ保存
-- MAGIC 
-- MAGIC (spark
-- MAGIC   .readStream
-- MAGIC   .format("delta")
-- MAGIC   #.load(bronzePath)
-- MAGIC   .table("bronze")
-- MAGIC   .select(col("wikipedia"),
-- MAGIC           col("namespace"),
-- MAGIC           col("user"),
-- MAGIC           col("page"),
-- MAGIC           col("geocoding.countryCode3").alias('CountryCode'),
-- MAGIC           unix_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX").cast("timestamp").alias('timestamp'),
-- MAGIC           col("pageURL")
-- MAGIC           )
-- MAGIC   .filter(col('CountryCode').isNotNull())
-- MAGIC   .writeStream
-- MAGIC   .format("delta")
-- MAGIC   .option("checkpointLocation", silverCkpt)
-- MAGIC   .outputMode("append")
-- MAGIC   .queryName('Silver Stream')
-- MAGIC   .table("silver")
-- MAGIC   #.start(silverPath)
-- MAGIC )

-- COMMAND ----------

describe extended silver;

-- COMMAND ----------

select * from silver;

-- COMMAND ----------

-- MAGIC %md ### Q3.集計データ(Goldテーブル)の準備
-- MAGIC Silverテーブルの集計処理結果をGoldテーブルとして保存してください。

-- COMMAND ----------

Drop table if exists gold;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #念のためディレクトリも削除しておく
-- MAGIC #dbutils.fs.rm(table_path + 'gold', True)
-- MAGIC dbutils.fs.rm(goldCkpt, True)
-- MAGIC 
-- MAGIC ( # 上記セルと同様の処理ロジックにWatermarkingを付け加える
-- MAGIC   spark
-- MAGIC    .readStream
-- MAGIC    .format("delta")
-- MAGIC    .table("silver")
-- MAGIC    .withWatermark('timestamp','1 minutes')  # withWatermark()でWindowの有効期間を設ける
-- MAGIC                                             # 即ち一定時間以上遅れて到着したイベントは無視される
-- MAGIC    .groupBy(col("CountryCode"), window('timestamp', '1 minutes'))
-- MAGIC    .count()
-- MAGIC    .selectExpr('CountryCode', 
-- MAGIC               'window.start as Start', 
-- MAGIC               'window.end as End',
-- MAGIC               'count as Total')
-- MAGIC    .writeStream                             # Windowの集計結果をGoldテーブルに保存
-- MAGIC    .format('delta')
-- MAGIC    .option('checkpointLocation', goldCkpt)
-- MAGIC    .outputMode('append')
-- MAGIC    .queryName('Gold Stream')
-- MAGIC    .table("gold")
-- MAGIC    #.start(goldPath)
-- MAGIC )

-- COMMAND ----------

describe extended gold

-- COMMAND ----------

select * from gold order by start desc;

-- COMMAND ----------

-- MAGIC %md ### Q4.ダッシュボードの構築
-- MAGIC DatabricksSQLを使ってストリーミング集計結果をSQLでリアルタイムで表示してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #シルバーテーブルを明示的にREADストリームと定義し、SparkStreamingの処理で新規データを読み続け、リアルタイムデータを常に表示させる。
-- MAGIC #pythonでも可能だがSQLで実行してみる。
-- MAGIC 
-- MAGIC # SilverテーブルからREADストリームを作成
-- MAGIC silver_stream = spark.readStream.format("delta").table("silver")
-- MAGIC 
-- MAGIC # READストリームからTEMPテーブルを作成
-- MAGIC silver_stream.createOrReplaceTempView("silver_stream_table")

-- COMMAND ----------

-- シルバーのデータに対し次の処理を施す：
--(1) 「CountryCode」のカウントを1分毎に集計 (Window Function利用)
--(2) Windowのstart/end要素を抽出し、startタイムの順で並べ替え
--(3) カウントの結果を「Total」に命名
--(4) ストリーム処理結果を保存せず、リアルタイムで表示する

SELECT 
window, -- this is a reserved word that lets us define windows
window.start as Start,
window.end   as End,
CountryCode,
count(1) as Total
from silver_stream_table
GROUP BY 
CountryCode,
window(timestamp, '1 minutes') -- # withWatermark()でWindowの有効期間を1分に設定し、一定時間以上遅れて到着したイベントは無視することとする。
ORDER BY window;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Databricks SQLによる可視化サンプル
-- MAGIC <br><br>
-- MAGIC - [Databricks SQL \- Databricks](https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#notebook/2731314951812113)
-- MAGIC - [デモダッシュボード](https://e2-demo-west.cloud.databricks.com/sql/dashboards/62068a16-ddee-43e2-a3dd-85442cc14be5--?o=2556758628403379)
