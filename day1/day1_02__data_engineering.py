# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 02. データエンジニアリングパイプラインの実践(目安 10:00~11:30)
# MAGIC ### 本ノートブックの目的：Databricksにおけるテーブル作成、データ格納処理について理解を深める
# MAGIC Q1. Sparkデータフレーム操作によりデータの書き込みを実施してください。<br>
# MAGIC Q2. Sparkデータフレーム操作により書き込んだデータをData Explorerで確認する<br>
# MAGIC Q3. Databricksオートローダーによりデータの書き込みを実施してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. データフレーム操作によりデータの書き込みを実施してください。
# MAGIC 
# MAGIC 変数`src_file_path__2_1`にある区切りテキストファイルを読み取り、変数`tgt_table_name__2_1`のテーブルに上書き処理を実施してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2_1"

# COMMAND ----------

src_file_path__2_1 = 'dbfs:/databricks-datasets/tpch/data-001/lineitem/'
tgt_table_name__2_1 = 'lineitme'

schema__2_1 = """
  L_ORDERKEY    INTEGER ,
  L_PARTKEY     INTEGER ,
  L_SUPPKEY     INTEGER ,
  L_LINENUMBER  INTEGER ,
  L_QUANTITY    DECIMAL(15,2) ,
  L_EXTENDEDPRICE  DECIMAL(15,2) ,
  L_DISCOUNT    DECIMAL(15,2) ,
  L_TAX         DECIMAL(15,2) ,
  L_RETURNFLAG  STRING ,
  L_LINESTATUS  STRING ,
  L_SHIPDATE    DATE ,
  L_COMMITDATE  DATE ,
  L_RECEIPTDATE DATE ,
  L_SHIPINSTRUCT STRING ,
  L_SHIPMODE     STRING ,
  L_COMMENT      STRING
"""

table_ddl__2_1 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_1}
    (
        {schema__2_1},
        _src_file_path STRING
    )
    USING delta
"""

# COMMAND ----------

# TODO 変数`table_ddl__2_1`を用いて、テーブルを作成
spark.<< FILL IN >>

# COMMAND ----------

# テーブル定義を確認
spark.sql(f'DESC EXTENDED {tgt_table_name__2_1}').display()

# COMMAND ----------

from  pyspark.sql.functions import input_file_name

# TODO 変数`src_file_path__2_1`をソースとして、区切り文字を`|`としてデータフレームを作成してください。
df = << FILL IN >>

# TODO 関数`input_file_name`により、ソースファイルのパスを保持したカラム（`_src_file_path`列）をデータフレームに追加してください。
df = << FILL IN >>

# TODO 変数`tgt_table_name__2_1`を保存先のテーブルとして、`Delta`形式で上書き（`overwrite`）によりデータの書き込み実施してください。
(df
   .<< FILL IN >>
)

# COMMAND ----------

# ToDo 保存先のテーブル（`tgt_table_name__2_1`）のデータをdisplay関数で表示してください。
spark.<< FILL IN >>

# COMMAND ----------

# MAGIC %md
# MAGIC **Tips** : レイテンシーに応じた処理方法の選択
# MAGIC 
# MAGIC データの生成からデータが利用可能になるまでの時間差（レイテンシー）要件に応じて、データ処理方法を選択する必要があります。<br>
# MAGIC 
# MAGIC | #    | レイテンシーに応じた処理方法                     | Databricksの実装例                                           |
# MAGIC | ---- | ---------------------------- | ------------------------------------------------------------ |
# MAGIC | 1    | バッチ                       | 1-1. スケジュールトリガーによるSparkデータフレーム処理<br/>1-2. Delta live tableのトリガーパイプラインによる処理 |
# MAGIC | 2    | 準リアルタイムとイベント駆動 | 2-1. ファイル到着イベントトリガーによる実行               |
# MAGIC | 3    | ストリーミング               | 3-1. Sparkストリーミング処理<br/>3-2. Databricksオートローダーによる処理<br/>3-3. Delta live tableの連続パイプラインによる処理 |

# COMMAND ----------

# MAGIC %md
# MAGIC **Tips** : レイテンシーに応じた処理方法の選択
# MAGIC 
# MAGIC データの生成からデータが利用可能になるまでの時間差（レイテンシー）要件に応じて、データ処理方法を選択する必要があります。<br>
# MAGIC 
# MAGIC | #    | レイテンシーに応じた処理方法                     | Databricksの実装例                                           |
# MAGIC | ---- | ---------------------------- | ------------------------------------------------------------ |
# MAGIC | 1    | バッチ                       | 1-1. スケジュールトリガーによるSparkデータフレーム処理<br/>1-2. Delta live tableのトリガーパイプラインによる処理 |
# MAGIC | 2    | ストリーミング               | 3-1. Sparkストリーミング処理<br/>3-2. Databricksオートローダーによる処理<br/>3-3. Delta live tableの連続パイプラインによる処理 |

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Q2. 作成したテーブルを確認する
# MAGIC 
# MAGIC Databricksではテーブルを作成するとカタログ（defaultではhive metastore）配下のデータベースへメタデータが登録されます<br>
# MAGIC 登録されたテーブルをData Explorer、またファイルレベルで確認してみましょう
# MAGIC 
# MAGIC ToDo: 
# MAGIC 1. 左のタブから **Data** を選択
# MAGIC 1. Catalogsから `hive_metastore`を選択
# MAGIC 1. DatabaseからCmd3で表示されているものを選択
# MAGIC 1. Tablesから **lineitem** を選択する
# MAGIC 1. 以上をSQL・Data Science & Engineering両方のモードで行い、SQLでしかできないことをまとめる

# COMMAND ----------

# MAGIC %md
# MAGIC - xxx
# MAGIC - xxx

# COMMAND ----------

# Deltaファイルを確認します
# トランザクションログの_delta_logとデータファイルのparquetのセットになっていることを確認する
display(dbutils.fs.ls(f"{data_path}/database/{database}/lineitem"))

# COMMAND ----------

# _delta_logの中身を確認する
dbutils.fs.head(f"{data_path}/database/{database}/lineitme/_delta_log/00000000000000000000.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. Databricksオートローダーによりデータの書き込みを実施してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2_2"

# COMMAND ----------

tgt_table_name__2_2 = 'auto_loader_table'

schema__2_2 = """
  CURRENT_DATETIME TIMESTAMP
"""

table_ddl__2_2 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_2}
    (
        {schema__2_2}
    )
    USING delta
"""

# Auto Loaderの手順で利用する変数を定義
src_file_dir__2_2   = f'{data_path}/auto_loader/{user_name}/src'
src_file_path__2_2   = f'{src_file_dir__2_2}/*'
checkpoint_path__2_2 = f'{data_path}/auto_loader/{user_name}/_checkpoints'

# COMMAND ----------

# 初期ファイルを配置
dbutils.fs.rm(src_file_dir__2_2, True)
dbutils.fs.rm(checkpoint_path__2_2, True)
put_files(src_file_dir__2_2)
display(dbutils.fs.ls(src_file_dir__2_2))

# COMMAND ----------

# TODO 変数`table_ddl__2_2`を用いて、テーブルを作成してください。
spark.<< FILL IN >

# COMMAND ----------

# テーブル定義を確認
spark.sql(f'DESC EXTENDED {tgt_table_name__2_2}').display()

# COMMAND ----------

display(dbutils.fs.ls(src_file_dir__2_2))

# COMMAND ----------

# ToDo Databricks Auto loaderにて、変数`schema__2_2`をスキーマに指定して、変数`src_file_path__2_2`をCSVファイルのソースとして読み込みを実施してください。
df__2_2 = (spark
             .<< FILL IN >
          )
 
# ToDo 変数`checkpoint_path`をチェックポイントとして指定して、ストリーム書き込み処理を実施してください。
(df__2_2
   .writeStream
   .<< FILL IN >>
)

# COMMAND ----------

display(spark.readStream.table(tgt_table_name__2_2))

# COMMAND ----------

# ファイルの再配置を行い、上記セルの表示結果が変わることを確認
put_files(src_file_dir__2_2)

# COMMAND ----------

# ストリーム処理を停止
for stream in spark.streams.active:
    stream.stop()
