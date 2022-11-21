# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 02. データエンジニアリングパイプラインの実践 (目安 11:00~12:30)
# MAGIC ### 本ノートブックの目的：Databricksにおけるテーブル作成、データ格納処理について理解を深める
# MAGIC Q1. Sparkデータフレーム操作によりデータの書き込みを実施してください。<br>
# MAGIC Q2. Sparkデータフレーム操作により書き込んだデータをData Explorerとファイルレベルで確認してください。<br>
# MAGIC Q3. COPY INTO によりデータの書き込みを実施してください。<br>
# MAGIC Q4. Databricks Auto Loader によりデータの書き込みを実施してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. データフレーム操作によりデータの書き込みを実施してください。
# MAGIC 
# MAGIC 変数`src_file_path__2_1`にある区切りテキストファイルを読み取り、変数`tgt_table_name__2_1`のテーブルに上書き処理を実施してください。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 実践例

# COMMAND ----------

src_file_path__2_1_1 = "dbfs:/databricks-datasets/tpch/data-001/customer"
tgt_table_name__2_1_1 = "customer"

schema__2_1_1 = """
  c_custkey long,
  c_name string,
  c_address string,
  c_nationkey long,
  c_phone string,
  c_acctbal decimal(12, 2),
  c_mktsegment string,
  c_comment string
"""

table_ddl__2_1_1 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_1_1}
    (
        {schema__2_1_1},
        _src_file_path STRING
    )
    USING delta
"""

# COMMAND ----------

# ディレクトリを確認
display(dbutils.fs.ls(src_file_path__2_1_1))

# COMMAND ----------

# ソースファイルを確認
print(dbutils.fs.head(f"{src_file_path__2_1_1}/customer.tbl", 500))

# COMMAND ----------

# テーブルを作成
spark.sql(table_ddl__2_1_1)

# COMMAND ----------

# テーブル定義を確認
spark.sql(f"DESC EXTENDED {tgt_table_name__2_1_1}").display()

# COMMAND ----------

# `src_file_path__2_1_1`変数をソースとして、区切り文字を`|`としてデータフレームを作成
df = (
    spark.read.format("csv")
    .schema(schema__2_1_1)
    .option("sep", "|")
    .load(src_file_path__2_1_1)
)

# `_metadata.file_path`のカラム値を、ソースファイルのパスを保持したカラム（`_src_file_path`列）としてデータフレームに追加
df = (
    df.select("*", "_metadata")
    .withColumn("_src_file_path", df["_metadata.file_path"])
    .drop("_metadata")
)

# `tgt_table_name__2_1_1`変数を保存先のテーブルとして、`Delta`形式で上書き（`overwrite`）によりデータの書き込み実施
(df.write.format("delta").mode("overwrite").saveAsTable(tgt_table_name__2_1_1))

# COMMAND ----------

# データをdisplay関数で表示
spark.table(tgt_table_name__2_1_1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ToDo `lineitem.tbl` をソースとして、`lineitem` テーブルにデータの書き込みを実施してください。

# COMMAND ----------

src_file_path__2_1_2 = "dbfs:/databricks-datasets/tpch/data-001/lineitem/"
tgt_table_name__2_1_2 = "lineitem"

schema__2_1_2 = """
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

table_ddl__2_1_2 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_1_2}
    (
        {schema__2_1_2},
        _src_file_path STRING
    )
    USING delta
"""

# COMMAND ----------

# ToDo `src_file_path__2_1_2`変数をもとにディレクトリを確認
<<FILL-IN>>

# COMMAND ----------

# ToDo ソースファイルを確認
<<FILL-IN>>

# COMMAND ----------

# ToDo `table_ddl__2_1_2`変数をもとにテーブルを作成
<<FILL-IN>>

# COMMAND ----------

# テーブル定義を確認
spark.sql(f"DESC EXTENDED {tgt_table_name__2_1_2}").display()

# COMMAND ----------

# TODO 変数`src_file_path__2_1_2`をソースとして、区切り文字を`|`としてデータフレームを作成してください。
<<FILL-IN>>

# TODO `_metadata.file_path`のカラム値を、ソースファイルのパスを保持したカラム（`_src_file_path`列）としてデータフレームに追加してください。
<<FILL-IN>>

# TODO `tgt_table_name__2_1_2`変数を保存先のテーブルとして、`Delta`形式で上書き（`overwrite`）によりデータの書き込み実施してください。
<<FILL-IN>>

# COMMAND ----------

# ToDo 保存先のテーブル（`tgt_table_name__2_1_2`変数）のデータをdisplay関数で表示してください。
<<FILL-IN>>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Q2. Sparkデータフレーム操作により書き込んだデータをData Explorerとファイルレベルで確認してください。
# MAGIC 
# MAGIC Databricksではテーブルを作成するとカタログ（defaultではhive metastore）配下のデータベースへメタデータが登録されます<br>
# MAGIC 登録されたテーブルをData Explorer、またファイルレベルで確認してみましょう
# MAGIC 
# MAGIC ToDo: 
# MAGIC 1. 左のタブから **Data** を選択
# MAGIC 1. Catalogsから `hive_metastore`を選択
# MAGIC 1. DatabaseはCmd3で表示されているものを選択
# MAGIC 1. Tablesから **lineitem** を選択する
# MAGIC 1. 以上をSQL・Data Science & Engineering両方のモードで行い、SQLでしかできないことをまとめる

# COMMAND ----------

# MAGIC %md
# MAGIC Answer
# MAGIC - Catalog, Database, TableのPermissionの変更
# MAGIC - Quick Query, Dashboardによって迅速に分析を開始できる
# MAGIC - Deltaテーブルか否かアイコンで確認できる
# MAGIC など

# COMMAND ----------

# `tgt_table_name__2_1_2` 変数のテーブルの保存先を取得
tbl_path = (
    spark.sql(f"DESC EXTENDED {tgt_table_name__2_1_2}")
    .where('col_name = "Location"')
    .select("data_type")
    .first()[0]
)

print(tbl_path)

# COMMAND ----------

# Deltaファイルを確認します
# トランザクションログの_delta_logとデータファイルのparquetのセットになっていることを確認する
display(dbutils.fs.ls(tbl_path))

# COMMAND ----------

# _delta_logの中身を確認する
print(dbutils.fs.head(f"{tbl_path}/_delta_log/00000000000000000000.json"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. COPY INTO によりデータの書き込みを実施してください。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 実践例

# COMMAND ----------

src_file_path__2_3_1 = "dbfs:/databricks-datasets/tpch/data-001/nation"
tgt_table_name__2_3_1 = "nation"

schema__2_3_1 = """
  N_NATIONKEY  integer,
  N_NAME       string,
  N_REGIONKEY  integer, 
  N_COMMENT    string
"""

drop_table_ddl__2_3_1 = f"""
DROP TABLE IF EXISTS {tgt_table_name__2_3_1}
"""
create_table_ddl__2_3_1 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_3_1}
    (
        {schema__2_3_1},
        _src_file_path STRING
    )
    USING delta
"""

# COMMAND ----------

# テーブルを作成
spark.sql(drop_table_ddl__2_3_1)
spark.sql(create_table_ddl__2_3_1)

# COMMAND ----------

# COPY INTO によりデータを書き込む
retruned_df = spark.sql(
    f"""
COPY INTO {tgt_table_name__2_3_1}
  FROM  (
    SELECT
      _c0::integer AS N_NATIONKEY
      ,_c1 AS N_NAME
      ,_c2::integer AS N_REGIONKEY 
      ,_c3 AS N_COMMENT
      ,_metadata.file_path AS _src_file_path

      FROM
        '{src_file_path__2_3_1}'
  )
  FILEFORMAT = CSV
  FORMAT_OPTIONS (
    'mergeSchema' = 'true'
    ,'ignoreCorruptFiles' = 'false'
    ,'sep' = '|'
    ,'inferSchema' = 'false'
  )
  COPY_OPTIONS (
    'mergeSchema' = 'true'
    -- ファイル再取り込み時には、`force` を `true` に設定
    -- ,'force' = 'true'
  )
"""
)

retruned_df.display()

# COMMAND ----------

# データをdisplay関数で表示
spark.table(tgt_table_name__2_3_1).display()

# COMMAND ----------

# 再取り込みを実施しても 25 件となることを確認
print(spark.table(tgt_table_name__2_3_1).count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### ToDo `orders.tbl` をソースとして、`orders` テーブルにデータの書き込みを実施してください。

# COMMAND ----------

src_file_path__2_3_2 = "dbfs:/databricks-datasets/tpch/data-001/orders"
tgt_table_name__2_3_2 = "orders"

schema__2_3_2 = """
    o_orderkey long,
    o_custkey long,
    o_orderstatus string,
    o_totalprice decimal(12, 2),
    o_orderdate date,
    o_orderpriority string,
    o_clerk string,
    o_shippriority int,
    o_comment string
"""

drop_table_ddl__2_3_2 = f"""
DROP TABLE IF EXISTS {tgt_table_name__2_3_2}
"""
create_table_ddl__2_3_2 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_3_2}
    (
        {schema__2_3_2},
        _src_file_path STRING
    )
    USING delta
"""

# COMMAND ----------

# テーブルを作成
spark.sql(drop_table_ddl__2_3_2)
spark.sql(create_table_ddl__2_3_2)

# COMMAND ----------

# ToDo COPY INTO により、`src_file_path__2_3_2`変数をソースとして`tgt_table_name__2_3_2`変数のテーブルへデータを書き込みを実施してください。
retruned_df = <<FILL-IN>>

retruned_df.display()

# COMMAND ----------

# データをdisplay関数で表示
spark.table(tgt_table_name__2_3_2).display()

# COMMAND ----------

# 再取り込みを実施しても 7,500,000 件となることを確認
print(spark.table(tgt_table_name__2_3_2).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4. Databricks Auto Loader によりデータの書き込みを実施してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="2_4"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 実践例

# COMMAND ----------

src_file_path__2_4_1 = f"{data_path}/auto_loader/{user_name}/src"
tgt_table_name__2_4_1 = "auto_loader_table"

schema__2_4_1 = """
  CURRENT_DATETIME TIMESTAMP
"""

table_ddl__2_4_1 = f"""
CREATE OR REPLACE TABLE {tgt_table_name__2_4_1}
(
    {schema__2_4_1}
    ,_src_file_path STRING
)
USING delta
"""

# Auto Loaderの手順で利用する変数を定義
checkpoint_path__2_4_1 = (
    f"{data_path}/auto_loader/{user_name}/_checkpoints/auto_loader_table"
)

# COMMAND ----------

# 初期ファイルを配置
dbutils.fs.rm(src_file_path__2_4_1, True)
dbutils.fs.rm(checkpoint_path__2_4_1, True)
put_files(src_file_path__2_4_1)
display(dbutils.fs.ls(src_file_path__2_4_1))

# COMMAND ----------

# 変数`table_ddl__2_4_1`を用いて、テーブルを作成してください。
spark.sql(table_ddl__2_4_1)

# COMMAND ----------

# テーブル定義を確認
spark.sql(f"DESC EXTENDED {tgt_table_name__2_4_1}").display()

# COMMAND ----------

# Databricks Auto loaderにて、`schema__2_4_1`変数をスキーマとして指定して、`src_file_path__2_4_1`変数をCSVファイルのソースとして読み込みを実施
df__2_4_1 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaHints", schema__2_4_1)
    .schema(schema__2_4_1)
    .load(src_file_path__2_4_1)
)

# `_metadata.file_path`のカラム値を、ソースファイルのパスを保持したカラム（`_src_file_path`列）としてデータフレームに追加
df__2_4_1 = df__2_4_1.withColumn("_src_file_path", df__2_4_1["_metadata.file_path"])


# `checkpoint_path`変数をチェックポイントとして指定して、ストリーム書き込み処理を実施
(
    df__2_4_1.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path__2_4_1)
    .table(tgt_table_name__2_4_1)
)

# COMMAND ----------

# データをdisplay関数で表示
display(spark.readStream.table(tgt_table_name__2_4_1))

# COMMAND ----------

# ファイルの再配置を行い、上記セルの表示結果が変わることを確認
put_files(src_file_path__2_4_1)

# COMMAND ----------

# ストリーム処理を停止
for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ToDo `region.tbl` をソースとして、`region` テーブルにデータの書き込みを実施してください。

# COMMAND ----------

src_file_path__2_4_2 = "dbfs:/databricks-datasets/tpch/data-001/region"
tgt_table_name__2_4_2 = "region"

schema__2_4_2 = """
    r_regionkey long,
    r_name string,
    r_comment string
"""

drop_table_ddl__2_4_2 = f"""
DROP TABLE IF EXISTS {tgt_table_name__2_4_2}
"""
create_table_ddl__2_4_2 = f"""
    CREATE OR REPLACE TABLE {tgt_table_name__2_4_2}
    (
        {schema__2_4_2},
        _src_file_path STRING
    )
    USING delta
"""

# Auto Loaderの手順で利用する変数を定義
checkpoint_path__2_4_2 = f"{data_path}/auto_loader/{user_name}/_checkpoints/region"

# COMMAND ----------

# テーブルを作成
spark.sql(drop_table_ddl__2_4_2)
spark.sql(create_table_ddl__2_4_2)

# checkpoint を初期化
dbutils.fs.rm(checkpoint_path__2_4_2, True)

# COMMAND ----------

# ToDo Databricks Auto Loader によりデータの書き込みを実施してください。
# Databricks Auto loaderにて、変数`schema__2_4_2`をスキーマに指定して、`src_file_path__2_4_1`変数をCSVファイルのソースとして読み込みを実施。
df__2_4_2 = <<FILL-IN>>

# `_metadata.file_path`のカラム値を、ソースファイルのパスを保持したカラム（`_src_file_path`列）としてデータフレームに追加
df__2_4_2 = df__2_4_2.withColumn("_src_file_path", df__2_4_2["_metadata.file_path"])

# `checkpoint_path`変数をチェックポイントとして指定して、ストリーム書き込み処理を実施。
(
    df__2_4_2.writeStream.trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path__2_4_2)
    .toTable(tgt_table_name__2_4_2)
)

# COMMAND ----------

# データを display 関数で表示
spark.table(tgt_table_name__2_4_2).display()

# COMMAND ----------

# ストリーム処理を停止
for stream in spark.streams.active:
    stream.stop()
