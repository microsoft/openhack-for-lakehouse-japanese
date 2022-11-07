# Databricks notebook source
# MAGIC %md
# MAGIC ## 3. メダリオンアーキテクチャ構築の実践
# MAGIC 
# MAGIC Q1. Sparkテーブルにおけるテーブルプロパティ、および、事後処理の検討してください。<br>
# MAGIC Q2. Bronzeテーブルのパイプラインを作成してください。<br>
# MAGIC Q3. Silverテーブルのパイプラインを作成を作成してください。<br>
# MAGIC Q4. Goldテーブルのパイプラインを作成してください。<br>

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="3"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Sparkテーブルにおけるテーブルプロパティ、および、事後処理の検討してください。

# COMMAND ----------

# MAGIC %md
# MAGIC ToDO 下記のドキュメントを参考に、設定すべきテーブルプロパティ、および、データエンジニアリング実施後に行うべき処理を、それぞれ3つ以上記載してください。
# MAGIC 
# MAGIC - [ファイル管理を使用してパフォーマンスを最適化する - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/delta/optimizations/file-mgmt)
# MAGIC - [自動最適化 - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/delta/optimizations/auto-optimize)
# MAGIC - [ANALYZE TABLE - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-syntax-aux-analyze-table)
# MAGIC - [VACUUM - Azure Databricks | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/databricks/spark/latest/spark-sql/language-manual/delta-vacuum)
# MAGIC - [What's the best practice on running ANALYZE on Delta Tables for query performance optimization? (databricks.com)](https://community.databricks.com/s/question/0D53f00001GHVicCAH/whats-the-best-practice-on-running-analyze-on-delta-tables-for-query-performance-optimization)

# COMMAND ----------

# MAGIC %md
# MAGIC Answer：
# MAGIC   
# MAGIC 設定を検討すべきテーブルプロパティ
# MAGIC - delta.autoOptimize.optimizeWrite
# MAGIC - delta.autoOptimize.autoCompact
# MAGIC - delta.dataSkippingNumIndexedCols
# MAGIC 
# MAGIC データエンジニアリング実施後に行うべき処理
# MAGIC - Vacuum
# MAGIC - Analyze table
# MAGIC - Optimize

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q2. Bronzeテーブルのパイプラインを作成してください。

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="3_2"

# COMMAND ----------

tgt_table_name__3_2 = 'olist_orders_dataset_bronze'

# COMMAND ----------

# Bronzeテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_2}
(
    `order_id` STRING,
    `customer_id` STRING,
    `order_status` STRING,
    _datasource STRING,
    _ingest_timestamp timestamp
)
USING delta
""")

# COMMAND ----------

# 現在のテーブル定義を確認
spark.sql(f'''DESC EXTENDED {tgt_table_name__3_2}''').display()

# COMMAND ----------

# ToDo 統計情報を取得対象とすべき`_ingest_timestamp`列を、`order_id`列の後に配置するように変更してください。
spark.sql(f'''
ALTER TABLE {tgt_table_name__3_2} 
  CHANGE COLUMN
    _ingest_timestamp _ingest_timestamp timestamp AFTER order_id
''')

# ToDo 下記のテーブルプロパティを設定してください。
## `optimizeWrite`、および、`autoCompact`を`True`に設定
## 結合キーに用いる2列を統計情報の取得対象に設定
spark.sql(f'''
ALTER TABLE {tgt_table_name__3_2}  
  SET TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 2
  )
''')

# COMMAND ----------

# ToDo カラム順が変更されていることを確認してください。
spark.sql(f'DESC EXTENDED {tgt_table_name__3_2}').display()

# COMMAND ----------

# ToDo テーブルプロパティが適切に設定されていることを確認してください。
spark.sql(f'DESC DETAIL {tgt_table_name__3_2}').display()

# COMMAND ----------

# 取り込みデータのパスを変数にセット
src_file_path__3_2 = f"{src_file_path__3_2__first}/*"

# COMMAND ----------

from  pyspark.sql.functions import input_file_name,current_timestamp

df = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "False")
        .load(src_file_path__3_2)
     )

# `_datasource`列と`_ingest_timestamp`列を追加
df = (df
          .withColumn("_datasource", input_file_name())
          .withColumn("_ingest_timestamp", current_timestamp())
     )

# ToDO ターゲットのテーブルへ`append`によりデータの書き込みを実施してください。
(df.write
     .format('delta')
     .mode('append')
     .saveAsTable(tgt_table_name__3_2)
)

# ToDo Vacuumを実行してください。
spark.sql(f'''
VACUUM {tgt_table_name__3_2}
''')

# ToDo Analyze Tableを実行してください。`order_id`列、および、`_ingest_timestamp`列が対象
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_2}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      order_id,
      _ingest_timestamp
''')

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_2))

# COMMAND ----------

# 2回目のデータが届いたため、本セルにてパスを指定後、再度データの書き込みを実施してみてください。
src_file_path__3_2 = src_file_path__3_2__second

# COMMAND ----------

# MAGIC %md
# MAGIC 下記のようなエラーメッセージが表示されるはずです。
# MAGIC 
# MAGIC `AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: 9fb3326a-400d-407d-971e-9874f75e4caa).`
# MAGIC 
# MAGIC ソースシステム側のスキーマ変更が、ダウンストリーム側のシステムに伝達されていなかったようです。
# MAGIC ソースシステム側の変更のたびに障害対応が必要となるシステムでは運用コストが高くなるため、ブロンズテーブルへの書き込み時にスキーマ展開を行う仕様に変更することにします。

# COMMAND ----------

# TODO スキーマ展開を許可するように変更してください。
from  pyspark.sql.functions import input_file_name,current_timestamp

df = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "False")
        .load(src_file_path__3_2)
     )

# `_datasource`列と`_ingest_timestamp`列を追加
df = (df
          .withColumn("_datasource", input_file_name())
          .withColumn("_ingest_timestamp", current_timestamp())
     )

# ToDO ターゲットのテーブルへ`append`によりデータの書き込みを実施してください。
(df.write
     .format('delta')
     .mode('append')
     .option("mergeSchema", "true")
     .saveAsTable(tgt_table_name__3_2)
)

# Vacuumを実行
spark.sql(f'''
VACUUM {tgt_table_name__3_2}
''')

# Analyze Tableを実行。`order_id`列、および、`_ingest_timestamp`列が対象
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_2}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      order_id,
      _ingest_timestamp
''')

# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q3. Silverテーブルのパイプラインを作成を作成してください。

# COMMAND ----------

src_table_name__3_3 = 'olist_orders_dataset_bronze'
tgt_table_name__3_3 = 'olist_orders_dataset'

# COMMAND ----------

# Silverテーブルを作成
spark.sql(f"""
CREATE OR REPLACE TABLE {tgt_table_name__3_3}
(
   `order_id` STRING,
    `customer_id` STRING,
    `order_status` STRING,
    `order_purchase_timestamp` TIMESTAMP,
    `order_approved_at` TIMESTAMP,
    `order_delivered_carrier_date` TIMESTAMP,
    `order_delivered_customer_date` TIMESTAMP,
    `order_estimated_delivery_date` TIMESTAMP,
    _ingest_timestamp timestamp
)
USING delta
TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 1
  )
""")

# COMMAND ----------

from  pyspark.sql.functions import current_timestamp,lit

# ToDo 下記の処理を実行したデータフレーム（df）を作成してください。
## 1. ブロンズテーブルから主キー（`order_id`）ごとに`_ingest_timestamp`列の最大日を抽出したサブセットを作成
## 2. 主キー＋`_ingest_timestamp`列の条件で、1のサブセットとブロンズテーブルを結合
## 3. ブロンズテーブルのデータ型をシルバーテーブルと同一のデータ型に変換
brz_to_slv_sql = f'''
with slv_records (
  SELECT
    order_id,
    MAX(_ingest_timestamp) AS max_ingest_timestamp
    
    FROM
      {src_table_name__3_3}
    
    GROUP BY
      order_id      
)

SELECT
  brz.`order_id`,
  brz.`customer_id`,
  brz.`order_status`,
  brz.`order_purchase_timestamp`::TIMESTAMP,
  brz.`order_approved_at`::TIMESTAMP,
  brz.`order_delivered_carrier_date`::TIMESTAMP,
  brz.`order_delivered_customer_date`::TIMESTAMP,
  brz.`order_estimated_delivery_date`::TIMESTAMP,
  _ingest_timestamp
  
  FROM
    {src_table_name__3_3} AS brz
  INNER JOIN 
    slv_records AS slv
    ON 
      brz.order_id =  slv.order_id
      AND brz._ingest_timestamp =  slv.max_ingest_timestamp
'''
df = spark.sql(brz_to_slv_sql)

# ToDo dropDuplicates関数にて、主キーの一意性を保証してください。連携日ごとの一意性が保証されないことがあるため。
df = df.drop_duplicates(['order_id'])



# ToDo 一時ビューからシルバーテーブルに対して、MERGE文によりアップサート処理を実施してください。
# 一時ビューの`_ingest_timestamp`列がシルバーテーブルの`_ingest_timestamp`列以降である場合のみ、UPDATE処理が実行されるようにしてください。
## 一時ビューを作成
temp_view_name = f'_tmp_{tgt_table_name__3_3}'
df.createOrReplaceTempView(temp_view_name)


## Merge処理を実行
spark.sql(f'''
MERGE INTO {tgt_table_name__3_3} AS tgt
  USING {temp_view_name} AS src
  
  ON tgt.order_id = src.order_id
  
  WHEN MATCHED
  AND tgt._ingest_timestamp < src._ingest_timestamp
    THEN UPDATE SET *
  WHEN NOT MATCHED
    THEN INSERT *
''')

# ToDo Vacuumを実行
spark.sql(f'''
VACUUM {tgt_table_name__3_3}
''')

# ToDo Analyze Tableを実行してください。`order_id`列が対象。
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_3}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      order_id
''')


# COMMAND ----------

# データが書き込まれたことを確認
display(spark.table(tgt_table_name__3_3))

# COMMAND ----------

# シルバーテーブルが99441レコードであることを確認。シルバーテーブルは、主キー列で一意のレコードを保持しているため、ブロンズテーブルとレコード数が一致しない。
print(f'Silver table Count : {spark.table(tgt_table_name__3_3).count()}')
print(f'Bronze table Count : {spark.table(src_table_name__3_3).count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q4. Goldテーブルのパイプラインを作成してください。

# COMMAND ----------

src_table_name__3_4 = 'olist_orders_dataset'
tgt_table_name__3_4 = 'monthly_sales_counts'

# COMMAND ----------

# `olist_orders_dataset`テーブルから下記の処理を行ったデータフレーム（df）を作成してください。
## `order_delivered_carrier_date`列がNULLでないレコードを抽出してください。
## `order_delivered_carrier_date`列を`yyyyMM`（例:201801）形式に変換して、`sales_yearmonth`として定義してください。
## `order_yearmonth`列で集計を行い、`order_id`列の重複排除したカウント数を`sales_counts`列として定義してください。
slv_to_gld_sql = f"""
with src (

SELECT
  od.order_id,
  date_format(od.order_delivered_carrier_date, "yMM") AS sales_yearmonth
  
  FROM
    {src_table_name__3_4} od
    
  WHERE order_delivered_carrier_date IS NOT NULL
    
), agg (

SELECT
    sales_yearmonth,
    COUNT(DISTINCT order_id) AS sales_counts --受注件数
  FROM
    src
  GROUP BY
    sales_yearmonth
)

SELECT
  *
  FROM
    agg
"""
df = spark.sql(slv_to_gld_sql)


# ToDo CTAS（CREAT TABLE AS SLECT）により、`monthly_sales_counts`テーブルを作成してください。
## 一時ビューを作成
tmp_view_name = f'_tmp_{tgt_table_name__3_4}'
df.createOrReplaceTempView(tmp_view_name)

## CTASを実行
ctas_sql = f'''
create or replace table {tgt_table_name__3_4}
  using delta
  TBLPROPERTIES (
    delta.autoOptimize.optimizeWrite = True, 
    delta.autoOptimize.autoCompact   = True,
    delta.dataSkippingNumIndexedCols = 1
  )
  AS 
  select 
    * 
    from 
      {tmp_view_name}
'''
spark.sql(ctas_sql)

# Vacuumを実行
spark.sql(f'''
VACUUM {tgt_table_name__3_4}
''')

# Analyze Tableを実行。`sales_yearmonth`列が対象。
spark.sql(f'''
ANALYZE TABLE {tgt_table_name__3_4}
  COMPUTE STATISTICS 
    FOR COLUMNS 
      sales_yearmonth
''')

# COMMAND ----------

# データを確認。全体で24レコードとなる想定。
display(spark.table(tgt_table_name__3_4))

# COMMAND ----------

# ToDo テーブルプロパティが適切に設定されていることを確認してください。
spark.sql(f'DESC DETAIL {tgt_table_name__3_4}').display()
