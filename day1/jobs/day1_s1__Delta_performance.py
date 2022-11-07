# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC # 高信頼性と高パフォーマンスを実現する Delta Lake / Photon を体験
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>作成者</td><th>Databricks Japan</th></tr>
# MAGIC   <tr><td>ランタイム</td><th>9.1 LTS Photon (includes Apache Spark 3.1.2, Scala 2.12)※必須</th></tr>
# MAGIC   <tr><td>インスタンス</td><td>i3.2xlarge 61GMem,8core シングルノード </td></tr>
# MAGIC   <tr><td>期日</td><td>2021/10/19</td></tr>
# MAGIC   <tr><td>バージョン</td><td>1.0</td></tr>
# MAGIC </table>
# MAGIC <img style="margin-top:25px;" src="https://jixjiadatabricks.blob.core.windows.net/images/databricks-logo-small-new.png" width="140">
# MAGIC <hr>
# MAGIC <h3>データレイクに<span style="color='#38a'">信頼性</span>と<span style="color='#38a'">パフォーマンス</span>をもたらす</h3>
# MAGIC <p>本編はデモデータを使用してDelta Lakeが提供する主要な機能に関して説明していきます。</p>
# MAGIC <div style="float:left; padding-right:60px; margin-top:20px; margin-bottom:200px;">
# MAGIC   <img src="https://jixjiadatabricks.blob.core.windows.net/images/delta-lake-square-black.jpg" width="220">
# MAGIC </div>
# MAGIC 
# MAGIC <div style="float:left; margin-top:0px; padding:0;">
# MAGIC   <h3>信頼性</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>DMLサポート（INSERTだけではなくUPDATE/DELETE/MERGEをサポート）</li>
# MAGIC     <li>データ品質管理　(スキーマ・エンフォース/エボリューション)</li>
# MAGIC     <li>トランザクションログによるACIDコンプライアンスとタイムトラベル (データのバージョン管理)</li>
# MAGIC    </ul>
# MAGIC 
# MAGIC   <h3>パフォーマンス</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>Photonエンジン</li>
# MAGIC     <li>Optimizeによるコンパクションとデータスキッピング</li>
# MAGIC     <li>Deltaキャッシング</li>
# MAGIC   </ul>
# MAGIC 
# MAGIC  <h3>バッチデータとストリーミングデータの統合</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>ストリーミングデータの取り込み</li>
# MAGIC     <li>ストリーミングデータのリアルタイムETL処理</li>
# MAGIC     <li>バッチデータとストリーミングデータに対する分析処理</li>
# MAGIC    </ul>
# MAGIC   </div>
# MAGIC 
# MAGIC <div style="display:block; clear:both; padding-top:20px;">
# MAGIC   <div style="background: #ff9; margin-top:10px;">
# MAGIC   <img src="https://jixjiadatabricks.blob.core.windows.net/images/exclamation-yellow.png" width="25px"><span style="padding-left:10px; padding-right:15px;">注) セル14、15と42は意図的にエラーを起こすよう作成しています</span>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parquet + トランザクションログ
# MAGIC <p  style="position:relative;width: 500px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/delta1.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの準備
# MAGIC 
# MAGIC 今回使用するデータはDatabricksのデモデータとしてストレージに格納されているTPCHのデータセットを使用します。

# COMMAND ----------

import re
from pyspark.sql.types import * 
import pandas as pd
from pyspark.sql.types import StringType

# ウィジットがあれば削除しておく
dbutils.widgets.removeAll()

# COMMAND ----------

# Username を取得。
username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Username の英数字以外を除去し、全て小文字化。Username をファイルパスやデータベース名の一部で使用可能にするため。
username = re.sub('[^A-Za-z0-9]+', '', username_raw).lower()

# データベース名を生成。
db_name = f"{username}" + '_db'

# パス指定 Sparkではdbfs:を使用してアクセス
table_path = '/user/hive/warehouse/' + username + '_db.db/' 

# 既存のデータを削除
# Parquest/Deltaテーブルのパスを削除。
#dbutils.fs.rm(table_path, True)

# データベースの準備
#spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")

print("database  : " + db_name)
print("table_path:" + table_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 付属のデモデータに対して外部表を作成
# MAGIC drop table if exists lineitem_ext;
# MAGIC create external table lineitem_ext
# MAGIC USING DELTA
# MAGIC   LOCATION 'dbfs:/databricks-datasets/tpch/delta-001/lineitem'

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # データ増幅用のクエリ
# MAGIC sqltext1 = "create table lineitem USING PARQUET LOCATION '" + table_path + "lineitem' AS select l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,substr(l_shipdate,6,2) as l_shipmonth from lineitem_ext order by l_orderkey"
# MAGIC 
# MAGIC sqltext2 = "insert into lineitem select l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment,substr(l_shipdate,6,2) as l_shipmonth from lineitem_ext order by l_orderkey"
# MAGIC 
# MAGIC ################
# MAGIC # データ増幅の開始
# MAGIC ################
# MAGIC #　　3分ほどかかるのでこの間に簡単にDeltaを説明します。
# MAGIC #　　テーブル作成
# MAGIC sql("drop table if exists lineitem")
# MAGIC print(1,sqltext1)
# MAGIC sql(sqltext1)
# MAGIC # データ増幅
# MAGIC print(2,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(3,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(4,sqltext2)
# MAGIC sql(sqltext2)

# COMMAND ----------

# 最初は従来のデータレイクとの比較のためDatabricksの機能はOFFにしておく

#PhotonをOFF
spark.conf.set("spark.databricks.photon.enabled","false")

#デルタキャッシングをOFF
spark.conf.set("spark.databricks.io.cache.enabled","false")

# COMMAND ----------

# MAGIC %md # 1.今までのデータレイクの世界（通常Sparkクラスタ+Parquetフォーマットを使用)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ワークショップで使用するテーブル定義
# MAGIC -- 最初は一般的なParquetフォーマットで作成
# MAGIC -- ProviderがParquetになっていることを確認
# MAGIC DESCRIBE extended lineitem;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ワークショップで使用するテーブル(TPCHベンチマークのLineitem)のデータ
# MAGIC select * from lineitem ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データ件数は約1.2億件(テキスト換算で約12GB)
# MAGIC select count(*) from lineitem;

# COMMAND ----------

# MAGIC %md ## データ更新処理の課題
# MAGIC <div style="float:left; padding-right:10px; margin-top:10px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-traditional.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   <div style="float:left;">
# MAGIC     Delta Lake以前の時代からデータレイクへ大量のデータを溜め込み、<br>Parquetで効率の良いデータ圧縮と分散を行ってきましたがいくつかの課題もあります
# MAGIC   分散ファイルの性質上、既存データに対するDELETE/UPDATE/MERGEなどの更新処理は<span style="color:#f00"><strong>出来ません。
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parquet形式のファイルフォーマットでは更新ができないため、UPDATE文はエラーとなります。
# MAGIC update lineitem set l_comment = 'Databricks is the best bigdata platform' where  l_orderkey = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parquet形式のファイルフォーマットでは更新ができないため、DELETE文はエラーとなります。
# MAGIC delete from lineitem where  l_orderkey = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC </strong></span>データは変更不可(immutable)で追加のみに対応(append-only)であるため、データ更新は基本的に洗い替え処理(CREATE TABLE AS SELECT)での新規データ作成となります。

# COMMAND ----------

# MAGIC %md ## データ品質管理の課題
# MAGIC <div style="float:left; padding-right:10px; margin-top:10px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-traditional.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   データレイクは基本Schema On Read思想（データに変更や変換を何も加えずにロードしREAD時に変換）が主流。なので、<br>スキーマの異なるデータを既存データが配置されているディレクトリへ追加するとデータそのものが汚れてしまう。
# MAGIC </div>

# COMMAND ----------

# MAGIC %python
# MAGIC # 全く関係のないスキーマの異なるデータを準備（3カラムから構成されるオーダー番号,国名データ、カラム名称も異なる。）
# MAGIC contry = sql('''      Select cast(100 as bigint) as l_orderkey, 'Janan'   as Country, 'Tokyo'      as Captal
# MAGIC                 union Select cast(100 as bigint) as l_orderkey, 'USA'     as Animal,  'Washington' as Size
# MAGIC                 union Select cast(100 as bigint) as l_orderkey, 'France'   as Animal, 'Paris'      as Size ''')
# MAGIC 
# MAGIC display(contry)

# COMMAND ----------

# MAGIC %python
# MAGIC # 国名データを先ほどのLineitemテーブルが格納されたディレクトリへ書き込む（追加）
# MAGIC contry.write.format('parquet').option('mergeSchema','false').mode('append').save(table_path + 'lineitem')
# MAGIC 
# MAGIC # Parqutフォーマットではファイル追加後はメタデータのリフレッシュが必要
# MAGIC sql("REFRESH TABLE lineitem")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- こういったゴミデータがデータ品質低下の原因。通常のParqutフォーマットではデータ削除(delete)もできないため,通常はデータの洗い替え(CREATE TABLE AS SELECT...)による新規テーブルの作成が必要。
# MAGIC SELECT * FROM lineitem where l_orderkey = 100 order by l_partkey;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スキーマを確認しても、上記のように一部のデータはレコードとして存在していますが、追加した新規スキーマはマージされておらず一貫性のない状態であり、このようゴミデータはデータスワンプの元となります。
# MAGIC describe extended lineitem

# COMMAND ----------

# MAGIC %python
# MAGIC #ここでは最初に追加したゴミデータの入ったParquetファイルは不要なので削除しておくことにします。
# MAGIC 
# MAGIC #ファイルのリストを取得
# MAGIC files = dbutils.fs.ls(table_path + 'lineitem')
# MAGIC df = spark.createDataFrame([str(i) for i in files], StringType()).toPandas()
# MAGIC s = df['value']
# MAGIC 
# MAGIC #Parqurtファイルで最もサイズの小さなファイル名を取得
# MAGIC df1 = s.str.extract('dbfs:(.*?)\', name=', expand=True).rename(columns={0: 'filename'})
# MAGIC df2 = s.str.extract('size=(.*?)\)', expand=True).rename(columns={0: 'size'})
# MAGIC df3 = spark.createDataFrame(pd.concat([df1, df2], axis=1, join='inner')).createOrReplaceTempView("files")
# MAGIC file = sql("select filename ,cast(size as int) as size from files where filename like '%.snappy.parquet' and size < 2000 order by cast(size as int) limit 1")
# MAGIC display(file)
# MAGIC filename = ' '.join(file.toPandas()["filename"].astype(str).tolist())
# MAGIC print(filename)
# MAGIC print("追加したParquetファイルを削除(削除対象のファイルがない場合はエラーとなりますが無視してください)")
# MAGIC dbutils.fs.rm(filename)

# COMMAND ----------

# MAGIC %md ## パフォーマンスの課題
# MAGIC <div style="float:left; padding-right:10px; margin-top:10px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-traditional.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   <div style="float:left;">
# MAGIC   分散ファイルの性質上、多数の小規模ファイルやサイズが均等でないParquetファイルの集合に対しては<span style="color:#f00"><strong>期待するパフォーマンスがでない</strong></span>場合があります。<br>また大量のデータセットではパーティショニングなどによる<span style="color:#f00"><strong>不要なI/Oを削減するためのチューニングが必要</strong></span>な場合があります。
# MAGIC </div>

# COMMAND ----------

# MAGIC %python
# MAGIC # 物理ファイルの確認
# MAGIC # 200個の物理ファイルから構成されています。
# MAGIC sql("REFRESH TABLE lineitem")
# MAGIC display(dbutils.fs.ls(table_path + 'lineitem'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 全件検索
# MAGIC -- 40-50秒程度かかる。
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- フィルター条件付き
# MAGIC -- 9-10秒程度かかる
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC where    l_shipmode = 'AIR' AND l_shipmonth = '10'
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %md 
# MAGIC これより、Delta LakeにすることでこれまでのParquetフォーマットの課題がどのように解決できるか見ていきます。

# COMMAND ----------

# MAGIC %md # 2.レイクハウスへの進化

# COMMAND ----------

# MAGIC %md ## ParquetからDeltaへの変換
# MAGIC <div style="float:left; padding-right:10px; margin-top:10px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-no-label.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   既存ParquetやCSVなどから<span style="color:green"><strong>コマンド一行</strong></span>でDeltaへ変換する事が出来ます
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Convert To機能で既存テーブルを変換（裏にある物理ファイルも含めて)
# MAGIC -- 2-3分秒ほど
# MAGIC Convert To Delta lineitem

# COMMAND ----------

# MAGIC %md
# MAGIC <p  style="position:relative;width: 500px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/convert.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 新規に作成することも可能
# MAGIC <p  style="position:relative;width: 500px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/conver2.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Provierがdeltaになっています
# MAGIC DESCRIBE extended lineitem;

# COMMAND ----------

# MAGIC %md ## データ更新処理のサポート
# MAGIC <div style="float:left; padding-right:10px; margin-top:20px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-no-label.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   Deltaフォーマットのテーブルには更新処理が可能です。
# MAGIC   ここでは従来のデータレイクでは出来なかったデータの<span style="color:green"><strong>更新</strong></span><span style="color:green"></span>や<span style="color:green"><strong>削除</strong></span>などのETL操作(分散更新)を行ってみたいと思います
# MAGIC </div>

# COMMAND ----------

# MAGIC %md ### DWHと同様にデータの追加/更新/削除/マージ処理が可能

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta形式では更新が可能です。
# MAGIC update lineitem set l_comment = 'Databricks is the best bigdata platform' where  l_orderkey = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta形式では削除が可能です。
# MAGIC delete from lineitem where  l_orderkey = 1;

# COMMAND ----------

# MAGIC %md 
# MAGIC DeltaフォーマットにすることでDWHと同様に任意のレコードに更新処理が可能となりますので、データ更新でのデータの洗い替え処理は不要です。

# COMMAND ----------

# MAGIC %md ## DWHと同等のデータ品質管理が可能
# MAGIC <div style="float:left; padding-right:10px; margin-top:20px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-no-label.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   また、スキーマエンフォース機能で既存のスキーマと一致しないDML操作を<span style="color:green"><strong>拒否(デフォルト）</strong></span>もしくはスキーマエボリューションでスキーマの違いを<span style="color:green"><strong>完璧にマージ</strong></span>する事が可能です。</br>

# COMMAND ----------

# MAGIC %python
# MAGIC # 先ほど全く関係のないスキーマの異なるデータを準備（3カラムから構成されるオーダー番号,国名データ、カラム名称も異なる。）
# MAGIC contry = sql('''      Select cast(100 as bigint) as l_orderkey, 'Janan'   as Country, 'Tokyo'      as Captal
# MAGIC                 union Select cast(100 as bigint) as l_orderkey, 'USA'     as Animal,  'Washington' as Size
# MAGIC                 union Select cast(100 as bigint) as l_orderkey, 'France'   as Animal, 'Paris'      as Size ''')
# MAGIC 
# MAGIC display(contry)

# COMMAND ----------

# MAGIC %md 
# MAGIC 再度、異なるスキーマを追加してみますが、デフォルトでスキーマの違いを検知してエラーを発生させ、不正なデータ追加を防止します（スキーマ・エンフォースメント)

# COMMAND ----------

# MAGIC %python
# MAGIC # 国名データを先ほどのLineitemテーブルが格納されたディレクトリへ書き込む（追加）
# MAGIC # DELTAフォーマットではスキーマが違う場合にきちんとエラーを返してくれます（デフォルトの挙動）
# MAGIC contry.write.format('delta').option('mergeSchema','false').mode('append').save(table_path + 'lineitem')

# COMMAND ----------

# MAGIC %md  
# MAGIC 今度は`mergeSchema`オプションを使って異なるスキーマ同士のデータを正しく融合させる（スキーマエボリューション)ことも可能なため、レコードフォーマットが頻繁に変更されるような場合でもテーブル再作成が不要です。

# COMMAND ----------

# MAGIC %python
# MAGIC # 国名データを先ほどのLBSテーブルが格納されたディレクトリへ書き込む（追加）
# MAGIC contry.write.format('delta').option('mergeSchema','true').mode('append').save(table_path + 'lineitem')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 新規に追加されたファイルのデータをSQLで確認
# MAGIC -- 正しくカラムも生成されている
# MAGIC SELECT * FROM lineitem where l_orderkey = 100 ORDER BY COUNTRY DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スキーマも正しく認識されています
# MAGIC DESCRIBE extended lineitem;

# COMMAND ----------

# MAGIC %md 
# MAGIC Deltaファーマットにすることで、スキーマエンフォース機能で既存のスキーマと一致しないDML操作を拒否(デフォルト）することも、スキーマエボリューションでスキーマの違いを完璧にマージする柔軟性の高いデータ基盤を構築することも可能です。

# COMMAND ----------

# MAGIC %md ### タイムトラベル
# MAGIC <div style="float:left; padding-right:10px; margin-top:20px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-no-label.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   Delta Lakeのログを利用して<span style="color:green"><strong>以前のデータスナップショット</strong></span>を取得したり、<br>データ<span style="color:green"><strong>変更履歴の監査</strong></span>をしたり、<span style="color:green"><strong>ロールバック</strong></span>をすることが容易に出来ます
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe History機能でデータの変更履歴を確認
# MAGIC Describe History lineitem

# COMMAND ----------

# MAGIC %sql
# MAGIC -- APPENDされたデータを再度確認
# MAGIC SELECT * FROM lineitem 
# MAGIC where l_orderkey = 100 and l_partkey is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- バージョンを指定してスナップショットから過去データにアクセス
# MAGIC -- Version AS OF 0は新規作成直後のデータ
# MAGIC -- 削除したデータを確認
# MAGIC SELECT * FROM lineitem Version AS OF 0
# MAGIC where l_orderkey = 100 and l_partkey is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 初期状態のデータへのリストアも可能
# MAGIC RESTORE TABLE lineitem TO version as of 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe History機能でデータの変更履歴を確認
# MAGIC Describe History lineitem

# COMMAND ----------

# MAGIC %sql
# MAGIC -- リストア後のデータにアクセス
# MAGIC select * FROM lineitem where l_orderkey = 100;

# COMMAND ----------

# MAGIC %md ## パフォーマンスの向上
# MAGIC <div style="float:left; padding-right:10px; margin-top:10px;">
# MAGIC   <img src='https://jixjiadatabricks.blob.core.windows.net/images/data-lake-traditional.png' width='70px'>
# MAGIC </div>
# MAGIC <div style="float:left;">
# MAGIC   <div style="float:left;">
# MAGIC   Databricksでは簡単な設定で劇的に<span style="color:#f00"><strong>パフォーマンスの向上</strong></span>が可能です。<br>ここでは<br>(1)Deltaキャッシュ<br>(2)Photonエンジン<br>(3)Optimize機能<br>をご紹介します。
# MAGIC </div>

# COMMAND ----------

# MAGIC %md  初期状態での処理速度を最後確認します。まだParquetファイルからの変換直後のデータなのでパフォーマンスは同等です。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 全件検索
# MAGIC -- Delta変換直後は同じく40-50秒ほどかかる
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- フィルター条件付き
# MAGIC -- Delta変換直後は同じく9-10秒ほどかかる
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC where    l_shipmode = 'AIR' AND l_shipmonth = '10'
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Photon
# MAGIC <p  style="position:relative;width: 500px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/Photon.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

#PhotonエンジンをON
spark.conf.set("spark.databricks.photon.enabled","True")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 全件検索
# MAGIC -- 40-50秒から8秒程度に短縮 (5-6倍高速化)
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- フィルター条件付き
# MAGIC -- 7秒程度(フィルター条件付きのクエリではフルスキャンの量は変わらず、且つ計算対象のデータ量が少ないのでわずかな差しかでませんでした。次はよりI/O削減が可能なチューニングを行ってみます。)
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC where    l_shipmode = 'AIR' AND l_shipmonth = '10'
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %md
# MAGIC `Photonエンジン` を実行することで、特に大量データを処理するクエリが遥かに高速に処理できていることがわかります。性能の改善度合はクラスターの設定に依存しますが、多くの場合、SparkSQLエンジンと比較して**数倍程度**の性能改善が期待できます。<br>
# MAGIC 次はよりI/O削減が可能なチューニングを行ってみます。

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optimize
# MAGIC <p  style="position:relative;width: 1000px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/Optimize.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2カラムのZorderで3-4分程度かかる
# MAGIC -- `OPTIMIZE` はコンピュートリソースを必要とする処理ですのでデータ量が大きなテーブルでは時間がかかります。ユースケースによりますが、深夜にバッチ実行することが一般的です。最適化中もテーブルには検索/データ追加が可能です。
# MAGIC optimize lineitem zorder by (l_shipmode,l_shipmonth)

# COMMAND ----------

# MAGIC %md
# MAGIC <p  style="position:relative;width: 1000px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/bin_packing.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC `Optimize` を実行することで、200ファイルから16ファイルにファイル数が削減され、ファイル数の平均サイズも約256MBに自動調整がされました。<br>
# MAGIC パフォーマンスが改善されたか確認してみます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- フィルター条件付き
# MAGIC -- 1秒程度に短縮(Where条件検索ではZorderの効果でスキャンするファイル数が全16ファイルから対象データが含まれるファイルのみ削減されるため、非圧縮換算でパフォーマンスが改善されます)
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC where    l_shipmode = 'AIR' AND l_shipmonth = '10'
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- フィルター条件付き
# MAGIC -- 1秒程度に短縮(Where条件検索ではZorderの効果でスキャンするファイル数が全16ファイルから対象データが含まれるファイルのみ削減されるため、非圧縮換算でパフォーマンスが改善されます)
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC where   l_shipmonth = '10'
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %md
# MAGIC `OPTIMIZE` を実行することで、Databricks Deltaテーブルに対するクエリ、特にフィルター条件指定されているクエリがインデックス効果で遥かに高速に処理できていることがわかります。性能の改善度合はクラスター設定/クエリ条件に依存しますが、多くの場合、オプティマイズされていない場合と比較して**数倍以上**の性能改善が期待できます。
# MAGIC 
# MAGIC なお、 `OPTIMIZE` はコンピュートリソースを必要とする処理ですのでデータ量が大きなテーブルでは時間がかかります。ユースケースによりますが、深夜にバッチ実行することが一般的です。最適化中もテーブルには検索/データ追加が可能です。

# COMMAND ----------

# MAGIC %md
# MAGIC #### Deltaキャッシュ
# MAGIC 繰り返し参照されるデータに対するクラウドストレージからのデータリードを回避できますのでストレージコストを抑えながら高速なアクセスを実現できます
# MAGIC <p  style="position:relative;width: 1000px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/deltacache.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

#デルタキャッシングをON
spark.conf.set("spark.databricks.io.cache.enabled","True")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 全件検索（１回目のアクセスではまだキャッシュにデータがありませんので差はありません。）
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 全件検索(２回目)
# MAGIC -- キャッシュの効果で2秒程度に短縮　※当初は40-50秒のクエリ
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- フィルター条件付き（フルスキャンによりデータがキャッシュされているのでレスポンスタイムは向上しています。）　※当初は9-10秒のクエリ
# MAGIC select 
# MAGIC l_shipmode,
# MAGIC avg(l_quantity),
# MAGIC avg(l_extendedprice),
# MAGIC avg(l_discount),
# MAGIC avg(l_tax),
# MAGIC avg(l_quantity),
# MAGIC avg(l_partkey),
# MAGIC avg(l_suppkey),
# MAGIC avg(l_linenumber),
# MAGIC count(*)
# MAGIC from lineitem
# MAGIC where    l_shipmode = 'AIR' AND l_shipmonth = '10'
# MAGIC group by l_shipmode
# MAGIC order by l_shipmode;

# COMMAND ----------

# MAGIC %md
# MAGIC `Deltaキャッシュ` を使用することで、繰り返し参照されるデータに対するクラウドストレージからのデータリードを回避できますのでストレージコストを抑えながら高速なアクセスを実現できます。性能の改善度合はクラスター設定/クエリ条件に依存しますが、多くの場合において性能改善が期待できます。<br><br>
# MAGIC 今回は`Photonエンジン` `Optimize(ファイルサイズ最適化+ZOrder)` `Deltaキャッシュ`を使用することで、シングルクラスタ構成においても**約20倍のパフォーマンス向上**を達成しました。

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.ストリーミングデータのサポート
# MAGIC 
# MAGIC <img style="margin-top:25px;" src="https://jixjiadatabricks.blob.core.windows.net/images/databricks-logo-small-new.png" width="140">
# MAGIC <hr>
# MAGIC ここではDeltaアーキテクチャを使ってストリームデータの処理を行い、ハンズオンでSpark Structured Streamingの扱い方を見ていきます。<br>
# MAGIC また、バッチ処理と同様、ストリーミングデータに対する集計や加工などを段階的に実装していきますが、DeltaアーキテクチャではストリームのETL処理が非常に簡単にできます。<br>
# MAGIC 
# MAGIC ## Spark Structured Streaming(構造化ストリーミング)とは
# MAGIC Sparkではバッチ処理に非常に近い方法でストリーミングデータの処理が可能です。ストリームデータをマイクロバッチで小分けして、集計や処理を施します：
# MAGIC <div style="position:relative; left:-50px">
# MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/mokmok/streaming-microbatch-animated.gif" width="1200px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md ##フィットネス用トラッキングデバイスのIoTデータを使ったストリーミングデータ処理
# MAGIC ### このワークショップでは、
# MAGIC * IoTデバイスのデータキャプチャーのワークフローと同等の処理をS3上にあるデモデータセットを使ってをストリーミング処理をシミュレートします。
# MAGIC * [**IoT Device Streaming**](https://en.wikipedia.org/wiki/Customer_attrition) は大量のデータをストリーミングするために使用されますが、毎秒数百万のレコードをストリーミングするデバイスにとっては、インフラ上の大きな課題となります。Databricks Deltaを活用してこの問題を解決します。
# MAGIC 
# MAGIC ![stream](https://dupress.deloitte.com/content/dam/dup-us-en/articles/internet-of-things-wearable-technology/Runners_spot.jpg)  

# COMMAND ----------

# MAGIC %md ###Step 1: IoTデータのリアルタイム取り込み
# MAGIC -  心拍測定器の内部生成データセットを使用しています。

# COMMAND ----------

###########
# 環境準備
###########
# 最初にデモで使用する入力データ格納ホルダを削除しておきます。
dbutils.fs.rm(table_path + "/input/", True)

# 次にデモで使用するデータ書き出し先ホルダも削除しておきます。
dbutils.fs.rm(table_path + "/output/", True)

# 入力データ格納ホルダの作成
dbutils.fs.mkdirs(table_path + "/input/")
dbutils.fs.ls(table_path)

# COMMAND ----------

# デモデータのコピー(1ファイルあたり圧縮後2.6GB/5万件*10ファイル=50万件)
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00000.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00001.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00002.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00003.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00004.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00005.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00006.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00007.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00008.json.gz", table_path + "/input/")
dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00009.json.gz", table_path + "/input/")
dbutils.fs.ls(table_path + "/input/" )

# COMMAND ----------

# デバイスからの生データの取り込み
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 内部生成データセットの配置ホルダ
read_path = table_path + "/input/"

# 生データを格納するカラム
jsonSchema = StructType([
    StructField("value", StringType(), True) ])

# READストリームソース　は、1トリガーにつき4ファイルを処理する設定とする
deviceDataRaw = spark.readStream\
    .option("maxFilesPerTrigger", "4")\
    .schema(jsonSchema) \
    .json(read_path)

# READストリームへのアクセスはSparkストリーミングジョブを起動します。

# デバイスからの生データの表示してみます。
display(deviceDataRaw)

# COMMAND ----------

# MAGIC %md ###Step 2: ストリーミングで生データの加工処理を実施

# COMMAND ----------

############################################################################################################
# 続いて、プロパティをユーザの理解しやすいカラム名のエイリアスにマッピングし、特定のフィールドを前処理し、特定のデータをフィルタリングする処理を
# 同じデータセットにストリーミングで実行
############################################################################################################

# デバイスからの生データの整形処理を実行します。
deviceData = deviceDataRaw\
    .select(get_json_object('value', '$.user_id').alias('user_id') \
    ,get_json_object('value', '$.device_id').cast("int").alias('device_id') \
    ,get_json_object('value', '$.num_steps').cast("int").alias('num_steps') \
    ,get_json_object('value', '$.miles_walked').cast("int").alias('miles_walked') \
    ,get_json_object('value', '$.calories_burnt').cast("int").alias('calories_burnt') \
    ,get_json_object('value', '$.time_stamp').alias('time_stamp') )

##############################
# 整形したデータを表示してみます。
##############################
display(deviceData)

# COMMAND ----------

# MAGIC %md ###Step 3: バッチデータとリアルタイムデータの結合

# COMMAND ----------

# ユーザマスターデータ(静的なCSVファイルの構造化データ)のロード
####################################################
# 次にCSVファイルにある静的なデータからユーザ情報をロードします。
####################################################
userData = spark.read\
                .options(header='true', inferSchema='true')\
                .csv('/databricks-datasets/iot-stream/data-user/')

#############################
# ロードしたユーザマスタ情報の表示
#############################
display(userData)

# COMMAND ----------

#マスタデータとストリーミングデータの結合処理し、結合結果をストレージにDeltaフォーマットで書き出す

####################################################################
# 静的なデータセットとIOTデバイスのストリーミングデータの結合処理
# この処理も先ほどのストリーミングデータセットに対して実行します。
####################################################################
from pyspark.sql.types import StringType
from pyspark.sql.types import DateType

# ユーザーデータとストリーミングデータの結合
data = userData.join(deviceData, 
                     userData.userid == deviceData.user_id).drop(deviceData.user_id)

# リスクラベルのカラムを追加し、文字列としてキャスト 
data = data.withColumn("riskLabel", when(col('risk')==-10, 'E')
                       .when(col('risk')==0, 'D')
                       .when(col('risk')==5, 'C')
                       .when(col('risk')==10, 'B')
                       .when(col('risk')==20, 'A')
                       .cast(StringType()))

# イベントの日付でデータフレームライターを作成し、データパーティショニングに使用
dfWrite = data.withColumn("eventDate", data["time_stamp"].cast(DateType()))

# アウトプットデータの書き出し
dfWriteML = dfWrite.writeStream\
              .format("delta")\
              .partitionBy("eventDate")\
              .queryName("eventLogs-s3-parquet")\
              .option("path", table_path + "/output/")\
              .option("checkpointLocation",table_path + "/output/checkpoint/")\
              .start()

# COMMAND ----------

# MAGIC %md ###Step 4: バッチデータとストリーミングデータを使用した分析

# COMMAND ----------

# SQLでアクセスできるよう、外部テーブルを作成
sql("DROP TABLE IF EXISTS heartlogs")

sqltext = "\
CREATE TABLE heartlogs\
(\
userid integer,\
gender string ,\
age integer ,\
height integer ,\
weight integer,\
smoker string ,\
familyhistory string ,\
cholestlevs string ,\
bp string ,\
risk integer ,\
device_id integer ,\
num_steps integer,\
miles_walked integer ,\
calories_burnt integer ,\
time_stamp string ,\
riskLabel string ,\
eventDate date\
) \
USING DELTA \
PARTITIONED by (eventDate) \
LOCATION '" + table_path + "output" + "'"
print(sqltext)
sql(sqltext)

# データ表示
df = sql("select * from heartlogs order by userid,time_stamp")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended heartlogs

# COMMAND ----------

# MAGIC %sql
# MAGIC --　リアルタイムで蓄積しているストリーミングデータに対し、通常テーブルと同様に集計を行う。
# MAGIC select count(*) from heartlogs;

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC #次にテーブルを明示的にREADストリームと定義し、新規データのみを対象に集計を行なってみる。
# MAGIC #pythonでも可能だがSQLで実行してみる。
# MAGIC 
# MAGIC # heartlogsテーブルからREADストリームを作成
# MAGIC heartlogs_stream = spark.readStream.format("delta").table("heartlogs")
# MAGIC 
# MAGIC # SQLでアクセスできるよう、READストリームにビューを作成
# MAGIC heartlogs_stream.createOrReplaceTempView("heartlogs_stream_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- READストリームから作成したテーブルを使用して直近の1分間のデータを対象としたリアルタイム集計を行う。
# MAGIC 
# MAGIC -- リルタイムデータに対し次の処理を施す：
# MAGIC --(1) 「CountryCode」のカウントを1分毎に集計 (Window Function利用)
# MAGIC --(2) Windowのstart/end要素を抽出し、startタイムの順で並べ替え
# MAGIC --(3) カウントの結果を「Total」に命名
# MAGIC --(4) ストリーム処理結果を保存せず、リアルタイムで表示する
# MAGIC 
# MAGIC SELECT 
# MAGIC window, -- this is a reserved word that lets us define windows
# MAGIC window.start as Start,
# MAGIC window.end   as End,
# MAGIC userid,
# MAGIC count(1) as Total
# MAGIC -- READストリームに対してクエリを実行
# MAGIC FROM heartlogs_stream_table
# MAGIC GROUP BY 
# MAGIC userid,
# MAGIC window(current_timestamp, '1 minutes')  -- # withWatermark()でWindowの有効期間を1分に設定し、一定時間以上遅れて到着したイベントは無視することとする。
# MAGIC ORDER BY window,
# MAGIC userid
# MAGIC ;

# COMMAND ----------

# MAGIC %md * インプットデータを追加

# COMMAND ----------

import time
import datetime

####################################
# インプットデータを追加(50万レコード分)
####################################
for i in range(10):
  dt_now = datetime.datetime.now()
  print("追加:" + "/input/part-" + dt_now.strftime('%H%M%S') + ".json.gz")
  dbutils.fs.cp("dbfs:/databricks-datasets/iot-stream/data-device/part-00010.json.gz", table_path + "/input/part-" + dt_now.strftime('%H%M%S') + ".json.gz")
  time.sleep(1)

# COMMAND ----------

# MAGIC %md ###Step 5: Clean-up

# COMMAND ----------

# MAGIC %python
# MAGIC # ストリーミングを停止
# MAGIC 
# MAGIC # タイムアウト時間(millseconds)を変更
# MAGIC spark.conf.set("spark.sql.streaming.stopTimeout", 60000)
# MAGIC 
# MAGIC # Sreaming処理を停止
# MAGIC for s in spark.streams.active:
# MAGIC   print("Strams name:" , s.name)
# MAGIC   s.stop()
# MAGIC 
# MAGIC print("Stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC # まとめ：高信頼性と高パフォーマンスを実現するDelta Lake
# MAGIC Delta Lakeにより動的なデータと静的なデータを同様に扱え、分散ストレージ（即ち、データレイク）上のオブジェクトに対して直接ETL・ELTが行えます。<br>
# MAGIC データを高品質に保ちながらデータの速度（ストリーム or バッチ）や性質（構造化 or 半構造化）によって別々で処理する必要がなくなり、ビッグデータパイプラインの設計が著しく簡単になります。<br>
# MAGIC 
# MAGIC <div style="float:left; margin-top:0px; padding:0;">
# MAGIC   <h3>信頼性　==> DWHと同様の高品質なデータ管理</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>DMLサポート（INSERTだけではなくUPDATE/DELETE/MERGEをサポート）</li>
# MAGIC     <li>データ品質管理　(スキーマ・エンフォース/エボリューション)</li>
# MAGIC     <li>トランザクションログによるACIDコンプライアンスとタイムトラベル (データのバージョン管理)</li>
# MAGIC    </ul>
# MAGIC 
# MAGIC   <h3>パフォーマンス ==> Apache Sparkを凌駕するパフォーマンス</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>Photonエンジン</li>
# MAGIC     <li>Optimizeによるコンパクションとデータスキッピング</li>
# MAGIC     <li>Deltaキャッシング</li>
# MAGIC   </ul>
# MAGIC 
# MAGIC  <h3>バッチデータとストリーミングデータの統合 ==> シンプルなビッグデータパイプラインの設計</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>ストリーミングデータの取り込み</li>
# MAGIC     <li>ストリーミングデータのリアルタイムETL処理</li>
# MAGIC     <li>バッチデータとストリーミングデータに対する分析処理</li>
# MAGIC    </ul>
# MAGIC   </div>
# MAGIC </div>
# MAGIC 
# MAGIC <img src="https://jixjiadatabricks.blob.core.windows.net/images/delta_architecture_demo.gif" width="1200px">
# MAGIC 
# MAGIC ## 是非、Delta Lakeをお試しください！
