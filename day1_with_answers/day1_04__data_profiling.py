# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 04. メダリオンアーキテクチャ構築の実践 - Data Profile - (目安 14:15~14:45)
# MAGIC ### 本ノートブックの目的：Data Profile機能を使った、数値・文字列・日付の各列の基本統計量と各列の値分布のヒストグラムの表示方法を理解する
# MAGIC Q1. Data Profileを確認して前処理方針を決定しよう

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="3"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q1. Data Profileを確認して前処理方針を決定しよう

# COMMAND ----------

# MAGIC %md ### Data Overview
# MAGIC 店舗データを読み込み、プロファイリング、delta table化、メダリオンアーキテクチャーにそった形で機械学習用に使えるデータに整形しましょう!
# MAGIC 
# MAGIC 今回利用するデータセットの関連図です。
# MAGIC 
# MAGIC <br>
# MAGIC <img src='https://github.com/skotani-db/databricks-hackathon-jp/raw/main/images/olist_data_relation.png' width='800' />
# MAGIC </br>
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC | データ名 | 内容 |
# MAGIC | - | - |
# MAGIC | olist_customers_dataset.csv | このデータセットには、顧客とその所在地に関する情報が含まれています。これを用いて、受注データセットに含まれる固有の顧客を特定したり、 受注の配送先を探したりします。私たちのシステムでは、各注文に一意な customerid が割り当てられています。つまり、同じ顧客でも注文によって異なる ID が与えられるということです。データセットに customerunique_id を設定する目的は、その店舗で再購入した顧客を識別できるようにするためです。そうでなければ、それぞれの注文に異なる顧客が関連付けられていることに気づくでしょう。 |
# MAGIC | olist_geolocation_dataset.csv | このデータセットには、ブラジルの郵便番号とその緯度経度座標の情報が含まれている。地図を作成したり、売り手と顧客の間の距離を調べるのに使用します。|
# MAGIC | olist_order_items_dataset.csv | このデータセットには、各注文の中で購入された商品に関するデータが含まれています。 |
# MAGIC | olist_order_payments_dataset.csv | このデータセットには、注文の支払いオプションに関するデータが含まれています。|
# MAGIC | olist_order_reviews_dataset.csv | このデータセットには、顧客によるレビューに関するデータが含まれている。顧客がOlist Storeで製品を購入すると、販売者にその注文を履行するよう通知される。顧客が製品を受け取ると、あるいは配送予定日になると、顧客は満足度アンケートをメールで受け取り、購入体験のメモやコメントを書き込むことができます。|
# MAGIC | olist_orders_dataset.csv | これがコアとなるデータセットです。各注文から他のすべての情報を見つけることができるかもしれません。 |
# MAGIC | olist_products_dataset.csv | このデータセットには、Olistが販売する製品に関するデータが含まれています。 |
# MAGIC | olist_sellers_dataset.json | このデータセットには、Olistで行われた注文を処理した販売者のデータが含まれています。販売者の所在地を調べたり、どの販売者が各商品を販売したかを特定するために使用します。 |
# MAGIC | product_category_name_translation.csv | productcategorynameを英語に翻訳します。 |

# COMMAND ----------

# MAGIC %md
# MAGIC ToDo : day1_03__bronzeで作成したテーブルのData Profileを確認し、欠損値・重複・その他気づきを整理し、チーム内で共有してみる<br>
# MAGIC Hint1 : https://www.databricks.com/blog/2021/12/07/introducing-data-profiles-in-the-databricks-notebook.html <br>
# MAGIC Hint2 : 上記のData Overviewを参考に結合キーになりそうなカラムのプロファイルは重点的に観察する <br>
# MAGIC Hint3 : Data Profileで確認できる情報以外にも気になる点があればクエリしてみる

# COMMAND ----------

# DBTITLE 1,order_items_bronzeのData Profileを確認する
# MAGIC %sql
# MAGIC SELECT * FROM order_items_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC <気づき> 
# MAGIC - xxx
# MAGIC - xxx

# COMMAND ----------

# DBTITLE 1,order_reviews_bronzeのData Profileを確認する
# MAGIC %sql
# MAGIC SELECT * FROM order_reviews_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ToDo : review scoreのscoreごとのカウントを表示し、Data Visualization機能を使ってノートブックダッシュボードに円グラフを表示してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   review_score,
# MAGIC   count(*)
# MAGIC FROM
# MAGIC   order_reviews_bronze
# MAGIC GROUP BY
# MAGIC   review_score;

# COMMAND ----------

# MAGIC %md
# MAGIC <気づき> 
# MAGIC - xxx
# MAGIC - xxx

# COMMAND ----------

# DBTITLE 1,orders_bronzeのData Profileを確認する
# MAGIC %sql
# MAGIC SELECT * FROM olist_orders_dataset_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC <気づき> 
# MAGIC - xxx
# MAGIC - xxx

# COMMAND ----------

# DBTITLE 1,sellers_bronzeのData Profileを確認する
# MAGIC %sql
# MAGIC -- seller.<key>を指定することで構造体の中の値にアクセスできる
# MAGIC SELECT
# MAGIC   seller_id,
# MAGIC   <<FILL-IN>>
# MAGIC   seller
# MAGIC FROM
# MAGIC   olist_sellers_dataset_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC <気づき> 
# MAGIC - xxx
# MAGIC - xxx
