# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC 
# MAGIC 実際にRawデータから加工してモデル学習＆デプロイまで構築するデモになります。以下のようなパイプラインを想定しております。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/overall.png' width='1200'/>

# COMMAND ----------

# MAGIC %md # 01. Create Delta Lake
# MAGIC Azure Blob Storage上のcsvデータを読み込み、必要なETL処理を実施した上でデルタレイクに保存するまでのノートブックになります。
# MAGIC 
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/maruyama/public_share/demo_end2end/1_createDelta.png' width='800' />

# COMMAND ----------

# MAGIC %run ./includes/setup $mode="init"

# COMMAND ----------

# DBTITLE 1,Define Variables
# 下準備
import re

#infraten_path = "/dbfs/tmp/"+ username
#load_path = f"dbfs:/user/hive/warehouse/{db_name}.db"

# データベース名を生成
db_name = database
spark.sql(f"USE {db_name}")

# データベースを表示
print(f"database_name: {db_name}")

# COMMAND ----------

# MAGIC 
# MAGIC %md # Data Overview
# MAGIC 店舗データを読み込み、プロファイリング、delta table化、メダリオンアーキテクチャーにそった形で機械学習用に使えるデータに整形しましょう!
# MAGIC 
# MAGIC 今回利用するデータセットの関連図です。
# MAGIC 
# MAGIC <br>
# MAGIC <img src='https://sajpstorage.blob.core.windows.net/mshack/images/olist_data_relation.png' width='800' />
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
# MAGIC | olist_sellers_dataset.csv | このデータセットには、Olistで行われた注文を処理した販売者のデータが含まれています。販売者の所在地を調べたり、どの販売者が各商品を販売したかを特定するために使用します。 |
# MAGIC | product_category_name_translation.csv | productcategorynameを英語に翻訳します。 |

# COMMAND ----------

# MAGIC %md ## Data Load
# MAGIC 
# MAGIC データをロードして、データプロファイルのチェックとdelta table化をやってみましょう。

# COMMAND ----------

# MAGIC %md ## Q1. テーブルからの読み込み
# MAGIC  - テーブルからデータを読み出して、Dataframe化を行なってください。

# COMMAND ----------

# olist_order_items_dataset
# table名 : (f"{db_name}.brz_order_items")
orderitemDF = spark.table("olist_order_items_dataset")

# olist_order_payments_dataset
# table名 : (f"{db_name}.brz_order_payments")
paymentDF = spark.table("olist_order_payments_dataset")

# olist_order_reviews_dataset
# table名 : (f"{db_name}.brz_order_reviews")
reviewsDF = spark.table("olist_order_reviews_dataset")

# olist_orders_dataset
# table名 : (f"{db_name}.brz_orders")
ordersDF = spark.table("olist_orders_dataset")

# olist_products_dataset
# table名 : (f"{db_name}.brz_products")
productDF = spark.table("olist_products_dataset")

# olist_sellers_dataset
# table名 : (f"{db_name}.brz_sellers")
sellersDF = spark.table("olist_sellers_dataset")

# product_category_name_translation
# table名 : (f"{db_name}.brz_product_category_name")
product_transDF = spark.table("product_category_name_translation")

# olist_customers_dataset
# table名 : (f"{db_name}.brz_product_customers_name")
customerDF = spark.table("olist_customers_dataset")

# olist_geolocation_dataset
# table名 : (f"{db_name}.brz_geo")
geoDF = spark.table("olist_geolocation_dataset")

# COMMAND ----------

# MAGIC %md ## データの可視化

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

# DBTITLE 1,ordersDFのnullチェック
from pyspark.sql.functions import *

# customerDFのデータチェック
display(ordersDF.select([count(when(col(c).isNull(), c)).alias(c) for c in ordersDF.columns]))

# COMMAND ----------

# DBTITLE 1,ordersDFの重複チェック
# order_idの重複がないかを確認
display(ordersDF.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,ordersDFのnullをDrop
from pyspark.sql.functions import *

# nullをna.drop()
ordersDF_null = ordersDF.na.drop(subset=['order_approved_at','order_delivered_carrier_date','order_delivered_customer_date'])
display(ordersDF_null.select([count(when(col(c).isNull(), c)).alias(c) for c in ordersDF_null.columns]))

# COMMAND ----------

# DBTITLE 1,後で利用するために一旦delta table化
# silverに変える
ordersDF_null.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable(f"{db_name}.sv_orders")

# COMMAND ----------

# DBTITLE 1,orderitemのDataFrameを確認
display(orderitemDF)

# COMMAND ----------

# DBTITLE 1,printSchema()で、Dataframeの型を確認
orderitemDF.printSchema()

# COMMAND ----------

# MAGIC %md ## Q2. Nullチェック
# MAGIC - orderitemDFのnullチェックを行なってください

# COMMAND ----------

# DBTITLE 1,orderitemDFのnullチェック
from pyspark.sql.functions import *

# orderitemDFのデータチェック
display(orderitemDF.select([count(when(col(c).isNull(), c)).alias(c) for c in orderitemDF.columns]))

# COMMAND ----------

# MAGIC %md ## Q3. 重複チェック
# MAGIC - orderitemDFの重複チェックを行なってください

# COMMAND ----------

# DBTITLE 1,orderitemDFでorder_idの重複がないかを確認
# 重複確認および削除
display(orderitemDF.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# 重複確認および削除
display(orderitemDF.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# MAGIC %md ## Q4.重複削除
# MAGIC - orderitemDFの重複したorder_idの削除と、デルタテーブル化を行なってください

# COMMAND ----------

# DBTITLE 1,orderitemDFのorder_idを削除
# 重複削除
# dropDuplicates()にて、重複削除が行えます
drop_orderitemDF = orderitemDF.drop_duplicates(['order_id'])

# 重複確認
display(drop_orderitemDF.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,後で利用するために一旦delta table化
# f"{db_name}.sv_order_items"として、delta tableを作成してください
drop_orderitemDF.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable(f"{db_name}.sv_order_items")

# COMMAND ----------

# MAGIC %md ## Q5. null・重複チェックの繰り返し
# MAGIC - customerDFのnullチェック・重複チェックを行なってください

# COMMAND ----------

# DBTITLE 1,customerDFのnullチェック
# customerDFのデータチェック
display(customerDF.select([count(when(col(c).isNull(), c)).alias(c) for c in customerDF.columns]))

# COMMAND ----------

# DBTITLE 1,customerDFでcustomer_idの重複がないかを確認
# customer_idの重複確認
display(customerDF.groupBy('customer_id').count().filter("count > 1"))

# COMMAND ----------

# MAGIC %md ## Q6. join
# MAGIC - customerDFとorderDF_nullをcustomer_id をkeyとして、inner joinしてください

# COMMAND ----------

# DBTITLE 1,customerDFとorderDF_nullをjoin
# customerDFとorder_nullDFをjoin
customer_orderDF = customerDF.join(ordersDF_null, "customer_id", 'inner')
display(customer_orderDF)

# COMMAND ----------

# MAGIC %md ## Q7. join
# MAGIC - customer_orderDFとdrop_orderitemDFをorder_idをkeyとしてinner joinしてください

# COMMAND ----------

# DBTITLE 1,customer_orderDFとdrop_orderitemDFをjoin
# customer_orderDFとdrop_orderitemをjoin
customer_order_itemDF = customer_orderDF.join(drop_orderitemDF, "order_id", 'inner')
display(customer_order_itemDF)

# COMMAND ----------

# DBTITLE 1,pyamentDFのnullチェック
# paymentDFのデータチェック
display(paymentDF.select([count(when(col(c).isNull(), c)).alias(c) for c in paymentDF.columns]))

# COMMAND ----------

# DBTITLE 1,paymentDFでpaymentidの重複チェック
# paymentidの重複確認および削除
display(paymentDF.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,paymentDFの重複削除
# 重複削除と確認
drop_paymentDF = paymentDF.dropDuplicates(['order_id'])
display(drop_paymentDF.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,customer_order_itemDFとdrop_paymentDFをjoin
# paymentデータもjoin
customer_order_item_payDF = (customer_order_itemDF.join(drop_paymentDF, ['order_id'], 'inner'))
display(customer_order_item_payDF)

# COMMAND ----------

# DBTITLE 1,customer_order_item_payDFのnullチェック
# customer_order_item_payDFのデータチェック
display(customer_order_item_payDF.select([count(when(col(c).isNull(), c)).alias(c) for c in customer_order_item_payDF.columns]))

# COMMAND ----------

# MAGIC %md ## Q8. cast
# MAGIC - reviewDFのreview scoreカラムをIntegerでcastしてください。

# COMMAND ----------

# DBTITLE 1,reviewDFのreview scoreをIntegerでcast
reviewsDF = reviewsDF.withColumn("review_score", F.col("review_score").cast("integer"))
reviewsDF.printSchema()

# COMMAND ----------

# DBTITLE 1,reviewDFのnullチェック
# reviewDFのnullチェック
display(reviewsDF.select([count(when(col(c).isNull(), c)).alias(c) for c in reviewsDF.columns]))

# COMMAND ----------

# DBTITLE 1,reviewDFのnull削除
# nullをna.drop()
reviewsDF_null = reviewsDF.na.drop()
display(reviewsDF_null.select([count(when(col(c).isNull(), c)).alias(c) for c in reviewsDF_null.columns]))

# COMMAND ----------

# DBTITLE 1,reviewDFでorder_idの重複チェック
# customer_idの重複確認
display(reviewsDF_null.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,reviewsDFのnull削除
# 重複削除と確認
drop_reviewsDF_null = reviewsDF_null.dropDuplicates(['order_id'])
display(drop_reviewsDF_null.groupBy('order_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,customer_order_item_payDFとdrop_reviewsDF_nullをjoin
# reviewデータもjoin
customer_order_item_pay_reviewDF = (customer_order_item_payDF.join(drop_reviewsDF_null, ['order_id'], 'inner'))
display(customer_order_item_pay_reviewDF)

# COMMAND ----------

# DBTITLE 1,customer_order_item_pay_reviewDFのnullチェック
# customer_order_item_pay_reviewDFのnullチェック
display(customer_order_item_pay_reviewDF.select([count(when(col(c).isNull(), c)).alias(c) for c in customer_order_item_pay_reviewDF.columns]))

# COMMAND ----------

# DBTITLE 1,productDFのnullチェック
# productDFのデータチェック
display(productDF.select([count(when(col(c).isNull(), c)).alias(c) for c in productDF.columns]))

# COMMAND ----------

# DBTITLE 1,productDFのnull削除
# nullをna.drop()
productDF_null = productDF.na.drop()
display(productDF_null.select([count(when(col(c).isNull(), c)).alias(c) for c in productDF_null.columns]))

# COMMAND ----------

# DBTITLE 1,productDFのproduct_idの重複チェック
# product_idの重複確認
display(productDF_null.groupBy('product_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,customer_order_item_pay_reviewDFとproductDFをjoin
# productデータもjoin
customer_order_item_pay_review_product_DF = (customer_order_item_pay_reviewDF.join(productDF_null, ['product_id'], 'inner'))
display(customer_order_item_pay_review_product_DF)

# COMMAND ----------

# DBTITLE 1,customer_order_item_pay_review_product_DFのnullチェック
# customer_order_item_pay_review_product_DFのデータチェック
display(customer_order_item_pay_review_product_DF.select([count(when(col(c).isNull(), c)).alias(c) for c in customer_order_item_pay_review_product_DF.columns]))

# COMMAND ----------

# DBTITLE 1,sellersDFのnullチェック
# sellersDFのデータチェック
display(sellersDF.select([count(when(col(c).isNull(), c)).alias(c) for c in sellersDF.columns]))

# COMMAND ----------

# DBTITLE 1,sellersDFでseller_idの重複チェック
# seller_idの重複確認
display(sellersDF.groupBy('seller_id').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,customer_order_item_pay_review_product_DFとsellersDFをjoin
# sellerデータもjoin
customer_order_item_pay_review_product_seller_DF = (customer_order_item_pay_review_product_DF.join(sellersDF, ['seller_id'], 'inner'))
display(customer_order_item_pay_review_product_seller_DF)

# COMMAND ----------

# DBTITLE 1,customer_order_item_pay_review_product_seller_DFのnullチェック
# customer_order_item_pay_review_product_seller_DFのデータチェック
display(customer_order_item_pay_review_product_seller_DF.select([count(when(col(c).isNull(), c)).alias(c) for c in customer_order_item_pay_review_product_seller_DF.columns]))

# COMMAND ----------

# DBTITLE 1,product_transDFのnullチェック
# product_transDFのデータチェック
display(product_transDF.select([count(when(col(c).isNull(), c)).alias(c) for c in product_transDF.columns]))

# COMMAND ----------

# DBTITLE 1,product_transDFでproduct_category_nameの重複チェック
# product_category_nameの重複確認
display(product_transDF.groupBy('product_category_name').count().filter("count > 1"))

# COMMAND ----------

# DBTITLE 1,customer_order_item_pay_review_product_seller_DFとproduct_transDFをjoin
# product_transDFもjoin
customer_order_item_pay_review_product_seller_trans_DF = (customer_order_item_pay_review_product_seller_DF.join(product_transDF, ['product_category_name'], 'inner'))
display(customer_order_item_pay_review_product_seller_trans_DF)

# COMMAND ----------

# DBTITLE 1,product_transDFのnullチェック
# product_transDFのデータチェック
display(customer_order_item_pay_review_product_seller_trans_DF.select([count(when(col(c).isNull(), c)).alias(c) for c in customer_order_item_pay_review_product_seller_trans_DF.columns]))

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

# DBTITLE 1,全てのデータセットをマージして抽出した特徴量を登録
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

dbName = f'olist_db_{user_name}'   # Database name
featureTableName = 'all_features'     # Table name

spark.sql(f'create database if not exists {dbName}')

#olist_feature_table = fs.create_feature_table( ## 
olistfeature_table = fs.create_table(
  name=f'{dbName}.{featureTableName}',
  primary_keys='order_id',
  schema=customer_order_item_pay_review_product_seller_trans_DF.schema,
  description='これらの特徴は、olist csvの各データをマージし、特徴量をorder_idをkeyとして1つにまとめたものです。'
)

fs.write_table(df=customer_order_item_pay_review_product_seller_trans_DF, name=f'{dbName}.{featureTableName}', mode='overwrite')

# COMMAND ----------

# MAGIC %md ## Q9. Visualize
# MAGIC - customer_order_item_pay_review_product_seller_trans_DFを利用して、review scoreのscoreごとのカウントを表示してください。

# COMMAND ----------

# DBTITLE 1,review scoreの状況
# review scoreの状況を見てみる
display(customer_order_item_pay_review_product_seller_trans_DF.groupby('review_score').count())

# COMMAND ----------

# MAGIC %md ## Q10. 列の生成 
# MAGIC - review scoreの割合をチェックしたいので、review scoreが3以上のものはpositiveとして1を、それ以下はnegativeとして0の値を新しい行として挿入してください。
# MAGIC - また、その結果を円グラフでポジティブとネガティブの割合を確認してください。

# COMMAND ----------

# DBTITLE 1,review_scoreの割合チェック
# スコアが3以上のものはpositive、それ以下はnegativeと想定して、1,0の値をつける
from pyspark.sql import types as T, functions as F
# +1する関数
def score_chk(v):
  if v >= 3 : 
    return 1 
  else :
    return 0

# UDFの宣言。引数１は関数、引数2は戻り値の型を渡す。
score_chk_udf = F.udf(score_chk, T.IntegerType())
# UDFを呼び出す
customer_order_item_pay_review_product_seller_trans_pnDF = customer_order_item_pay_review_product_seller_trans_DF.withColumn('review_score_1', score_chk_udf('review_score'))
customer_order_item_pay_review_product_seller_trans_pnDF.select('review_score', 'review_score_1').take(5)

# COMMAND ----------

# DBTITLE 1,goldテーブルとして登録
customer_order_item_pay_review_product_seller_trans_pnDF.write \
                                                        .format("delta") \
                                                        .mode("overwrite") \
                                                        .option("overwriteSchema", "true") \
                                                        .saveAsTable(f"{db_name}.gd_olist_all_data")


# COMMAND ----------

# DBTITLE 1,Summary 確認
# dataのsumamryをみてみる
display(customer_order_item_pay_review_product_seller_trans_pnDF.summary())

# COMMAND ----------

# MAGIC %md ## Q11. 列生成(UDF)
# MAGIC - priceの大まかな分布を図りたいので、priceカラムのsummaryから、25/50/75タイルを利用して、priceが75%カラム以上であれば、expensive、25%-75%の間はaffordable、その他はcheapとして、price_category列に値を挿入する関数をUDFで作成してください。
# MAGIC - その結果、price categoryごとのカウントをvisualizeしてください

# COMMAND ----------

# DBTITLE 1,priceカテゴリを生成
# 25% 44.9
# 50% 79.99
# 75% 144.9
from pyspark.sql import types as T, functions as F
# +1する関数
def fuc_price_category(v):
  if v >= 144.9 : 
    return 'expensive'
  elif v >= 44.9 and v < 144.9:
    return 'affordable'
  else :
    return 'cheap'

# UDFの宣言。引数１は関数、引数2は戻り値の型を渡す。
fuc_price_category_udf = F.udf(fuc_price_category, T.StringType())
# UDFを呼び出す
finalDF = customer_order_item_pay_review_product_seller_trans_pnDF.withColumn('price_category', score_chk_udf('price'))
finalDF.select('price', 'price_category').take(5)

# COMMAND ----------

# MAGIC %md ## Q12. EDA
# MAGIC - product_category_name_englishを利用して、上位10件の平均売上を計算してください
# MAGIC - seller_stateから、どの州が購入している人がおおいのかを計算してください
# MAGIC - seller_cityから、町での購入平均額を算出してください
# MAGIC - product_photos_qtyは商品に対する詳細画像の枚数になりますが、product_photos_qtyの枚数ごとの売上数を計算してください
# MAGIC - product_photos_qtyの枚数で、review scoreに影響があるかを確認したいため、product_photos_qtyごとのreview scoreの平均を計算してください
# MAGIC - review_comment_messageの長さがreview scoreに影響があるかを確認したいため、review scoreごとのreview_comment_messageの長さの平均を計算してください

# COMMAND ----------

display(finalDF.groupBy('product_category_name_english').agg(avg('price').alias('avg_price')).orderBy(F.col('avg_price').desc()))

# COMMAND ----------


