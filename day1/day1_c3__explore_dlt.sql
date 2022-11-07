-- Databricks notebook source
-- MAGIC %md # Hack Day 1
-- MAGIC ## Challenge3. Delta Live Tablesを使ったパイプラインの構築方法
-- MAGIC ### 本ノートブックの目的：SQLに基づくDelta Live Tablesのパイプライン構築方法を学ぶ

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def display_slide(slide_id, slide_number):
-- MAGIC   displayHTML(f"""
-- MAGIC   <div style="width:1150px; margin:auto">
-- MAGIC   <iframe
-- MAGIC     src="https://docs.google.com/presentation/d/{slide_id}/embed?slide={slide_number}"
-- MAGIC     frameborder="0"
-- MAGIC     width="1150"
-- MAGIC     height="683"
-- MAGIC   ></iframe></div>
-- MAGIC   """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display_slide("1ShJUFjNfkPvn5QjG7dDFi6nn66cQZfVMs1cjjzErEIE", 6)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ブロンズレイヤーテーブルを宣言する（Declare Bronze Layer Tables）
-- MAGIC 
-- MAGIC 以下では、ブロンズレイヤーを実装する2つのテーブルを宣言します。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### dlt_orders_bronze
-- MAGIC 
-- MAGIC  **`dlt_orders_bronze`** は、 */dbfs/FileStore/db_hackathon4lakehouse_2022/datasource/dlt/orders/* にあるデータセットからcsvデータを段階的に取り込みます。
-- MAGIC 
-- MAGIC （構造化ストリーミングと同じ処理モデルを使用した）<a herf="https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html" target="_blank">Auto Loader</a>を介した増分処理は、以下のように宣言に  **`STREAMING`**  キーワードを追加する必要があります。  **`cloud_files()`** メソッドを使うと、Auto LoaderをSQLでネイティブに使用できます。  **`cloud_files()`** メソッドは、次の位置パラメーターを取ります。
-- MAGIC * 上記の通り、ソースの場所
-- MAGIC * ソースデータフォーマット。今回の場合はJSONを指す
-- MAGIC * 任意読み取りオプションの配列。 この場合、 **`cloudFiles.inferColumnTypes`** を **`true`** に設定します。
-- MAGIC * 任意読み取りオプションの配列。 この場合、 **`cloudFiles.overwriteSchema`** を **`true`** に設定します。
-- MAGIC * 任意読み取りオプションの配列。 この場合、 **`cloudFiles.schemaHints`** を **day1_03__bronze Cmd 16** で設定したスキーマに設定します。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE dlt_orders_bronze AS
SELECT
  *
FROM
  cloud_files("/FileStore/db_hackathon4lakehouse_2022/datasource/dlt/orders/", "csv",
  map(
    "cloudFiles.inferColumnTypes", "true",
    "overwriteSchema", "true",
    "cloudFiles.schemaHints",
    " `order_id` STRING,
      `customer_id` STRING,
      `order_status` STRING,
      `order_purchase_timestamp` TIMESTAMP,
      `order_approved_at` TIMESTAMP,
      `order_delivered_carrier_date` TIMESTAMP,
      `order_delivered_customer_date` TIMESTAMP,
      `order_estimated_delivery_date` TIMESTAMP,
      `_rescued_data` STRING "
      )
   );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### dlt_order_items_bronze
-- MAGIC 
-- MAGIC  **`dlt_order_items_bronze`** は、 */dbfs/FileStore/db_hackathon4lakehouse_2022/datasource/dlt/order_items/* にあるデータセットからcsvデータを段階的に取り込みます。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE dlt_order_items_bronze AS
SELECT
  *
FROM
  cloud_files("/FileStore/db_hackathon4lakehouse_2022/datasource/dlt/order_items/", "csv", 
  map("cloudFiles.inferColumnTypes", "true",
    "overwriteSchema", "true",
  "cloudFiles.schemaHints", 
  " `order_id` STRING,
    `order_item_id` INT,
    `product_id` STRING,
    `seller_id` STRING,
    `shipping_limit_date` STRING,
    `price` DOUBLE,
    `freight_value` DOUBLE,
    `_rescued_data` STRING "
    )
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## シルバーレイヤーテーブルを宣言する（Declare Silver Layer Tables）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ### dlt_order_items_silver
-- MAGIC 
-- MAGIC  **`dlt_order_items_silver`** は、 **`dlt_order_items_bronze`** をソースに作成します
-- MAGIC  
-- MAGIC #### DLTテーブルとビューの参照（References to DLT Tables and Views）
-- MAGIC 他のDLTテーブルとビューへの参照は、常に **`live.`** プレフィックスを含みます。 ターゲットのデータベース名はランタイム時に自動で置き換えられるため、DEV/QA/PROD環境間でのパイプラインの移行が簡単に行えます。
-- MAGIC 
-- MAGIC #### ストリーミングテーブルの参照（References to Streaming Tables）
-- MAGIC 
-- MAGIC ストリーミングDLTテーブルへの参照は **`STREAM()`** を使用して、テーブル名を引数として渡します。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE dlt_order_items_silver AS (
  SELECT
    *,
    price + freight_value product_sales
  FROM
    STREAM(LIVE.dlt_order_items_bronze)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### dlt_orders_silver
-- MAGIC 
-- MAGIC  **`dlt_orders_silver`** は、 **`dlt_orders_bronze`** をソースに作成します
-- MAGIC 
-- MAGIC この宣言は多くの新しい概念を導入します。
-- MAGIC 
-- MAGIC #### 品質管理（Quality Control）
-- MAGIC 
-- MAGIC  **`CONSTRAINT`** キーワードで品質管理を導入します。 従来の **`WHERE`** 句の機能と同じように、 **`CONSTRAINT`** はDLTと統合することで制約違反のメトリクスを集めることができます。 制約はオプションの **`ON VIOLATION`** 句を提供し、制約違反のレコードに対して実行するアクションを指定します。 現在DLTでサポートされている3つのモードは以下の通りです
-- MAGIC 
-- MAGIC |  **`ON VIOLATION`**  | 動作                             |
-- MAGIC | ------------------ | ------------------------------ |
-- MAGIC |  **`FAIL UPDATE`**   | 制約違反が発生した際のパイプライン障害            |
-- MAGIC |  **`DROP ROW`**      | 制約違反のレコードを破棄する                 |
-- MAGIC | 省略                 | 制約違反のレコードが含まれる（但し、違反はメトリクスで報告） |
-- MAGIC 
-- MAGIC 今回のケースでは **order_status** が`delivered`または`shipped`である場合、配送日を示す`order_delivered_carrier_date`が欠損であることはデータに何らかの異常があると考えられるので、レコードとして削除します。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE dlt_orders_silver(
  CONSTRAINT valid_delivered_date EXPECT 
    (order_delivered_carrier_date IS NOT NULL) ON VIOLATION DROP ROW)
AS (
  SELECT
    *,
    CAST(
      date_format(order_purchase_timestamp, 'yyyy-MM-dd') AS DATE
    ) purchase_date
  FROM
    STREAM(LIVE.dlt_orders_bronze)
  WHERE
    order_status = 'delivered' OR order_status = 'shipped'
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### dlt_order_info_silver
-- MAGIC 
-- MAGIC  **`dlt_order_info_silver`** は、 **`dlt_orders_silver`** と **`dlt_order_items_silver`** をソースに作成します

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE dlt_order_info_silver AS (
  SELECT
    a.order_id,
    seller_id,
    product_id,
    purchase_date,
    product_sales
  FROM
    STREAM(LIVE.dlt_orders_silver) a
    JOIN STREAM(LIVE.dlt_order_items_silver) b ON a.order_id = b.order_id
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 
-- MAGIC ## ゴールドテーブルを宣言する（Declare Gold Table）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### dlt_sales_history_gold
-- MAGIC 
-- MAGIC  **`dlt_sales_history_gold`** は、 **`dlt_order_info_silver`** をソースに作成します
-- MAGIC  
-- MAGIC  年月日によって集計を行い、テーブルを完全に書き換える増分的でない処理のため **`STREAMING`**  キーワードがなくなっていることに注意してください

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dlt_sales_history_gold AS (
SELECT
  purchase_date,
  SUM(product_sales) sales
FROM
  LIVE.dlt_order_info_silver
GROUP BY
  purchase_date
ORDER BY
  purchase_date asc
  );
