# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## Challenge2. Azure Data Factory と Azure Databricks の連携 (目安 16:45~17:00)
# MAGIC ### 本ノートブックの目的：Azure Data Factory から Azure Databricks ノートブックを実行する方法を学ぶ
# MAGIC - Q1. Data Factory スタジオにアクセスしてください
# MAGIC - Q2. Data Factory のパイプラインを作成してください
# MAGIC - Q3. Data Factory のパイプラインを実行し、結果を確認してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Data Factory とは
# MAGIC - フルマネージドのサーバーレス データ統合サービス
# MAGIC - 100 を超える組み込みのコネクターを追加コストなしで使用してデータ ソースを視覚的に統合し、ETL プロセスをコード不要で簡単に構築
# MAGIC 
# MAGIC (以下の画像をクリックすることで Data Factory の概要を説明する PDF が開きます)
# MAGIC <a href="https://github.com/microsoft/openhack-for-lakehouse-japanese/blob/main/deck/azure_data_factory_overview.pdf" target="_blank"><img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_overview.png" alt="Data Factory 概要"></a>
# MAGIC 
# MAGIC 参考リンク
# MAGIC - [Azure Data Factory の概要 - Azure Data Factory | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/data-factory/introduction)
# MAGIC - [アクティビティを使用して Databricks Notebook を実行する - Azure Data Factory | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/data-factory/transform-data-using-databricks-notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Data Factory スタジオにアクセスしてください
# MAGIC - Azure ポータル (https://portal.azure.com) にアクセスし、検索ボックスに「データ ファクトリ」と入力し、候補のサービスに表示される「データ ファクトリ」をクリックします
# MAGIC   - (Note) 英語 UI をご利用の場合は検索ボックスに「Data Factory」と入力してください
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/azure_portal_select_data_factory.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - 自身のチームで利用しているリソース グループの Data Factory のリンクをクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/azure_portal_list_data_factory.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - 「概要」メニューの画面中央の「スタジオの起動」をクリックします。すると別タブで Data Factory スタジオが起動します
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/azure_portal_launch_data_factory_studio.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - Data Factory スタジオのホーム画面が表示されることを確認します。
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_home.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - お好みで画面右上の歯車アイコンから日本語に設定します。
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_lang.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Data Factory のパイプラインを作成してください
# MAGIC - 左側メニューの「Author」をクリック、「ファクトリのリソース」の「+」ボタンをクリック、「パイプライン」>「パイプライン」をクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_create_pipeline.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - パイプラインの下書きが作成されます。作業領域を広めに確保するため、左側メニューの「<<」とファクトリ リソースの「<<」をクリックして表示を最小化します (キャプチャ省略)
# MAGIC - 画面右側にパイプラインのプロパティが表示されているはずです。「名前」を `day1_c2__azure_data_factory_pipeline1` に変更します。作業領域を広めに確保するため、以下キャプチャのアイコンをクリックしてプロパティを非表示にします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_pipeline_property.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - アクティビティの「Databricks」を展開し、その中にある「ノートブック」を右側の領域 (パイプライン デザイナー画面) にドラッグ & ドロップします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_notebook_1.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC <p>パイプライン デザイナー画面上の「Notebook1」という名前の箱 (この箱をアクティビティと総称します) をクリックします。すると画面下部にアクティビティの設定項目が表示されますので、必要な情報を入力していきます</p><br>
# MAGIC 
# MAGIC - 「Azure Databricks」タブをクリックします
# MAGIC   - 「Databricks のリンク サービス」の右横のリスト ボックスをクリックすると、一つだけリンク サービスが表示されますので、そちらを選択します
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_notebook_2.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - 「設定」タブをクリックします
# MAGIC   - 「ノートブックのパス」の右横の参照をクリックし、本ノートブック (`day1_c2__azure_data_factory`) を選択します
# MAGIC   - 「ベース パラメーター」の「+ 新規」をクリックし、以下の通り入力します
# MAGIC     - 名前: `input_text`
# MAGIC     - 値: `Foo Bar`
# MAGIC - 画面上部の「発行」をクリックし、変更を保存します
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_notebook_3.png" width="800">
# MAGIC 
# MAGIC 以上でパイプライン作成は完了です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 参考: リンク サービス
# MAGIC Data Factory から Databricks ノートブックを実行するためには、Azure Databricks 用のリンク サービスを作成しておく必要があります。Azure Databricks 用のリンク サービスでは、Azure Databricks ワークスペースの接続情報や Databricks ノートブック アクティビティを実行するクラスターのスペックやバージョンなどの情報を管理しています。
# MAGIC 
# MAGIC 本ハンズオンでは環境構築の際にリンク サービスを作成済みのため、受講者がリンク サービスを作成する必要はありません。どのような内容が定義されているかは、Data Factory の左側メニューの「Manage」 > 「Linked services」 > Azure Databricks のリンク サービスを選択することで確認できます。
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_linked_service_1.png" width="800">
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_linked_service_2.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Data Factory のパイプラインを実行し、結果を確認してください
# MAGIC - パイプラインの「デバッグ」を実行します。すると画面下部にデバッグの進捗状況が表示されます
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_debug.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - デバッグの進捗状況の Notebook1 にマウスをフォーカスすると眼鏡のようなアイコンが表示されるのでクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_debug_monitor_1.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - 詳細ダイアログの「実行ページ URL」をクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_debug_monitor_2.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - Databricks ワークスペースのジョブの実行画面に遷移します。どんな内容が表示されているか確認してみましょう
# MAGIC   - (Note) ジョブの実行画面には Databricks ワークスペースの左側メニューの「ワークフロー」 > 「ジョブ実行」 > 対象のジョブをクリックという流れでも遷移できます
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/databricks_job_result.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - Databricks のノートブック アクティビティは、Azure Databricks のジョブ クラスターで処理されます。ジョブ クラスターはジョブの開始時にクラスターが起動し、ジョブの完了とともにクラスターも終了します
# MAGIC - ジョブ クラスターの情報は、Databricks ワークスペースの左側メニューの「コンピューティング」 > 「Job compute」に表示されるので、見てみましょう
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/databricks_job_compute_1.png" width="800">
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/databricks_job_compute_2.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - Data Factory スタジオに戻ってパイプラインを表示します。デバッグの進捗状況の Notebook1 にマウスをフォーカスすると右矢印のようなアイコンが表示されるのでクリックします。これで出力結果が見られるのですが、`runOutput` に `raB ooF` と表示されているはずです。
# MAGIC - このノートブックはパラメーターとして指定された文字列を逆順にして返却するという非常に簡単な処理を行います。Notebook アクティビティのベース パラメーター `input_text` の値に指定した `Foo Bar` を逆順にした文字列が結果に表示されたことになります。
# MAGIC   - 簡単のために非常に単純な処理にしていますが、実際のデータ加工の現場では Databricks ノートブック アクティビティの処理結果を Data Factory パイプラインの後続のアクティビティで利用する際に使えるテクニックです
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c2__azure_data_factory/data_factory_studio_debug_output.png" width="800">
# MAGIC 
# MAGIC 本ノートブックの演習内容は以上です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 処理定義
# MAGIC - 以降のセルは変更不要です

# COMMAND ----------

# パラメーター (Widget) 定義
dbutils.widgets.text("input_text", "Hello World")
# dbutils.widgets.removeAll()

# COMMAND ----------

# 入力テキストを反転
input_text = dbutils.widgets.get("input_text")
reversed_text = input_text[::-1]
reversed_text

# COMMAND ----------

# 反転したテキストを返却する形で処理終了
dbutils.notebook.exit(reversed_text)
