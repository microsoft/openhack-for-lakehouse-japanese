# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## Challenge5. Microsoft Power BI と Azure Databricks の連携
# MAGIC ### 本ノートブックの目的：Power BI レポートから Databricks SQL ウェアハウスに接続して視覚化する方法を学ぶ
# MAGIC - Q1. Power BI Desktop から Databricks SQL ウェアハウスに接続してください
# MAGIC - Q2. Power BI Desktop でレポートを作成してください

# COMMAND ----------

# MAGIC %md
# MAGIC ## 重要なガイダンス (必ず目を通すこと)
# MAGIC - 本ノートブックはコーチがデモ ベースで説明することを前提としています
# MAGIC   - コーチが利用する端末の OS が Windows の場合は Power BI Desktop をインストールしておいてください
# MAGIC   - コーチが利用する端末の OS が Windows 以外の場合は Windows 端末を持つ他チームのコーチにデモを依頼してください
# MAGIC - 受講者も Power BI Desktop が利用可能であれば、本ノートブックの内容を実施頂くことが可能です
# MAGIC   - 実施は任意で、コーチによるデモを閲覧するのみでも全く問題ありません
# MAGIC   - Power BI Desktop の最新版は https://powerbi.microsoft.com/ja-jp/downloads/ からダウンロードできます

# COMMAND ----------

# MAGIC %md
# MAGIC ## Microsoft Power BI とは
# MAGIC ### Microsoft Power Platform
# MAGIC - Power BI は Microsoft Power Platform のサービスの一つ
# MAGIC - Power Platform には他にもアプリケーション開発のための Power Apps, ワークフロー自動化のための Power Automate, インテリジェント仮想エージェントの Power Virtual Agents といったサービスがある
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_overview_1.jpg" width="800">
# MAGIC 
# MAGIC ---
# MAGIC ### Power BI の役割
# MAGIC - Power BI はデータと意思決定をつなぐ架け橋の役割を果たす
# MAGIC - クラウドやオンプレミス、SaaS データを Power BI に連携し、データの分析・豊富な可視化によりインサイトを導き出す
# MAGIC - Web や Teams、PowerPoint など様々な場所でユーザーの意思決定に役立てられる
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_overview_2.jpg" width="800">
# MAGIC 
# MAGIC ---
# MAGIC ### Power BI の全体像
# MAGIC - Power BI は Power BI Desktop, Power BI サービス, Power BI モバイル / ブラウザからなる
# MAGIC - Power BI Desktop はレポート作成用の完全無料のデスクトップ ツール、Windows で利用可能、データの取得・加工・可視化までワンストップで行える
# MAGIC - Power BI サービスは Power BI レポートなどのコンテンツを共有・管理するためのクラウド環境 (複数人にレポートを共有する場合はライセンスが必要)
# MAGIC - Power BI モバイル / ブラウザにより、コンテンツを Web / モバイルで参照 / 分析できる
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_overview_3.jpg" width="800">
# MAGIC 
# MAGIC ---
# MAGIC ### Power BI の市場評価
# MAGIC - 米国調査会社 Gartner 発表の「アナリティクス & ビジネス インテリジェンス プラットフォーム」分野の 2022 年 3 月のマジック クアドラントにて Microsoft はリーダーとして選出
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_overview_4.jpg" width="800">
# MAGIC 
# MAGIC ---
# MAGIC ### 顧客が Power BI を選択する 10 の理由
# MAGIC - 上から順番に Teams や Excel などの Office 365 製品との統合 / 100 を超える標準データ コネクタ / Azure サービスと統合した高度なビッグ データ分析システムの構築などの理由で顧客は Power BI を選択
# MAGIC   - 英語の原文 https://powerbi.microsoft.com/ja-jp/blog/microsoft-named-a-leader-in-2021-gartner-magic-quadrant-for-analytics-and-bi-platforms/
# MAGIC 
# MAGIC No. | 理由
# MAGIC --- | ---
# MAGIC 1 | 普段ご利用の Teams や Excel などの Office 365 製品との統合による、BI 領域での生産性向上
# MAGIC 2 | 100 を超える標準データコネクタとデータ加工用の Power Query
# MAGIC 3 | 高度なビッグデータ分析システムを構築するための Microsoft Azure Synapse Analytics との統合
# MAGIC 4 | ローコードアプリ開発ツールである Power Apps との統合によるインサイト(BI)からのアクション
# MAGIC 5 | Microsoft Information Protection と Microsoft Defender for Cloud Apps による、BI 層での Data Loss Prevention (DLP) 実装
# MAGIC 6 | 標準の AI だけでなく Azure Machine Learning を利用することによるレポートでの高度な AI 機能の実現
# MAGIC 7 | モバイル向けのアプリケーション提供
# MAGIC 8 | 世界中の多くの企業で利用され信頼されている Microsoft クラウドでの提供
# MAGIC 9 | ユーザーからのフィードバックに基づき Monthly での新機能のリリース
# MAGIC 10 | 全ての人にデータ活用を体験頂くための価格設定

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Power BI Desktop から Databricks SQL ウェアハウスに接続してください
# MAGIC Databricks SQL の画面から SQL ウェアハウスへの接続情報のみが定義された Power BI レポート用のファイル (拡張子 `pbids`) を数クリックでダウンロードできます。この PBIDS ファイルを使って SQL ウェアハウスに接続していきます。

# COMMAND ----------

# MAGIC %md
# MAGIC - Databricks SQL の「SQL ウェアハウス」のページにアクセスします
# MAGIC - 既存の SQL ウェアハウスをクリックし「接続の詳細」タブにアクセスします
# MAGIC - 画面下部にある「Power BI」アイコンをクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/sql_warehouse_powerbi.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - 「パートナーに接続ダイアログ」が表示されるので「接続ファイルをダウンロード」をクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/download_pbids.png" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC - PBIDS ファイルを開くための前提条件を確認するダイアログが表示されるので、前提条件を問題なく満たしている場合は「閉じる」をクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/prereq_pbids.png" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC - ダウンロードされた PBIDS ファイルを開きます。Power BI Desktop が起動するはずです
# MAGIC - PBIDS ファイルの初回オープン時に以下キャプチャのような Azure Databricks 接続の認証情報を求めるダイアログが表示されます。個人用アクセス トークンなどが選べますが、ここでは Azure Active Directory を認証情報として用います
# MAGIC - Azure Active Directory のサインインが正常に完了したら「接続」ボタンをクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_connect_adb.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Power BI Desktop でレポートを作成してください
# MAGIC これまでの演習で Databricks に作成済みの `sales_history_gold` テーブルを使ってレポートを作成します。

# COMMAND ----------

# MAGIC %md
# MAGIC - ナビゲーター画面で `sales_history_gold` テーブルを選択して「読み込み」をクリックします
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_navigator.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC - 折れ線グラフをプロット、X 軸に `purchase_date` を、Y 軸に `sales の合計` を選択します
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/microsoft/openhack-for-lakehouse-japanese/main/images/day1_c5__powerbi/powerbi_line_graph.png" width="800">

# COMMAND ----------

# MAGIC %md
# MAGIC これで本ノートブックの演習は完了です。
