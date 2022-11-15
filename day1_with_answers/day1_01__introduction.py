# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 01. メダリオンアーキテクチャに基づいたデータエンジニアリング概要
# MAGIC ### 本ノートブックの目的：Databricksにおけるデータ処理の基礎と[メダリオンアーキテクチャ](https://www.databricks.com/jp/glossary/medallion-architecture)について理解を深める

# COMMAND ----------

# MAGIC %md
# MAGIC ![メダリオンアーキテクチャ](https://www.databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg)

# COMMAND ----------

# MAGIC %md
# MAGIC ### メダリオンアーキテクチャとは
# MAGIC 
# MAGIC データを、Bronze、Silver、Goldの３層の論理レイヤーで管理する手法です。Databricks では、すべてのレイヤーを Delta Lake 形式で保持することが推奨されています。
# MAGIC 
# MAGIC | #    | データレイヤー | 概要                                                   | 類義語             |
# MAGIC | ---- | -------------- | ------------------------------------------------------ | ------------------ |
# MAGIC | 1    | Bronze         | 未加工データを保持するレイヤー                             | Raw     |
# MAGIC | 2    | Silver         | クレンジング・適合済みデータデータを保持するレイヤー | Enriched      |
# MAGIC | 3    | Gold           | ビジネスレベルのキュレート済みデータを保持するレイヤー   | Curated |
# MAGIC 
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC - [Medallion Architecture | Databricks](https://databricks.com/jp/glossary/medallion-architecture)
# MAGIC - [What's Data Lake ?](https://docs.google.com/presentation/d/1pViTuBmK4nDWg4n8_yGKbN4gOPbbFUTw/edit?usp=sharing&ouid=110902353658379996895&rtpof=true&sd=true)
# MAGIC 
# MAGIC 
# MAGIC 次のメリットがあります。
# MAGIC 
# MAGIC - データレイヤーごとの役割分担が可能となること
# MAGIC - データレイクにてデータ品質が担保できるようなること
# MAGIC - ローデータから再度テーブルの再作成が容易となること
# MAGIC 
# MAGIC 
# MAGIC **Bronzeの特徴について**
# MAGIC - 取り込んだローデータのコピーを、スキーマ展開を許可するなどそのまま保持。
# MAGIC - ロード日時などの監査列（システム列）を必要に応じて付加。
# MAGIC - データ型を文字型として保持するなどの対応によりデータ損失の発生を低減。
# MAGIC - データを削除する場合には、物理削除ではなく、論理削除が推奨。
# MAGIC 
# MAGIC **Silverの特徴について**
# MAGIC - Bronze のデータに処理を行い、クレンジング・適合済みデータを保持。
# MAGIC - スキーマを適用し、dropDuplicates関数を利用した重複排除等によるデータ品質チェック処理を実施。
# MAGIC - 最小限、あるいは「適度な」変換およびデータクレンジングルールのみを適用。
# MAGIC - Bronze との関係性が、「1 対多」方式となることもある。
# MAGIC 
# MAGIC **Goldの特徴について**
# MAGIC - 企業や部門のデータプロジェクトにおいてビジネス上の課題の解決するように編成・集計したデータを保持。
# MAGIC - アクセス制御リスト（ACL）や行レベルセキュリティ等のデータアクセス制御を考慮することが多い。
# MAGIC 
# MAGIC **参考:データソースの種類について**
# MAGIC - [Unity Catalogにおける外部ロケーション](https://learn.microsoft.com/ja-jp/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-external-locations)
