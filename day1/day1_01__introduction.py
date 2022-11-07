# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. メダリオンアーキテクチャに基づいたデータエンジニアリング概要

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1-1. メダリオンアーキテクチャとは
# MAGIC 
# MAGIC ソースシステムのデータを、Bronze、Silver、Goldの三層で管理する手法。
# MAGIC 
# MAGIC 次のメリットがあります。
# MAGIC 
# MAGIC - データレイヤーごとの役割が明確となること
# MAGIC - データ品質が担保されたデータの提供が可能となること
# MAGIC - ローデータから再度テーブルを再作成できること
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC - [Medallion Architecture | Databricks](https://databricks.com/jp/glossary/medallion-architecture)
# MAGIC - [What's Data Lake ? Azure Data Lake best practice - Speaker Deck](https://speakerdeck.com/ryomaru0825/whats-data-lake-azure-data-lake-best-practice?slide=18)

# COMMAND ----------

def mermeaid_display(code):
    mermeid_html =f"""
                    <html>
                        <head>
                            <link rel="stylesheet" href="https://unpkg.com/mermaid/dist/mermaid.min.css">
                            <script src="https://unpkg.com/mermaid/dist/mermaid.min.js"></script>
                            <script>mermaid.initialize({{startOnLoad:true,theme: 'neutral'}});</script>
                        </head>
                        <body>
                            <div class="mermaid">
                                {code}
                            </div>
                        </body>
                    </html>
                    """
    displayHTML(mermeid_html)
    
code = """
graph LR
    s010[source]
    r010[(Bronze)]
    e002[(Silver)]
    c002[( Gold )]

    s010 ==> r010 ==> e002 ==> c002

    subgraph メダリオンアーキテクチャ
        r010
        e002
        c002
    end

  classDef done fill:#aaa;
  classDef naosim fill:#faa;
  class T0,T1,T3 done;
  class T2 naosim;
"""

mermeaid_display(code)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC | #    | データレイヤー | 概要                                                   | 類義語             |
# MAGIC | ---- | -------------- | ------------------------------------------------------ | ------------------ |
# MAGIC | 1    | Bronze         | 生データを保持するレイヤー                             | Raw、Data lake     |
# MAGIC | 2    | Silver         | ソースシステムと同様の粒度で検証済みのデータを保持するレイヤー | Enriched、DWH      |
# MAGIC | 3    | Gold           | 集計したデータを保持するレイヤー   | Curated、Data Mart |
# MAGIC 
# MAGIC 
# MAGIC **Bronzeの特徴について**
# MAGIC 
# MAGIC - 取り込んだ生データのコピーを、履歴として保持。
# MAGIC - データを削除する場合には、物理削除ではなく、論理削除が推奨。
# MAGIC - スキーマ展開を許可するなどソースシステム側の変更対応を容易化。
# MAGIC - データ型を文字型として保持するなどシステムエラーの発生を低減。
# MAGIC 
# MAGIC **Silverの特徴について**
# MAGIC - Bronzeのデータに基づき、ソースシステムと同等のデータ粒度で保持。
# MAGIC - スキーマを適用し、dropDuplicates関数を利用した重複排除等によるデータ品質チェック処理を実施。
# MAGIC 
# MAGIC 
# MAGIC **Goldの特徴について**
# MAGIC - データ利活用（消費）の目的に合致するように編成・集計したデータを保持。
# MAGIC - ACLや行レベルセキュリティ等のデータアクセス制御を考慮することが多い。
# MAGIC 
# MAGIC 参考リンク
# MAGIC 
# MAGIC - [データ ランディング ゾーンごとに 3 つの Azure Data Lake Storage Gen2 アカウントをプロビジョニングする - Cloud Adoption Framework | Microsoft Docs](https://docs.microsoft.com/ja-jp/azure/cloud-adoption-framework/scenarios/data-management/best-practices/data-lake-services)
