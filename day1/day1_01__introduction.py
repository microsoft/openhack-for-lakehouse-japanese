# Databricks notebook source
# MAGIC %md # Hack Day 1
# MAGIC ## 01. メダリオンアーキテクチャに基づいたデータエンジニアリング概要
# MAGIC ### 本ノートブックの目的：[メダリオンアーキテクチャ](https://www.databricks.com/jp/glossary/medallion-architecture)について理解を深める

# COMMAND ----------

# MAGIC %md
# MAGIC ### メダリオンアーキテクチャとは
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
# MAGIC - [What's Data Lake ?](https://docs.google.com/presentation/d/1pViTuBmK4nDWg4n8_yGKbN4gOPbbFUTw/edit?usp=sharing&ouid=110902353658379996895&rtpof=true&sd=true)

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
# MAGIC ![メダリオンアーキテクチャ](https://www.databricks.com/wp-content/uploads/2022/03/delta-lake-medallion-architecture-2.jpeg)

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
# MAGIC **参考:データソースの種類について**
# MAGIC - [Unity Catalogにおける外部ロケーション](https://learn.microsoft.com/ja-jp/azure/databricks/spark/latest/spark-sql/language-manual/sql-ref-external-locations)
