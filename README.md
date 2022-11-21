# OpenHack for Lakehouse [Japanese]
本リポジトリでは OpenHack for Lakehouse で利用する Azure Databricks ノートブックおよび関連リソースを管理します。

## ノートブックの参照方法
Azure Databricks ワークスペースで本リポジトリのノートブックを参照するには以下 2 つの方法があります。

1. Azure Databricks の [Repos](https://learn.microsoft.com/ja-jp/azure/databricks/repos/) 機能を用いて、Azure Databricks ワークスペースに本リポジトリをクローンする
2. 本リポジトリの [releases](https://github.com/microsoft/openhack-for-lakehouse-japanese/releases) に添付された最新の Databricks ノートブック (dbc) ファイルをダウンロードし Azure Databricks ワークスペースにインポートする

## リポジトリ構造
本リポジトリのフォルダ構造は以下の通りです。

### Azure Databricks ノートブック
- `dayN` フォルダに空白の回答欄を含む受講者用ノートブックがあります。
- `dayN_with_answers` フォルダに回答例を含むコーチ用ノートブックがあります。
- 学びの最大化のため、受講者は OpenHack の最中は `dayN_with_answers` フォルダのノートブックは極力参照しないことを推奨します。

### Azure 環境セットアップ スクリプト
- `infra` フォルダに Azure 環境セットアップのための Bicep および Bash スクリプトがあります。コーチ、または自己学習を希望する受講者は、これらのスクリプトを利用することで環境セットアップが可能です。

### その他のリソース
- `deck` フォルダに Azure サービスなどに関するスライド資料があります。必要に応じて参照してください。
- `images` フォルダはノートブックで参照する画像ファイルを管理するフォルダです。

## Contributing
This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks
This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
