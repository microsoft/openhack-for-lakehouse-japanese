# デプロイ方法

## 前提

- VSCode devcontainer 環境
- サブスクリプション所有者
- - あらかじめリソースグループを作成済みの場合はリソースグループ所有者でOK

## デプロイリソース

### トレーナー用リソースグループ

- databricks workspace
- data factory 
- azure data lake storage gen2
- databricks access connector

### 生徒用リソースグループ

- databricks workspace
- data factory 

## 方法

- .devcontainer/devcontainer.env を作成し、以下を追加する

```
TRAINER_ARM_SUBSCRIPTION_ID=<講師用サブスクリプションID>
STUDENT_ARM_SUBSCRIPTION_ID=<生徒用サブスクリプションID>
```

- ./code/student_rg_list.txt 上に生徒用リソースグループ名を記入します。
- - 必要に応じて、./environments/dev/trainer.tfvars の内容でトレーナー用リソースグループを変更することが可能です。
- .devcontainer に基づいてdevcontainer を立ち上げます。
- 以下のコマンドを実行します。実行した際にaz loginが試行された場合は、対象のテナントでログインをしてください。

```

bash ./code/deploy.sh

```

- - 必要に応じてdeploy.sh 内で mode="plan" に変更することでterraform planに変更することができます。