## デプロイ

/infra/code/manualdeploy.azcli内のコードを実行
devcontainerを利用しない場合は、/infra/.devcontainer/devcontainer.env の内容を環境変数に設定しておくこと
 
## デプロイ後


## user 設定など
Trainerリソースデプロイ出力からdevcontainer.envを編集

cd infra/code/databricks_setup/

``` bash :
bash deploy.sh >> `date +%Y%m%d_%H-%M-%S`.log

``` 

