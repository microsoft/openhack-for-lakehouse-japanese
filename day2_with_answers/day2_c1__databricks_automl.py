# Databricks notebook source
# MAGIC %md # Hack Day 2
# MAGIC ## Challenge1. 時系列予測以外の Databricks AutoML の実行
# MAGIC
# MAGIC Q1. 回帰と分類の Databricks AutoML の実行<br>
# MAGIC Q2. Shapley 値の確認<br>
# MAGIC Q3. SparkTrials によるハイパーパラメーターチューニング

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Q1. 回帰と分類の Databricks AutoML の実行**
# MAGIC
# MAGIC 次のリンク先の２つのノートブックを取り込み、回帰と分類の Databricks AutoML を実行してください。
# MAGIC
# MAGIC - 回帰のノートブック
# MAGIC   - [automl-classification-example - Databricks (microsoft.com)](https://learn.microsoft.com/ja-jp/azure/databricks/_extras/notebooks/source/machine-learning/automl-classification-example.html)
# MAGIC - 分類のノートブック
# MAGIC   - [automl-regression-example - Databricks (microsoft.com)](https://learn.microsoft.com/ja-jp/azure/databricks/_extras/notebooks/source/machine-learning/automl-regression-example.html)
# MAGIC
# MAGIC
# MAGIC 各ノートブックを実行する際には、次の作業を実施してください。
# MAGIC
# MAGIC 1. Databricks AutoML 実行時のパラメータの設定値の定義を確認して、必要に応じてパラメータを追加してください。詳細については、次のリンク先を確認してください。
# MAGIC
# MAGIC - [分類と回帰のパラメーター - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl/train-ml-model-automl-api#--classification-and-regression-parameters)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Q2. Shapley 値の確認**
# MAGIC
# MAGIC 最も精度が高いトレーニングノートブックにて。 Shapley 値を計算するために`shap_enabled = True`に変更して再実行してください。詳細については、次のリンク先を確認してください。
# MAGIC
# MAGIC - [モデルの説明を容易にするための Shapley 値 (SHAP) - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl/how-automl-works#shapley-values-shap-for-model-explainability)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **Q3. SparkTrials によるハイパーパラメーターチューニング**
# MAGIC
# MAGIC ハイパーパラメーターチューニングを Databricks 上で並列で実行するために、Databricks AutoML により生成されるノートブックを修正して実行してください。Databricks AutoML では複数の trial （ML モデルのトレーニング）を並列で実行しますが、生成されるノートブックは Databricks 上で並列処理されないコードとなっています。一部のコードを修正することで、Hyperopt というハイパーパラメーターチューニングのライブラリにより Databricks 上で高速に実行できます。Hyperopt の詳細については、参考リンクで確認してください。
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 1. 回帰の Databricks AutoML により生成された`LightGBMClassifier`モデルのトレーニングノートブック（ノートブック名例：`23-07-06-23:47-LightGBMClassifier-7b6c3ab37dbd6b45ae654c4126006bb8`）をクローンしてください<br>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 2. MLflow Experiment ID を取得してください<br>
# MAGIC
# MAGIC クローンしたノートブックにて次のようなコードを探し、`experiment_id`（例：`868801952129442`）の値を控えてください。
# MAGIC
# MAGIC ```python
# MAGIC with mlflow.start_run(experiment_id="868801952129442") as mlflow_run:
# MAGIC ```
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. space の変数値を変更してください<br>
# MAGIC
# MAGIC `space`変数の値を、変更後のコードに修正してください。
# MAGIC
# MAGIC 変更前のコード例
# MAGIC
# MAGIC ```python
# MAGIC space = {
# MAGIC   "colsample_bytree": 0.618172659302935,
# MAGIC   "lambda_l1": 4.298961472487383,
# MAGIC   "lambda_l2": 1.3418172424649684,
# MAGIC   "learning_rate": 0.08489082494398487,
# MAGIC   "max_bin": 139,
# MAGIC   "max_depth": 10,
# MAGIC   "min_child_samples": 22,
# MAGIC   "n_estimators": 2492,
# MAGIC   "num_leaves": 28,
# MAGIC   "path_smooth": 19.445910315189664,
# MAGIC   "subsample": 0.6400685031400503,
# MAGIC   "random_state": 852779329,
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC 変更後のコード
# MAGIC
# MAGIC ```python
# MAGIC from hyperopt.pyll.base import scope
# MAGIC space = {
# MAGIC   "colsample_bytree": hp.uniform('colsample_bytree', 0.4, 0.7),
# MAGIC   "lambda_l1": hp.uniform('lambda_l1', 0.0001, 1),
# MAGIC   "lambda_l2": hp.uniform('lambda_l2', 0.0001, 1),
# MAGIC   "learning_rate": hp.uniform('learning_rate', 0.001, 0.01),
# MAGIC   "max_bin": scope.int(hp.choice('max_bin', range(50,300))),
# MAGIC   "max_depth": scope.int(hp.choice('max_depth', range(4,15))),
# MAGIC   "min_child_samples": scope.int(hp.quniform('min_child_samples', 20, 50, 1)),
# MAGIC   "n_estimators": scope.int(hp.quniform('n_estimators', 50, 1000, 50)),
# MAGIC   "num_leaves": scope.int(hp.quniform('num_leaves', 4, 100, 4)),
# MAGIC   "path_smooth": hp.uniform('path_smooth',0.0, 3),
# MAGIC   "subsample": hp.uniform('subsample', 0.5, 1.0),
# MAGIC   "random_state": 852779329,
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. `fmin`メソッド実行時の traial を SparkTrial に変更してください<br>
# MAGIC
# MAGIC `fmin`メソッド実行時のコードを、変更後のコードに修正してください。
# MAGIC
# MAGIC 変更前のコード
# MAGIC
# MAGIC ```python
# MAGIC trials = Trials()
# MAGIC fmin(objective,
# MAGIC      space=space,
# MAGIC      algo=tpe.suggest,
# MAGIC      max_evals=1,  # Increase this when widening the hyperparameter search space.
# MAGIC      trials=trials)
# MAGIC ```
# MAGIC
# MAGIC 変更後のコード（`{experiment_id}`を取得した`experiment_id`の値に書き換える）
# MAGIC
# MAGIC ```python
# MAGIC from hyperopt import SparkTrials
# MAGIC trials = SparkTrials()
# MAGIC
# MAGIC mlflow.set_experiment(experiment_id="868801952129442")
# MAGIC
# MAGIC fmin(objective,
# MAGIC      space=space,
# MAGIC      algo=tpe.suggest,
# MAGIC      max_evals=30,
# MAGIC      trials=trials)
# MAGIC ```
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 5. ノートブックをすべて実行して、ML モデルの精度を確認してください。<br>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 参考リンク
# MAGIC
# MAGIC - [Hyperopt の概念 - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl-hyperparam-tuning/hyperopt-concepts)
# MAGIC - [ベスト プラクティス: Hyperopt を使用したハイパーパラメーターーのチューニング - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl-hyperparam-tuning/hyperopt-best-practices)
# MAGIC - [scikit-learn と MLflow を使用したハイパーパラメーターー チューニングの並列化 - Azure Databricks | Microsoft Learn](https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl-hyperparam-tuning/hyperopt-spark-mlflow-integration)

# COMMAND ----------


