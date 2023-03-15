# Databricks notebook source
dbutils.widgets.dropdown("shap_enabled", "true", ["true", "false"], "Compute shape feature importance")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Use the best Auto-ML generated notebook to bootstrap our ML Project
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-ml-experiment.png" style="float: right" width="600px">
# MAGIC 
# MAGIC Databricks Autt-ML tries many models and generate notebooks containing the code used to build the model.
# MAGIC 
# MAGIC Here, we selected the notebook from best run from the Auto ML experiment.
# MAGIC 
# MAGIC All the code below has been automatically generated. As Data Scientist, we can tune it based on our business knowledge, or use the model generated as it is.
# MAGIC 
# MAGIC This saves Datascientists hours of developement and allow team to quickly bootstrap and validate new project.
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fml_notebook&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %run ../_resources/02-create-churn-tables $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC # LightGBM training
# MAGIC This is an auto-generated notebook. To reproduce these results, attach this notebook to a DBRML cluster and rerun it.
# MAGIC - Compare trials in the MLflow experiment
# MAGIC - Navigate to the parent notebook
# MAGIC - Clone this notebook into your project folder by selecting **File > Clone** in the notebook toolbar.
# MAGIC 
# MAGIC Runtime Version: _8.4.x-cpu-ml-scala2.12_

# COMMAND ----------

import mlflow
#Added for the demo purpose
run = get_automl_churn_run(force_refresh = False)
# Use MLflow to track experiments
mlflow.set_experiment(run["experiment_path"])

target_col = "churn"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd

from mlflow.tracking import MlflowClient

# Create temp directory to download input data from MLflow
input_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], str(uuid.uuid4())[:8])
os.makedirs(input_temp_dir)

# Download the artifact and read it into a pandas DataFrame
input_client = MlflowClient()
input_data_path = input_client.download_artifacts(run["data_run_id"], "data", input_temp_dir)
df_loaded = pd.read_parquet(os.path.join(input_data_path, "training_data"))

# Delete the temp data
shutil.rmtree(input_temp_dir)

# Preview data
df_loaded.head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select supported columns
# MAGIC Select only the columns that are supported. This allows us to train a model that can predict on a dataset that has extra columns that are not used in training.
# MAGIC `["customer_id"]` are dropped in the pipelines. See the Alerts tab of the AutoML Experiment page for details on why these columns are dropped.

# COMMAND ----------

from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
supported_cols = ["event_count", "gender", "total_amount", "order_count", "total_item", "days_since_last_activity", "canal", "days_last_event", "days_since_creation", "session_count", "age_group", "country"]
col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical columns
# MAGIC 
# MAGIC Missing values for numerical columns are imputed with mean by default.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["age_group", "days_last_event", "days_since_creation", "days_since_last_activity", "event_count", "gender", "order_count", "session_count", "total_amount", "total_item"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce"))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["event_count", "gender", "total_amount", "order_count", "total_item", "days_since_last_activity", "days_last_event", "days_since_creation", "session_count", "age_group"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low-cardinality categoricals
# MAGIC Convert each low-cardinality categorical column into multiple binary columns through one-hot encoding.
# MAGIC For each input categorical column (string or numeric), the number of output columns is equal to the number of unique values in the input column.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

one_hot_imputers = []

one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="ignore")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["age_group", "canal", "country", "event_count", "gender", "order_count", "session_count"])]

# COMMAND ----------

from sklearn.compose import ColumnTransformer

transformers = numerical_transformers + categorical_one_hot_transformers

preprocessor = ColumnTransformer(transformers, remainder="passthrough", sparse_threshold=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC The input data is split by AutoML into 3 sets:
# MAGIC - Train (60% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (20% of the dataset used to report the true performance of the model on an unseen dataset)
# MAGIC 
# MAGIC `_automl_split_col_8425` contains the information of which set a given row belongs to.
# MAGIC We use this column to split the dataset into the above 3 sets. 
# MAGIC The column should not be used for training so it is dropped after split is done.

# COMMAND ----------

# AutoML completed train - validation - test split internally and used _automl_split_col_xxxx to specify the set
split_col = [c for c in df_loaded.columns if c.startswith('_automl_split_col')][0]

split_train_df = df_loaded.loc[df_loaded[split_col] == "train"]
split_val_df = df_loaded.loc[df_loaded[split_col] == "val"]
split_test_df = df_loaded.loc[df_loaded[split_col] == "test"]

# Separate target column from features and drop _automl_split_col_xxx
X_train = split_train_df.drop([target_col, split_col], axis=1)
y_train = split_train_df[target_col]

X_val = split_val_df.drop([target_col, split_col], axis=1)
y_val = split_val_df[target_col]

X_test = split_test_df.drop([target_col, split_col], axis=1)
y_test = split_test_df[target_col]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train classification model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/3087977229142441/s?orderByKey=metrics.%60val_f1_score%60&orderByAsc=false)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

import lightgbm
from lightgbm import LGBMClassifier

help(LGBMClassifier)

# COMMAND ----------

import mlflow
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline

set_config(display="diagram")

lgbmc_classifier = LGBMClassifier(
  colsample_bytree=0.6598667807968007,
  lambda_l1=1.4454111570314836,
  lambda_l2=1.1010300499737529,
  learning_rate=0.5722591255109539,
  max_bin=199,
  max_depth=2,
  min_child_samples=192,
  n_estimators=5,
  num_leaves=6,
  path_smooth=58.78241158366447,
  subsample=0.6868912280002921,
  random_state=949709769,
)

model = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
    ("classifier", lgbmc_classifier),
])

# Create a separate pipeline to transform the validation dataset. This is used for early stopping.
mlflow.sklearn.autolog(disable=True)
pipeline_val = Pipeline([
    ("column_selector", col_selector),
    ("preprocessor", preprocessor),
])
pipeline_val.fit(X_train, y_train)
X_val_processed = pipeline_val.transform(X_val)

model

# COMMAND ----------

# Enable automatic logging of input samples, metrics, parameters, and models
mlflow.sklearn.autolog(log_input_examples=True, silent=True)
with mlflow.start_run(experiment_id=run['experiment_id'], run_name="lightgbm") as mlflow_run:
    model.fit(X_train, y_train, classifier__callbacks=[lightgbm.early_stopping(5), lightgbm.log_evaluation(0)], classifier__eval_set=[(X_val_processed,y_val)])
    
    # Log metrics for the training set
    lgbmc_training_metrics = mlflow.sklearn.eval_and_log_metrics(model, X_train, y_train, prefix="training_", pos_label=1)

    # Log metrics for the validation set
    lgbmc_val_metrics = mlflow.sklearn.eval_and_log_metrics(model, X_val, y_val, prefix="val_", pos_label=1)

    # Log metrics for the test set
    lgbmc_test_metrics = mlflow.sklearn.eval_and_log_metrics(model, X_test, y_test, prefix="test_", pos_label=1)

    # Display the logged metrics
    lgbmc_val_metrics = {k.replace("val_", ""): v for k, v in lgbmc_val_metrics.items()}
    lgbmc_test_metrics = {k.replace("test_", ""): v for k, v in lgbmc_test_metrics.items()}
    display(pd.DataFrame([lgbmc_val_metrics, lgbmc_test_metrics], index=["validation", "test"]))
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance
# MAGIC 
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation, so to ensure that AutoML can run trials without
# MAGIC   running out of memory, we disable SHAP by default.<br />
# MAGIC   You can set the flag defined below to `shap_enabled = True` and re-run this notebook to see the SHAP plots.
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the validation set to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC - SHAP cannot explain models using data with nulls; if your dataset has any, both the background data and
# MAGIC   examples to explain will be imputed using the mode (most frequent values). This affects the computed
# MAGIC   SHAP values, as the imputed samples may not match the actual data distribution.
# MAGIC 
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).

# COMMAND ----------

# Set this flag to True and re-run the notebook to see the SHAP plots
shap_enabled = dbutils.widgets.get("shap_enabled") == "true"

# COMMAND ----------

if shap_enabled:
    mlflow.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)
    from shap import KernelExplainer, summary_plot
    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train.sample(n=min(100, X_train.shape[0]), random_state=949709769)

    # Sample some rows from the validation set to explain. Increase the sample size for more thorough results.
    example = X_val.sample(n=min(100, X_val.shape[0]), random_state=949709769)

    # Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
    predict = lambda x: model.predict(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=500)
    summary_plot(shap_values, example, class_names=model.classes_)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confusion matrix, ROC and Precision-Recall curves for validation data
# MAGIC 
# MAGIC We show the confusion matrix, ROC and Precision-Recall curves of the model on the validation data.
# MAGIC 
# MAGIC For the plots evaluated on the training and the test data, check the artifacts on the MLflow run page.

# COMMAND ----------

# Paste the entire output (%md ...) to an empty cell, and click the link to see the MLflow run page
displayHTML(f"""<a href="#mlflow/experiments/{ run['experiment_id'] }/runs/{ mlflow_run.info.run_id }/artifactPath/model">Link to model run page</a>""")

# COMMAND ----------

import uuid
from IPython.display import Image

# Create temp directory to download MLflow model artifact
eval_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(eval_temp_dir, exist_ok=True)

# Download the artifact
eval_path = mlflow.artifacts.download_artifacts(run_id=mlflow_run.info.run_id, dst_path=eval_temp_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Confusion matrix for validation dataset

# COMMAND ----------

eval_confusion_matrix_path = os.path.join(eval_path, "val_confusion_matrix.png")
display(Image(filename=eval_confusion_matrix_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROC curve for validation dataset

# COMMAND ----------

eval_roc_curve_path = os.path.join(eval_path, "val_roc_curve.png")
display(Image(filename=eval_roc_curve_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Precision-Recall curve for validation dataset

# COMMAND ----------

eval_pr_curve_path = os.path.join(eval_path, "val_precision_recall_curve.png")
display(Image(filename=eval_pr_curve_path))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### MLFlow tracked all our model information and the model is ready to be deployed in our registry!
# MAGIC We can do that manually:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/mlflow_artifact.gif" alt="MLFlow artifacts"/>
# MAGIC 
# MAGIC or using MLFlow APIs directly:

# COMMAND ----------

# DBTITLE 1,Let's register a first model version as example
model_name = "dbdemos_customer_churn"
model_registered = mlflow.register_model(f"runs:/{ mlflow_run.info.run_id }/model", model_name)

#Move the model in production
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(model_name, model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md
# MAGIC We just moved our model as production ready! [Open the dbdemos_customer_churn model](#mlflow/models/dbdemos_customer_churn) to explore its artifact and analyze the parameters used, including traceability to the notebook used for its creation.

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Next: Deploying our model in production: batch or serverless inference 
# MAGIC 
# MAGIC Let's now see how we can deploy this model and run inferences in production to provide Churn insight for our business
# MAGIC 
# MAGIC Next: [04.3-running-inference]($./04.3-running-inference)
