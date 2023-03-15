# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Churn Prediction - AutoML을 통한 클릭 한번으로 모델 배포 하기
# MAGIC 
# MAGIC 이제 C360 데이터를 활용하여 고객 이탈을 예측하고 설명하는 모델을 구축하는 방법을 살펴보겠습니다.
# MAGIC 
# MAGIC 데이터 과학자로서 우리의 첫 번째 단계는 모델 훈련에 사용할 기능을 분석하고 구축하는 것입니다.
# MAGIC 
# MAGIC 이탈 데이터로 보강된 사용자 테이블은 델타 라이브 테이블 파이프라인 내에 저장되었습니다. 우리가 해야 할 일은 이 정보를 읽고 분석하고 Auto-ML 실행을 시작하는 것뿐입니다.
# MAGIC 
# MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-ml-flow.png" width="800px">
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fautoml&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %run ../_resources/02-create-churn-tables $reset_all_data=false

# COMMAND ----------

# MAGIC %md
# MAGIC ## 데이터 탐색 및 분석
# MAGIC 
# MAGIC 데이터 세트를 검토하고 이탈을 예측해야 하는 데이터 분석을 시작하겠습니다.

# COMMAND ----------

# DBTITLE 1,고객 이탈 Gold 테이블 조회
# Read our churn_features table
churn_dataset = spark.table("demo_ykko.churn_features")
display(churn_dataset)

# COMMAND ----------

# DBTITLE 1,데이터 탐색 및 분석
import seaborn as sns
g = sns.PairGrid(churn_dataset.sample(0.01).toPandas()[['age_group','gender','order_count']], diag_sharey=False)
g.map_lower(sns.kdeplot)
g.map_diag(sns.kdeplot, lw=3)
g.map_upper(sns.regplot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### pandas API를 사용한 추가 데이터 분석 및 준비
# MAGIC 
# MAGIC 데이터 과학자 팀은 Pandas에 익숙하기 때문에 `pandas on spark`를 사용하여 `pandas` 코드를 확장합니다. Pandas 의 명령어는 스파크 엔진에서 변환되어 분산처리됩니다.
# MAGIC 
# MAGIC 일반적으로 데이터 과학 프로젝트에는 더 많은 고급화된 준비가 필요하며 더 복잡한 피쳐 가공을 포함하여 추가 데이터 준비 단계가 필요할 수 있습니다. 이 데모에서는 간단하게 유지하겠습니다.
# MAGIC 
# MAGIC *`spark 3.2`부터는 koalas가 내장되어 있으며 `pandas_api()`를 사용하여 Pandas Dataframe을 가져올 수 있습니다.*

# COMMAND ----------

# DBTITLE 1,전체 데이터 세트 위에  pandas 변환/코드
# Convert to koalas
dataset = churn_dataset.pandas_api()
dataset.describe()  
# Drop columns we don't want to use in our model
dataset = dataset.drop(columns=['address', 'email', 'firstname', 'lastname', 'creation_date', 'last_activity_date', 'last_event'])
# Drop missing valuesb
dataset = dataset.dropna()   

# COMMAND ----------

dataset.head(10)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## Feature Store 에 저장
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-feature-store.png" style="float:right" width="500" />
# MAGIC 
# MAGIC Feature가 준비되면 Databricks Feature Store에 저장합니다. Feature Store는 Delta Lake 테이블로 지원됩니다.
# MAGIC 
# MAGIC 이를 통해 조직 전체에서 Feature를 검색하고 재사용할 수 있어 팀 효율성이 향상됩니다.
# MAGIC 
# MAGIC Feature Store는 어떤 모델이 어떤 Feature 집합에 종속되는지 파악하여 배포 시 추적성과 거버넌스를 제공합니다. 또한 실시간 제공을 단순화합니다.

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
database = "demo_ykko"
fs = FeatureStoreClient()

try:
  #drop table if exists
  fs.drop_table(f'{database}.churn_user_features')
except:
  pass
#Note: You might need to delete the FS table using the UI
churn_feature_table = fs.create_table(
  name=f'{database}.churn_user_features',
  primary_keys='user_id',
  schema=dataset.spark.schema(),
  description='해당 Feature는 레이크하우스의 churn_bronze_customers 테이블에서 파생됩니다. Categorical 컬럼에 대한 더미 변수를 만들고 이름을 삭제하고 고객 이탈 여부에 대한 Boolean Flag를 추가했습니다. 집계는 수행되지 않았습니다.'
)

fs.write_table(df=dataset.to_spark(), name=f'{database}.churn_user_features', mode='overwrite')
features = fs.read_table(f'{database}.churn_user_features')
display(features)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## MLFlow 및 Databricks Auto-ML을 사용하여 Churn 모델 생성 가속화
# MAGIC 
# MAGIC MLflow는 모델 추적, 패키징 및 배포를 허용하는 오픈 소스 프로젝트입니다. 데이터 과학자 팀이 모델에 대해 작업할 때마다 Databricks는 사용된 모든 매개 변수와 데이터를 추적하고 저장합니다. 이를 통해 ML 추적성과 재현성을 보장하여 어떤 매개변수/데이터를 사용하여 어떤 모델이 빌드되었는지 쉽게 알 수 있습니다.
# MAGIC 
# MAGIC ### Glassbox 솔루션
# MAGIC 
# MAGIC Databricks는 MLFlow를 통해 MLOps(모델 배포 및 거버넌스)를 단순화하지만 새로운 ML 프로젝트를 시작하는것은 여전히 시간이 소요되고 비효율적일 수 있습니다.
# MAGIC 
# MAGIC 각각의 새 프로젝트에 대해 동일한 boilerplate code를 만드는 대신 Databricks Auto-ML은 분류, 회귀 및 예측을 위한 최신 모델을 자동으로 생성할 수 있습니다
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/auto-ml-full.png"/>
# MAGIC 
# MAGIC 
# MAGIC 모델을 직접 배포하거나 대신 생성된 노트북을 활용하여 모범 사례로 프로젝트를 시작하여 몇 주 동안의 노력을 절약할 수 있습니다.
# MAGIC 
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC <img style="float: right" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-auto-ml.png"/>
# MAGIC 
# MAGIC ### Churn 데이터 세트와 함께 Databricks Auto ML 사용
# MAGIC 
# MAGIC 자동 ML은 "머신 러닝" 공간에서 사용할 수 있습니다. 새로운 Auto-ML 실험을 시작하고 방금 생성한 기능 테이블(`churn_features`)을 선택하기만 하면 됩니다.
# MAGIC 
# MAGIC 우리의 예측 대상은 `churn` 컬럼입니다.
# MAGIC 
# MAGIC 시작을 클릭하면 Databricks가 나머지 작업을 수행합니다.
# MAGIC 
# MAGIC UI 로도 가능하지만, [python API](https://docs.databricks.com/applications/machine-learning/automl.html#automl-python-api-1) 를 활용해서 노트북 내에서 수행 가능합니다.

# COMMAND ----------

# DBTITLE 1,이미 학습된 AutoML 모델을 여기에서 탐색할 수 있습니다.
# MAGIC %md
# MAGIC AutoML은 MLFlow 레지스트리에 최상의 모델을 저장했습니다. [dbdemos_customer_churn 모델 열기](#/Users/dongwook.kim@databricks.com/databricks_automl/dongwook_churn_user_features-2023_02_22)를 통해 아티팩트를 탐색하고 생성에 사용된 노트북에 대한 추적 가능성을 포함하여 사용된 매개변수를 분석합니다.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### AutoML에서 생성된 모델은 DLT 파이프라인에서 이탈하려는 고객을 감지하는 데 사용할 준비가 되었습니다.
# MAGIC 
# MAGIC 이제 데이터 엔지니어가 Auto ML 실행에서 모델 `dongwook_demos_customer_churn`을 쉽게 검색하고 델타 라이브 테이블 파이프라인 내에서 변동을 예측할 수 있습니다.<br>
# MAGIC DLT 파이프라인을 다시 열어 이것이 어떻게 수행되는지 확인합니다.
# MAGIC 
# MAGIC #### 다음 달 이탈 영향 및 캠페인 영향 추적
# MAGIC 
# MAGIC 이 이탈 예측은 대시보드에서 재사용하여 향후 이탈을 분석하고 조치를 취하고 이탈 감소를 측정할 수 있습니다.
# MAGIC 
# MAGIC Lakehouse로 생성된 파이프라인은 강력한 ROI를 제공할 것입니다. 이 파이프라인을 설정하는 데 몇 시간이 걸렸고 월 $129,914의 잠재적인 이익을 얻었습니다!
# MAGIC 
# MAGIC <img width="800px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
# MAGIC 
# MAGIC <a href='/sql/dashboards/8b008d69-32ce-4009-8548-4f43152d617d?o=1444828305810485'>Open the Churn prediction DBSQL dashboard</a> | [Go back to the introduction]($../00-churn-introduction-lakehouse)
# MAGIC 
# MAGIC #### More advanced model deployment (batch or serverless realtime)
# MAGIC 
# MAGIC 또한 `dbdemos_custom_churn` 모델을 사용하고 독립 실행형 배치 또는 실시간 추론에서 예측을 실행할 수 있습니다!
# MAGIC 
# MAGIC Next step:  [Explore the generated Auto-ML notebook]($./04.2-automl-generated-notebook) and [Run inferences in production]($./04.3-running-inference)

# COMMAND ----------


