# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Data Science with Databricks
# MAGIC 
# MAGIC ## ML 은 개인화의 핵심
# MAGIC 
# MAGIC C360 데이터베이스를 수집하고 쿼리할 수 있는 것이 첫 번째 단계이지만 경쟁이 치열한 시장에서 경쟁우위를 하기에는 충분하지 않습니다.
# MAGIC 
# MAGIC 이제 고객은 실시간 개인화와 새로운 형태의 커뮤니케이션을 기대합니다. 현재적인 데이터 회사는 AI를 통해 이를 달성합니다.
# MAGIC 
# MAGIC <style>
# MAGIC .right_box{
# MAGIC   margin: 30px; box-shadow: 10px -10px #CCC; width:600px;height:250px; background-color: #1b3139ff; box-shadow:  0 0 10px  rgba(0,0,0,0.6);
# MAGIC   border-radius:25px;font-size: 30px; float: left; padding: 20px; color: #f9f7f4; }
# MAGIC .badge {
# MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
# MAGIC .badge_b { 
# MAGIC   height: 35px}
# MAGIC </style>
# MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
# MAGIC <div style="font-family: 'DM Sans'">
# MAGIC   <div style="width: 700px; color: #1b3139; margin-left: 50px; float: left">
# MAGIC     <div style="color: #ff5f46; font-size:60px">90%</div>
# MAGIC     <div style="font-size:20px;  margin-top: -20px; line-height: 30px;">
# MAGIC       엔터프라이즈 애플리케이션은 2025년까지 AI로 강화될 예정 —IDC
# MAGIC     </div>
# MAGIC     <div style="color: #ff5f46; font-size:60px">$10T+</div>
# MAGIC     <div style="font-size:20px;  margin-top: -20px; line-height: 30px;">
# MAGIC        2030년 AI가 창출할 것으로 예상되는 비즈니스 가치 —PWC
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC   <div class="right_box">
# MAGIC       그러나 ML을 대규모로 작동시키는데는 <br/>큰 어려움이 있습니다!<br/><br/>
# MAGIC       대부분의 ML 프로젝트는 <br/>프로덕션에 들어가기 전에 여전히 실패합니다.
# MAGIC   </div>
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC ## Machine learning is data + transforms.
# MAGIC 
# MAGIC 비즈니스 라인에 가치를 제공하는 것은 모델 구축에 관한 것만이 아니기 때문에 ML은 어렵습니다. <br>
# MAGIC ML 수명 주기는 데이터 파이프라인으로 구성됩니다. 데이터 사전 처리, 피쳐 엔지니어링, 학습, 추론, 모니터링, 재학습...<br>
# MAGIC 모든 파이프라인은 데이터 + 코드입니다.
# MAGIC 
# MAGIC <img style="float: right; margin-top: 10px" width="700px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-4.png" />
# MAGIC 
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="float: left;" width="80px"> 
# MAGIC <h3 style="padding: 10px 0px 0px 5px">Marc는 데이터 과학자로서 <br/>모든 ML 및 DS 단계를 가속화하는 데이터 + ML 플랫폼이 필요합니다:</h3>
# MAGIC 
# MAGIC <div style="font-size: 19px; margin-left: 73px; clear: left">
# MAGIC <div class="badge_b"><div class="badge">1</div> 실시간 처리를 지원하는 데이터 파이프라인 구축(with DTL)</div>
# MAGIC <div class="badge_b"><div class="badge">2</div> 데이터 탐색</div>
# MAGIC <div class="badge_b"><div class="badge">3</div> 피쳐 생성</div>
# MAGIC <div class="badge_b"><div class="badge">4</div> 빌드 & 모델 학습</div>
# MAGIC <div class="badge_b"><div class="badge">5</div> 모델 배포(배치 또는 실시간)</div>
# MAGIC <div class="badge_b"><div class="badge">6</div> 모니터링</div>
# MAGIC </div>
# MAGIC 
# MAGIC **Marc는 데이터 레이크 하우스가 필요합니다**.  Lakehouse 내 프로덕션에서 Churn 모델을 배포하는 방법을 살펴보겠습니다.

# COMMAND ----------

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
# MAGIC <img  src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-churn-ds-flow.png" width="1000px">

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
churn_dataset = spark.table("dongwook_demos.churn_features")
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
database = "dongwook_demos"
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


