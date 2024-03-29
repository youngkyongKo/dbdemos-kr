-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Databricks를 사용한 데이터 엔지니어링 - C360 데이터베이스 구축
-- MAGIC 
-- MAGIC C360 데이터베이스를 구축하려면 여러 데이터 소스를 수집해야 합니다.
-- MAGIC 
-- MAGIC 개인화 및 마케팅 타겟팅에 사용되는 실시간 인사이트를 지원하기 위해 배치 로드 및 스트리밍 수집이 필요한 복잡한 프로세스입니다.
-- MAGIC 
-- MAGIC 다운스트림 사용자(데이터 분석가 및 데이터 과학자)를 위해 정형화된 SQL 테이블을 생성하기 위해 데이터를 수집, 변환 및 정리하는 것은 복잡합니다.
-- MAGIC 
-- MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
-- MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
-- MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
-- MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
-- MAGIC       73%
-- MAGIC     </div>
-- MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">엔터프라이즈 데이터는 분석 및 의사 결정에 사용되지 않습니다.</div>
-- MAGIC   </div>
-- MAGIC   <div style="color: #bfbfbf; padding-top: 5px">출처: Forrester</div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC <br>
-- MAGIC 
-- MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John은 데이터 엔지니어로서 엄청난 시간을 보냅니다…
-- MAGIC 
-- MAGIC 
-- MAGIC * 데이터 수집 및 변환 수동 코딩 및 기술 문제 처리:<br>
-- MAGIC   *스트리밍 및 배치 지원, 동시 작업 처리, 작은 파일 읽기 문제, GDPR 요구 사항, 복잡한 DAG 종속성...*<br><br>
-- MAGIC * 품질 및 테스트 시행을 위한 맞춤형 프레임워크 구축<br><br>
-- MAGIC * 관찰 및 모니터링 기능을 갖춘 확장 가능한 인프라 구축 및 유지<br><br>
-- MAGIC * 다른 시스템에서 호환되지 않는 거버넌스 모델 관리
-- MAGIC <br style="clear: both">
-- MAGIC 
-- MAGIC 이로 인해 **운영 복잡성**과 오버헤드가 발생하여 별도의 전문가를 필요로하고 궁극적으로 **데이터 프로젝트를 위험에 빠뜨립니다**.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # 델타 라이브 테이블(DLT)로 데이터 수집과 변환 과정을 간소화
-- MAGIC 
-- MAGIC <img style="float: right" width="300px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-2.png" />
-- MAGIC 
-- MAGIC 이 노트북에서 우리는 데이터 엔지니어가 되어 c360 데이터베이스를 구축할 것입니다. <br>
-- MAGIC BI 및 ML 워크로드에 필요한 테이블을 준비하기 위해 원시 데이터 소스를 사용하고 정리합니다.
-- MAGIC 
-- MAGIC Blob 스토리지(`/demos/retail/churn/`)에 새로운 파일을 보내는 3개의 데이터 소스가 있으며 이 데이터를 Datawarehousing 테이블에 점진적으로 로드하려고 합니다.
-- MAGIC 
-- MAGIC - 고객 프로필 데이터 *(이름, 나이, 주소 등)*
-- MAGIC - 주문 내역 *(시간이 지남에 따라 고객이 구입하는 것)*
-- MAGIC - 애플리케이션의 스트리밍 이벤트 *(고객이 애플리케이션을 마지막으로 사용한 시간, 일반적으로 Kafka 대기열의 스트림)*
-- MAGIC 
-- MAGIC Databricks는 모두가 데이터 엔지니어링에 액세스할 수 있도록 DLT(Delta Live Table)를 사용하여 이 작업을 단순화합니다.
-- MAGIC 
-- MAGIC DLT를 사용하면 데이터 분석가가 일반 SQL로 고급 파이프라인을 만들 수 있습니다.
-- MAGIC 
-- MAGIC ## Delta Live Table: 최신 고품질 데이터를 위한 데이터 파이프라인을 구축하고 관리하는 간단한 방법!
-- MAGIC 
-- MAGIC <div>
-- MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
-- MAGIC       <strong>ETL 개발 가속화</strong> <br/>
-- MAGIC       분석가와 데이터 엔지니어가 간단한 파이프라인 개발 및 유지 관리를 통해 빠르게 혁신할 수 있도록 지원 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
-- MAGIC       <strong>운영 복잡성 제거</strong> <br/>
-- MAGIC       복잡한 관리 작업을 자동화하고 파이프라인 작업에 대한 더 넓은 가시성을 확보함
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC   <div style="width: 48%; float: left">
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
-- MAGIC       <strong>데이터에 대한 신뢰도 확장</strong> <br/>
-- MAGIC       내장된 품질 제어 및 품질 모니터링을 통해 정확하고 유용한 BI, 데이터 과학 및 ML을 보장합니다 
-- MAGIC     </p>
-- MAGIC     <p>
-- MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
-- MAGIC       <strong>배치 및 스트리밍 간소화</strong> <br/>
-- MAGIC       일괄 처리 또는 스트리밍 처리를 위한 자체 최적화 및 자동 확장 데이터 파이프라인 포함 
-- MAGIC     </p>
-- MAGIC   </div>
-- MAGIC </div>
-- MAGIC 
-- MAGIC <br style="clear:both">
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
-- MAGIC 
-- MAGIC ## Delta Lake
-- MAGIC 
-- MAGIC Lakehouse에서 생성할 모든 테이블은 Delta Lake 테이블로 저장됩니다. Delta Lake는 안정성과 성능을 위한 개방형 스토리지 프레임워크이며 많은 기능을 제공합니다(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## 고객 이탈에 대한 분석 및 감소를 위한 델타 라이브 테이블 파이프라인 구축
-- MAGIC 
-- MAGIC 이 예에서는 고객 정보를 consuming 하는 엔드 투 엔드 DLT 파이프라인을 구현합니다. medaillon 아키텍처를 사용하지만 스타 스키마, 데이터 저장소 또는 기타 모델링을 구축할 수 있습니다.
-- MAGIC 
-- MAGIC AutoLoader로 새로운 데이터를 점진적으로 로드하고 이 정보를 보강한 다음 MLFlow에서 모델을 로드하여 고객 이탈 예측을 수행합니다.
-- MAGIC 
-- MAGIC 그런 다음 이 정보는 DBSQL 대시보드를 구축하여 고객 행동 및 이탈을 추적하는 데 사용됩니다.
-- MAGIC 
-- MAGIC 다음 흐름을 구현해 보겠습니다.
-- MAGIC  
-- MAGIC <div><img width="1100px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de.png"/></div>
-- MAGIC 
-- MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-churn-prediction) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Your DLT Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/4704e053-1b43-4eb9-9024-908dd52735f3" target="_blank">Churn Delta Live Table pipeline</a> to see it in action.<br/>
-- MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

-- COMMAND ----------

-- DBTITLE 1,수신되는 원시 데이터인 사용자 데이터(json)
-- MAGIC %python
-- MAGIC display(spark.read.json('/demos/retail/churn/users'))

-- COMMAND ----------

-- cf. 아래와 같이 직접 SQL로 조회할 수도 있습니다. 
-- SELECT * FROM json.`/demos/retail/churn/users`

-- COMMAND ----------

-- DBTITLE 1,수신되는 원시 데이터인 주문 데이터 (json)
-- MAGIC %python
-- MAGIC display(spark.read.json('/demos/retail/churn/orders'))

-- COMMAND ----------

-- DBTITLE 1,수신되는 원시 데이터인 클릭 스트림 데이터(csv)
-- MAGIC %python
-- MAGIC display(spark.read.csv('/demos/retail/churn/events', header=True))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 1/ Databricks Autoloader(cloud_files)를 사용하여 데이터 로드
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC 오토로더를 사용하면 클라우드 스토리지에서 수백만 개의 파일을 효율적으로 수집하고 대규모로 효율적인 스키마 추론(Inference) 및 진화(Evolution)를 지원할 수 있습니다.
-- MAGIC 
-- MAGIC 이를 파이프라인에 사용하고 blob 스토리지 `/demos/retail/churn/...`에서 전달되는 원시 JSON 및 CSV 데이터를 수집해 보겠습니다.

-- COMMAND ----------

-- DBTITLE 1,증분 모드로 앱 이벤트 스트림 수집
CREATE STREAMING LIVE TABLE churn_app_events (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "앱 이벤트 원본 데이터"
AS SELECT * FROM cloud_files("/demos/retail/churn/events", "csv", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 여기서 LIVE TABLE(VIEW)는 DB의 Materialized View 처럼, 테이블을 정의하는 쿼리가 바뀌거나 입력 데이터 원본이 업데이트되면 그에 따라 변경되는 테이블입니다. 
-- MAGIC <br/>
-- MAGIC 
-- MAGIC * LIVE TABLE: Underlying 테이블이 바뀌면  변경된 내용을 반영하여 전체 업데이트 됩니다.
-- MAGIC 
-- MAGIC * STREAMING LIVE TABLE: 마지막 파이프라인 업데이트 이후에 새로 추가된 데이터만 증분으로 처리되며, 기존 데이터는 재계산 되지 않습니다. 

-- COMMAND ----------

-- DBTITLE 1,ERP 에서 주문내역 수집
CREATE STREAMING LIVE TABLE churn_orders_bronze (
  CONSTRAINT orders_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "ERP 주문 내역 원본 데이터"
AS SELECT * FROM cloud_files("/demos/retail/churn/orders", "json")

-- COMMAND ----------

-- DBTITLE 1,사용자 데이터 수집
CREATE STREAMING LIVE TABLE churn_users_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "사용자 원본 데이터. 스키마 추론하여 반영"
AS SELECT * FROM cloud_files("/demos/retail/churn/users", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ 데이터 분석가를 위한 품질 강화 및 테이블 구체화
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 종종 실버라고 부르는 다음 레이어는 브론즈 레이어에서 **증분** 데이터를 소비하고 일부 정보를 정리합니다.
-- MAGIC 
-- MAGIC 또한 데이터 품질을 적용하고 추적하기 위해 다른 필드에 [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html)를 추가하고 있습니다. 이렇게 하면 대시보드가 관련성이 있고 데이터 이상으로 인한 잠재적 오류를 쉽게 발견할 수 있습니다.
-- MAGIC 
-- MAGIC 이 테이블은 깨끗하고 BI 팀에서 사용할 준비가 되어 있습니다!

-- COMMAND ----------

-- DBTITLE 1,사용자 데이터 정리 및 익명화
CREATE STREAMING LIVE TABLE churn_users (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "id")
COMMENT "분석을 위해 정리하고 익명화된 사용자 데이터"
AS SELECT
  id as user_id,
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  initcap(firstname) as firstname, 
  initcap(lastname) as lastname, 
  address, 
  canal, 
  country,
  cast(gender as int),
  cast(age_group as int), 
  cast(churn as int) as churn
from STREAM(live.churn_users_bronze)

-- COMMAND ----------

-- DBTITLE 1,주문 데이터 정리
CREATE STREAMING LIVE TABLE churn_orders (
  CONSTRAINT order_valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW, 
  CONSTRAINT order_valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "분석을 위해 정리되고 익명화된 주문 데이터"
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date
from STREAM(live.churn_orders_bronze)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ 데이터를 집계하고 조인하여 ML 기능 생성
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 이제 Churn 예측에 필요한 기능을 만들 준비가 되었습니다.
-- MAGIC 
-- MAGIC 모델이 다음과 같이 이탈을 예측하는 데 사용할 추가 정보로 사용자 데이터 세트를 보강해야 합니다.
-- MAGIC 
-- MAGIC * 마지막 접속 날짜
-- MAGIC * 구매한 아이템 수
-- MAGIC * 웹 사이트의 작업 수
-- MAGIC * 사용한 기기(ios/iphone)
-- MAGIC * ...

-- COMMAND ----------

CREATE LIVE TABLE churn_features
COMMENT "분석 및 ML을 위한 모든 정보를 포함한 최종 사용자 테이블"
AS 
  WITH 
    churn_orders_stats AS (
      SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM live.churn_orders GROUP BY user_id),  
    churn_app_events_stats as (
      SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
      FROM live.churn_app_events GROUP BY user_id)
      
  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity,
         datediff(now(), last_event) as days_last_event
  FROM live.churn_users
  INNER JOIN churn_orders_stats using (user_id)
  INNER JOIN churn_app_events_stats using (user_id)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ ML 모델로 Gold 데이터 강화
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-4.png"/>
-- MAGIC </div>
-- MAGIC 
-- MAGIC 데이터 과학자 팀은 Auto ML을 사용하여 이탈 예측 모델을 구축하고 Databricks 모델 레지스트리에 저장했습니다.
-- MAGIC 
-- MAGIC Lakehouse의 핵심 가치 중 하나는 이 모델을 쉽게 로드하고 이탈을 파이프라인으로 바로 예측할 수 있다는 것입니다.
-- MAGIC 
-- MAGIC 모델 프레임워크(sklearn 또는 기타)에 대해 걱정할 필요가 없으며 MLFlow가 이를 추상화합니다.

-- COMMAND ----------

-- DBTITLE 1,학습 모델을 SQL 함수로 로드
-- MAGIC %python
-- MAGIC import mlflow
-- MAGIC #                                                                              Stage/version    output
-- MAGIC #                                                                 Model name         |            |
-- MAGIC #                                                                     |              |            |
-- MAGIC predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/dbdemos_customer_churn/Production", "int")
-- MAGIC spark.udf.register("predict_churn", predict_churn_udf)

-- COMMAND ----------

-- DBTITLE 1,모델을 호출하고 파이프라인에서 이탈을 예측
CREATE LIVE TABLE churn_prediction 
COMMENT "이탈 위험이 있는 고객"
AS 
SELECT predict_churn(struct(user_id, age_group, canal, country, gender, order_count, total_amount, total_item, platform, event_count, session_count, days_since_creation, days_since_last_activity, days_last_event)) as churn_prediction, * 
FROM live.churn_features

-- COMMAND ----------

-- MAGIC %md ## 이제 파이프라인이 준비되었습니다!
-- MAGIC 
-- MAGIC 보시다시피 databricks로 Data Pipeline을 구축하면 엔진이 모든 어려운 데이터 엔지니어링 작업을 해결하는 동안 비즈니스 구현에 집중할 수 있습니다.
-- MAGIC 
-- MAGIC <!-- <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/4704e053-1b43-4eb9-9024-908dd52735f3" target="_blank">--> 
-- MAGIC <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/d3a24e91-91e7-48e9-9f0a-345554fd13b4/updates/5275dd53-6b5a-4604-88d4-43f20bc85c5d" target="_blank">Churn 델타 라이브 테이블 파이프라인</a>을 열고 클릭 데이터 리니지를 시각화하고 새로운 데이터를 점진적으로 소비하기 시작합니다!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## (Optional) Delta Live Tables로 데이터 품질 지표 확인
-- MAGIC Delta Live Tables는 모든 데이터 품질 지표를 추적합니다. Databricks SQL을 사용하여 기대를 SQL 테이블로 직접 활용하여 기대 메트릭을 추적하고 필요에 따라 경고를 보낼 수 있습니다. 이렇게 하면 다음 대시보드를 만들 수 있습니다
-- MAGIC 
-- MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
-- MAGIC 
-- MAGIC <a href="/sql/dashboards/6f73dd1b-17b1-49d0-9a11-b3772a2c3357-dlt---retail-data-quality-stats" target="_blank">데이터 ETL 수집 상황 대시보드</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Databricks SQL로 첫 번째 비즈니스 대시보드 구축
-- MAGIC 
-- MAGIC 이제 데이터를 사용할 수 있습니다! 과거 및 현재 비즈니스에서 통찰력을 얻기 위해 대시보드 구축을 시작할 수 있습니다.
-- MAGIC <br>
-- MAGIC <a href="https://e2-demo-field-eng.cloud.databricks.com/sql/dashboards/4a17b407-ee4a-48c2-8881-d58946161983?o=1444828305810485" target="_blank">
-- MAGIC <img style="float: left; margin-right: 50px;" width="500px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png" /></a>
-- MAGIC 
-- MAGIC <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
-- MAGIC 
-- MAGIC <!-- <a href="/sql/dashboards/8b008d69-32ce-4009-8548-4f43152d617d?o=1444828305810485" target="_blank">고객 이탈 예측 대쉬보드</a> -->
-- MAGIC <a href="/sql/dashboards/efdffed1-8e4e-4df1-9e1c-c03ff26373e2?o=1444828305810485" target="_blank">고객 이탈 예측 대쉬보드</a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: Unity Catalog로 데이터 보호 및 공유
-- MAGIC 
-- MAGIC 이제 Lakehouse에서 이러한 테이블을 사용할 수 있으므로 데이터 과학자 및 데이터 분석가 팀과 테이블을 공유할 수 있는 방법을 검토해 보겠습니다.
-- MAGIC 
-- MAGIC [Unity 카탈로그 노트북을 사용한 거버넌스]($../00-churn-introduction-lakehouse) 또는 [소개로 돌아가기]($../00-churn-introduction-lakehouse)로 이동

-- COMMAND ----------


