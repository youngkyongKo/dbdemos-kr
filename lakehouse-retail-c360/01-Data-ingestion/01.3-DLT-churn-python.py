# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC # Databricks를 사용한 데이터 엔지니어링 - C360 데이터베이스 구축
# MAGIC 
# MAGIC C360 데이터베이스를 구축하려면 여러 데이터 소스를 수집해야 합니다.
# MAGIC 
# MAGIC 개인화 및 마케팅 타겟팅에 사용되는 실시간 인사이트를 지원하기 위해 배치 로드 및 스트리밍 수집이 필요한 복잡한 프로세스입니다.
# MAGIC 
# MAGIC 다운스트림 사용자(데이터 분석가 및 데이터 과학자)를 위해 정형화된 SQL 테이블을 생성하기 위해 데이터를 수집, 변환 및 정리하는 것은 복잡합니다.
# MAGIC 
# MAGIC <link href="https://fonts.googleapis.com/css?family=DM Sans" rel="stylesheet"/>
# MAGIC <div style="width:300px; text-align: center; float: right; margin: 30px 60px 10px 10px;  font-family: 'DM Sans'">
# MAGIC   <div style="height: 250px; width: 300px;  display: table-cell; vertical-align: middle; border-radius: 50%; border: 25px solid #fcba33ff;">
# MAGIC     <div style="font-size: 70px;  color: #70c4ab; font-weight: bold">
# MAGIC       73%
# MAGIC     </div>
# MAGIC     <div style="color: #1b5162;padding: 0px 30px 0px 30px;">엔터프라이즈 데이터는 분석 및 의사 결정에 사용되지 않습니다.</div>
# MAGIC   </div>
# MAGIC   <div style="color: #bfbfbf; padding-top: 5px">출처: Forrester</div>
# MAGIC </div>
# MAGIC 
# MAGIC <br>
# MAGIC 
# MAGIC ## <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="float:left; margin: -35px 0px 0px 0px" width="80px"> John은 데이터 엔지니어로서 엄청난 시간을 보냅니다…
# MAGIC 
# MAGIC 
# MAGIC * 데이터 수집 및 변환 수동 코딩 및 기술 문제 처리:<br>
# MAGIC   *스트리밍 및 배치 지원, 동시 작업 처리, 작은 파일 읽기 문제, GDPR 요구 사항, 복잡한 DAG 종속성...*<br><br>
# MAGIC * 품질 및 테스트 시행을 위한 맞춤형 프레임워크 구축<br><br>
# MAGIC * 관찰 및 모니터링 기능을 갖춘 확장 가능한 인프라 구축 및 유지<br><br>
# MAGIC * 다른 시스템에서 호환되지 않는 거버넌스 모델 관리
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC 이로 인해 **운영 복잡성**과 오버헤드가 발생하여 별도의 전문가를 필요로하고 궁극적으로 **데이터 프로젝트를 위험에 빠뜨립니다**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # 델타 라이브 테이블(DLT)로 수집 및 변환 간소화
# MAGIC 
# MAGIC <img style="float: right" width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-1.png" />
# MAGIC 
# MAGIC 이 노트북에서 우리는 데이터 엔지니어로 c360 데이터베이스를 구축할 것입니다. <br>
# MAGIC BI 및 ML 워크로드에 필요한 테이블을 준비하기 위해 원시 데이터 소스를 사용하고 정리합니다.
# MAGIC 
# MAGIC Blob 스토리지(`/demos/retail/churn/`)에 새로운 파일을 보내는 3개의 데이터 소스가 있으며 이 데이터를 Datawarehousing 테이블에 점진적으로 로드하려고 합니다.
# MAGIC 
# MAGIC - 고객 프로필 데이터 *(이름, 나이, 주소 등)*
# MAGIC - 주문 내역 *(시간이 지남에 따라 고객이 구입하는 것)*
# MAGIC - 애플리케이션의 스트리밍 이벤트 *(고객이 애플리케이션을 마지막으로 사용한 시간, 일반적으로 Kafka 대기열의 스트림)*
# MAGIC 
# MAGIC Databricks는 모두가 데이터 엔지니어링에 액세스할 수 있도록 DLT(Delta Live Table)를 사용하여 이 작업을 단순화합니다.
# MAGIC 
# MAGIC DLT를 사용하면 데이터 분석가가 일반 SQL로 고급 파이프라인을 만들 수 있습니다.
# MAGIC 
# MAGIC ## Delta Live Table: 최신 고품질 데이터를 위한 데이터 파이프라인을 구축하고 관리하는 간단한 방법!
# MAGIC 
# MAGIC <div>
# MAGIC   <div style="width: 45%; float: left; margin-bottom: 10px; padding-right: 45px">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-accelerate.png"/> 
# MAGIC       <strong>ETL 개발 가속화</strong> <br/>
# MAGIC       분석가와 데이터 엔지니어가 간단한 파이프라인 개발 및 유지 관리를 통해 빠르게 혁신할 수 있도록 지원 
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-complexity.png"/> 
# MAGIC       <strong>운영 복잡성 제거</strong> <br/>
# MAGIC       복잡한 관리 작업을 자동화하고 파이프라인 작업에 대한 더 넓은 가시성을 확보함
# MAGIC     </p>
# MAGIC   </div>
# MAGIC   <div style="width: 48%; float: left">
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-trust.png"/> 
# MAGIC       <strong>데이터에 대한 신뢰도 확장</strong> <br/>
# MAGIC       내장된 품질 제어 및 품질 모니터링을 통해 정확하고 유용한 BI, 데이터 과학 및 ML을 보장합니다.
# MAGIC     </p>
# MAGIC     <p>
# MAGIC       <img style="width: 50px; float: left; margin: 0px 5px 30px 0px;" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/logo-stream.png"/> 
# MAGIC       <strong>배치 및 스트리밍 간소화</strong> <br/>
# MAGIC       일괄 처리 또는 스트리밍 처리를 위한 자체 최적화 및 자동 확장 데이터 파이프라인 포함
# MAGIC     </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC 
# MAGIC <br style="clear:both">
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right;" width="200px">
# MAGIC 
# MAGIC ## Delta Lake
# MAGIC 
# MAGIC Lakehouse에서 생성할 모든 테이블은 Delta Lake 테이블로 저장됩니다. Delta Lake는 안정성과 성능을 위한 개방형 스토리지 프레임워크이며 많은 기능을 제공합니다(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)<br>

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 고객 이탈에 대한 분석 및 감소를 위한 델타 라이브 테이블 파이프라인 구축
# MAGIC 
# MAGIC 이 예에서는 고객 정보를 consuming 하는 엔드 투 엔드 DLT 파이프라인을 구현합니다. medaillon 아키텍처를 사용하지만 스타 스키마, 데이터 저장소 또는 기타 모델링을 구축할 수 있습니다.
# MAGIC 
# MAGIC AutoLoader로 새로운 데이터를 점진적으로 로드하고 이 정보를 보강한 다음 MLFlow에서 모델을 로드하여 고객 이탈 예측을 수행합니다.
# MAGIC 
# MAGIC 그런 다음 이 정보는 DBSQL 대시보드를 구축하여 고객 행동 및 이탈을 추적하는 데 사용됩니다.
# MAGIC 
# MAGIC 다음 흐름을 구현해 보겠습니다.
# MAGIC  
# MAGIC <div><img width="1100px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-churn-de.png"/></div>
# MAGIC 
# MAGIC *Note that we're including the ML model our [Data Scientist built]($../04-Data-Science-ML/04.1-automl-churn-prediction) using Databricks AutoML to predict the churn. We'll cover that in the next section.*

# COMMAND ----------

# MAGIC %md
# MAGIC Your DLT Pipeline has been installed and started for you! Open the <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/a6ba1d12-74d7-4e2d-b9b7-ca53b655f39d" target="_blank">Churn Delta Live Table pipeline</a> to see it in action.<br/>
# MAGIC *(Note: The pipeline will automatically start once the initialization job is completed, this might take a few minutes... Check installation logs for more details)*

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Databricks Autoloader(cloud_files)를 사용하여 데이터 로드
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-churn-de-small-1.png.png"/>
# MAGIC </div>
# MAGIC   
# MAGIC 오토로더를 사용하면 클라우드 스토리지에서 수백만 개의 파일을 효율적으로 수집하고 대규모로 효율적인 스키마 추론(Inference) 및 진화(Evolution)를 지원할 수 있습니다.
# MAGIC 
# MAGIC 
# MAGIC 이를 파이프라인에 사용하고 blob 스토리지 `/demos/retail/churn/...`에서 전달되는 원시 JSON 및 CSV 데이터를 수집해 보겠습니다.

# COMMAND ----------

# DBTITLE 1,Ingest raw app events stream in incremental mode 
import dlt
from pyspark.sql import functions as F

@dlt.create_table(comment="Application events and sessions")
@dlt.expect("App events correct schema", "_rescued_data IS NULL")
def churn_app_events():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/demos/retail/churn/events"))

# COMMAND ----------

# DBTITLE 1,Ingest raw orders from ERP
@dlt.create_table(comment="Spending score from raw data")
@dlt.expect("Orders correct schema", "_rescued_data IS NULL")
def churn_orders_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/demos/retail/churn/orders"))

# COMMAND ----------

# DBTITLE 1,Ingest raw user data
@dlt.create_table(comment="Raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution")
@dlt.expect("Users correct schema", "_rescued_data IS NULL")
def churn_users_bronze():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("/demos/retail/churn/users"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 2/ 데이터 분석가를 위한 품질 강화 및 테이블 구체화
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-churn-de-small-2.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC 종종 실버라고 부르는 다음 레이어는 브론즈 레이어에서 **증분** 데이터를 소비하고 일부 정보를 정리합니다.
# MAGIC 
# MAGIC 또한 데이터 품질을 적용하고 추적하기 위해 다른 필드에 [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html)를 추가하고 있습니다. 이렇게 하면 대시보드가 관련성이 있고 데이터 이상으로 인한 잠재적 오류를 쉽게 발견할 수 있습니다.
# MAGIC 
# MAGIC 이 테이블은 깨끗하고 BI 팀에서 사용할 준비가 되어 있습니다!

# COMMAND ----------

# DBTITLE 1,Clean and anonymise User data
@dlt.create_table(comment="User data cleaned and anonymized for analysis.")
@dlt.expect_or_drop("user_valid_id", "user_id IS NOT NULL")
def churn_users():
  return (dlt
          .read_stream("churn_users_bronze")
          .select(F.col("id").alias("user_id"),
                  F.sha1(F.col("email")).alias("email"), 
                  F.to_timestamp(F.col("creation_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"), 
                  F.to_timestamp(F.col("last_activity_date"), "MM-dd-yyyy HH:mm:ss").alias("last_activity_date"), 
                  F.initcap(F.col("firstname")).alias("firstname"), 
                  F.initcap(F.col("lastname")).alias("lastname"), 
                  F.col("address"), 
                  F.col("canal"), 
                  F.col("country"),
                  F.col("gender").cast("int").alias("gender"),
                  F.col("age_group").cast("int").alias("age_group"), 
                  F.col("churn").cast("int").alias("churn")))

# COMMAND ----------

# DBTITLE 1,Clean orders
@dlt.create_table(comment="Order data cleaned and anonymized for analysis.")
@dlt.expect_or_drop("order_valid_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("order_valid_user_id", "user_id IS NOT NULL")
def churn_orders():
  return (dlt
          .read_stream("churn_orders_bronze")
          .select(F.col("amount").cast("int").alias("amount"),
                  F.col("id").alias("order_id"),
                  F.col("user_id"),
                  F.col("item_count").cast("int").alias("item_count"),
                  F.to_timestamp(F.col("transaction_date"), "MM-dd-yyyy HH:mm:ss").alias("creation_date"))
         )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ 데이터를 집계하고 조인하여 ML 기능 생성
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-churn-de-small-3.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC 이제 Churn 예측에 필요한 기능을 만들 준비가 되었습니다.
# MAGIC 
# MAGIC 모델이 다음과 같이 이탈을 예측하는 데 사용할 추가 정보로 사용자 데이터 세트를 보강해야 합니다.
# MAGIC 
# MAGIC * 마지막 접속 날짜
# MAGIC * 구매한 아이템 수
# MAGIC * 웹 사이트의 작업 수
# MAGIC * 사용한 기기(ios/iphone)
# MAGIC * ...

# COMMAND ----------

@dlt.create_table(comment="Final user table with all information for Analysis / ML")
def churn_features():
  churn_app_events_stats_df = (dlt
          .read("churn_app_events")
          .groupby("user_id")
          .agg(F.first("platform").alias("platform"),
               F.count('*').alias("event_count"),
               F.count_distinct("session_id").alias("session_count"),
               F.max(F.to_timestamp("date", "MM-dd-yyyy HH:mm:ss")).alias("last_event"))
                              )
  
  churn_orders_stats_df = (dlt
          .read("churn_orders")
          .groupby("user_id")
          .agg(F.count('*').alias("order_count"),
               F.sum("amount").alias("total_amount"),
               F.sum("item_count").alias("total_item"),
               F.max("creation_date").alias("last_transaction"))
         )
  
  return (dlt
          .read("churn_users")
          .join(churn_app_events_stats_df, on="user_id")
          .join(churn_orders_stats_df, on="user_id")
          .withColumn("days_since_creation", F.datediff(F.current_timestamp(), F.col("creation_date")))
          .withColumn("days_since_last_activity", F.datediff(F.current_timestamp(), F.col("last_activity_date")))
          .withColumn("days_last_event", F.datediff(F.current_timestamp(), F.col("last_event")))
         )

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5/ ML 모델로 Gold 데이터 강화
# MAGIC <div style="float:right">
# MAGIC   <img width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-churn-de-small-4.png"/>
# MAGIC </div>
# MAGIC 
# MAGIC 데이터 과학자 팀은 Auto ML을 사용하여 이탈 예측 모델을 구축하고 Databricks 모델 레지스트리에 저장했습니다.
# MAGIC 
# MAGIC Lakehouse의 핵심 가치 중 하나는 이 모델을 쉽게 로드하고 이탈을 파이프라인으로 바로 예측할 수 있다는 것입니다.
# MAGIC 
# MAGIC 모델 프레임워크(sklearn 또는 기타)에 대해 걱정할 필요가 없으며 MLFlow가 이를 추상화합니다.

# COMMAND ----------

# DBTITLE 1,Load the model as SQL function
import mlflow
#                                                                              Stage/version    output
#                                                                 Model name         |            |
#                                                                     |              |            |
predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/dbdemos_customer_churn/Production", "int")
spark.udf.register("predict_churn", predict_churn_udf)

# COMMAND ----------

# DBTITLE 1,Call our model and predict churn in our pipeline
model_features = predict_churn_udf.metadata.get_input_schema().input_names()

@dlt.create_table(comment="Customer at risk of churn")
def churn_prediction():
  return (dlt
          .read('churn_features')
          .withColumn('churn_prediction', predict_churn_udf(*model_features)))

# COMMAND ----------

# MAGIC %md ## 이제 파이프라인이 준비되었습니다!
# MAGIC 
# MAGIC 보시다시피 databricks로 Data Pipeline을 구축하면 엔진이 모든 어려운 데이터 엔지니어링 작업을 해결하는 동안 비즈니스 구현에 집중할 수 있습니다.
# MAGIC 
# MAGIC <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/4704e053-1b43-4eb9-9024-908dd52735f3" target="_blank">변동 델타 라이브 테이블 파이프라인</a>을 열고 클릭 데이터 리니지를 시각화하고 새로운 데이터를 점진적으로 소비하기 시작합니다!

# COMMAND ----------

# MAGIC %md
# MAGIC # Next: secure and share data with Unity Catalog
# MAGIC 
# MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
# MAGIC 
# MAGIC Jump to the [Governance with Unity Catalog notebook]($../00-churn-introduction-lakehouse) or [Go back to the introduction]($../00-churn-introduction-lakehouse)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables로 데이터 품질 지표 확인
# MAGIC Delta Live Tables는 모든 데이터 품질 지표를 추적합니다. Databricks SQL을 사용하여 기대를 SQL 테이블로 직접 활용하여 기대 메트릭을 추적하고 필요에 따라 경고를 보낼 수 있습니다. 이렇게 하면 다음 대시보드를 만들 수 있습니다
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC 
# MAGIC <a href="/sql/dashboards/4a17b407-ee4a-48c2-8881-d58946161983?o=1444828305810485" target="_blank">Data Quality Dashboard</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Live Tables로 데이터 품질 지표 확인
# MAGIC Delta Live Tables는 모든 데이터 품질 지표를 추적합니다. Databricks SQL을 사용하여 기대를 SQL 테이블로 직접 활용하여 기대 메트릭을 추적하고 필요에 따라 경고를 보낼 수 있습니다. 이렇게 하면 다음 대시보드를 만들 수 있습니다
# MAGIC 
# MAGIC <img width="1000" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-data-quality-dashboard.png">
# MAGIC 
# MAGIC <a href="/sql/dashboards/4a17b407-ee4a-48c2-8881-d58946161983?o=1444828305810485" target="_blank">Data Quality Dashboard</a>

# COMMAND ----------


