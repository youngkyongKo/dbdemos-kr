-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # Lakehouse for retail - Customer 360 플랫폼으로 고객 이탈 감소
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn.png" style="float: left; margin-right: 30px" width="600px" />
-- MAGIC 
-- MAGIC <br/>
-- MAGIC <div style="padding-left: 630px">
-- MAGIC   
-- MAGIC ## Retail 에서의 Databricks Lakehouse는 무엇입니까?
-- MAGIC 
-- MAGIC 모든 워크로드에서 모든 소스의 모든 데이터를 활용하여 항상 최저 비용과 실시간 데이터로 구동되는 보다 매력적인 고객 경험을 제공할 수 있는 유일한 엔터프라이즈 데이터 플랫폼입니다.
-- MAGIC 
-- MAGIC Lakehouse for Retail 통합 분석 및 AI 기능을 사용하면 이전에는 불가능했던 규모로 개인화된 참여, 직원 생산성, 운영 속도 및 효율성을 달성할 수 있습니다. 이는 미래 보장형 소매 혁신 및 데이터 정의 기업의 기반입니다.
-- MAGIC 
-- MAGIC 
-- MAGIC ### Simple
-- MAGIC    데이터 웨어하우징 및 AI를 위한 단일 플랫폼 및 거버넌스/보안 계층으로 **혁신을 가속화**하고 **위험을 줄입니다**. 이질적인 거버넌스와 고도로 복잡한 여러 솔루션을 함께 연결할 필요가 없습니다.
-- MAGIC 
-- MAGIC ### Open
-- MAGIC    오픈 소스 및 개방형 표준을 기반으로 합니다. 외부 솔루션과 쉽게 통합하여 데이터를 소유하고 벤더 종속을 방지합니다. 개방적이면 데이터 스택/공급업체에 관계없이 모든 외부 조직과 데이터를 공유할 수 있습니다.
-- MAGIC 
-- MAGIC ### MultiCloud
-- MAGIC    클라우드 전반에서 일관된 단일 데이터 플랫폼. 필요한 곳에서 데이터를 처리하십시오.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 데모: c360 데이터베이스를 구축하고 Databricks Lakehouse로 고객 이탈을 줄입니다.
-- MAGIC 
-- MAGIC 이 데모에서는 반복적인 사업을 통해 상품을 판매하는 소매 회사의 입장이 되어 보겠습니다.
-- MAGIC 
-- MAGIC 경영진들은 회사의 비즈니스는 고객 이탈에 중점을 두어야 한다고 결정했으며 우리는 다음을 요청받습니다.
-- MAGIC 
-- MAGIC * 현재 고객 이탈 분석 및 설명: 이탈, 추세 및 비즈니스에 미치는 영향을 정량화
-- MAGIC * 대상 이메일, 전화 등 자동화된 조치를 취하여 이탈을 예측하고 줄이기 위한 능동적인 시스템을 구축합니다.
-- MAGIC 
-- MAGIC 
-- MAGIC ### 우리가 만들어야 할 것
-- MAGIC 
-- MAGIC 이를 위해 Lakehouse와 함께 종단 간 솔루션을 구축할 것입니다. 고객 이탈을 적절하게 분석하고 예측할 수 있으려면 다양한 외부 시스템에서 오는 정보가 필요합니다. 웹사이트에서 오는 고객 프로필, ERP 시스템에서 오는 주문 세부 정보, 고객 활동을 분석하기 위한 모바일 애플리케이션 클릭 스트림.
-- MAGIC 
-- MAGIC 대략적으로 구현할 흐름은 다음과 같습니다.
-- MAGIC 
-- MAGIC <img width="900px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-0.png"/>
-- MAGIC 
-- MAGIC <!-- <img width="900px" src="#Workspace/Repos/dongwook.kim@databricks.com/field-demos-kr/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-0.png"/> -->
-- MAGIC 
-- MAGIC 1. 데이터를 수집하고 c360 데이터베이스를 생성하며, SQL에서 쉽게 쿼리할 수 있는 테이블을 사용해야 합니다.
-- MAGIC 2. 데이터를 보호하고 데이터 분석가 및 데이터 과학 팀에 대한 읽기 액세스 권한을 부여합니다.
-- MAGIC 3. BI 쿼리를 실행하여 기존 이탈 분석
-- MAGIC 4. ML 모델을 구축하여 이탈할 고객과 그 이유를 예측합니다.
-- MAGIC 
-- MAGIC 그 결과, 유지율을 높이기 위해 사용자 지정 작업을 트리거하는 데 필요한 모든 정보(맞춤형 이메일, 특별 제안, 전화 통화...)를 갖게 됩니다.
-- MAGIC 
-- MAGIC ### 보유한 데이터 세트
-- MAGIC 
-- MAGIC 이 데모를 단순화하기 위해 외부 시스템이 주기적으로 Blob Storage(S3/ADLS/GCS)로 데이터를 전송한다고 가정합니다.
-- MAGIC 
-- MAGIC - 고객 프로필 데이터 *(이름, 나이, 주소 등)*
-- MAGIC - 주문 내역 *(시간이 지남에 따라 고객이 구입하는 것)*
-- MAGIC - 당사 애플리케이션의 이벤트 *(고객이 애플리케이션을 마지막으로 사용한 시기, 클릭, 일반적으로 스트리밍)*
-- MAGIC 
-- MAGIC *기술적으로 데이터는 모든 소스에서 가져올 수 있습니다. Databricks는 모든 시스템(SalesForce, Fivetran, kafka와 같은 대기열 메시지, Blob 저장소, SQL 및 NoSQL 데이터베이스...)에서 데이터를 수집할 수 있습니다.*
-- MAGIC 
-- MAGIC Lakehouse 내에서 이 데이터를 사용하여 고객 이탈을 분석하고 줄이는 방법을 살펴보겠습니다!

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1/ 데이터 수집 및 준비(데이터 엔지니어링)
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 20px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-2.png" />
-- MAGIC 
-- MAGIC 
-- MAGIC <br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC 첫 번째 단계는 데이터 분석가 팀이 분석을 수행할 수 있도록 원시 데이터를 수집하고 클린징 하는 것 입니다. 
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="float: right; margin-top: 20px" width="200px">
-- MAGIC 
-- MAGIC ### 델타 레이크 (Delta Lake)
-- MAGIC 
-- MAGIC Lakehouse에서 생성할 모든 테이블은 Delta Lake 테이블로 저장됩니다. <br> [Delta Lake](https://delta.io)는 안정성과 성능을 위한 개방형 스토리지 프레임워크이며 많은 기능을 제공합니다 *(ACID Transaction, DELETE/UPDATE/MERGE, Clone zero copy, Change data Capture...)* <br />
-- MAGIC 
-- MAGIC ### 델타 라이브 테이블(DLT)로 수집 간소화
-- MAGIC 
-- MAGIC Databricks는 SQL 사용자가 배치 또는 스트리밍으로 고급 파이프라인을 만들 수 있도록 허용하여 Delta Live Tables로 데이터 수집 및 변환을 간소화합니다. 이 엔진은 파이프라인 배포 및 테스트를 간소화하고 운영 복잡성을 줄여주므로 비즈니스 혁신에 집중하고 데이터 품질을 보장할 수 있습니다.<br/>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the customer churn 
-- MAGIC   <a dbdemos-pipeline-id="dlt-churn" href="#joblist/pipelines/4704e053-1b43-4eb9-9024-908dd52735f3" target="_blank">Delta Live Table pipeline</a> or the [SQL notebook]($./01-Data-ingestion/01.1-DLT-churn-SQL) *(Alternatives: [DLT Python version]($./01-Data-ingestion/01.3-DLT-churn-python) - [plain Delta+Spark version]($./01-Data-ingestion/plain-spark-delta-pipeline/01.5-Delta-pipeline-spark-churn))*. <br>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2/ 데이터 보호 및 거버넌스 (Unity Catalog)
-- MAGIC 
-- MAGIC <img style="float: left" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-6.png" />
-- MAGIC 
-- MAGIC <br/><br/><br/>
-- MAGIC <div style="padding-left: 420px">
-- MAGIC   이제 첫 번째 테이블이 생성되었으므로 데이터 분석가 팀에 고객 이탈 정보 분석을 시작할 수 있도록 읽기(READ) 액세스 권한을 부여해야 합니다.
-- MAGIC   
-- MAGIC    Unity Catalog가 데이터 리니지 및 감사 로그를 포함하여 데이터 자산 전체에 **보안 및 거버넌스** 를 제공하는 방법을 살펴보겠습니다.
-- MAGIC   
-- MAGIC    Unity Catalog는 스택에 관계없이 모든 외부 조직과 데이터를 공유하는 개방형 프로토콜인 Delta Sharing을 통합합니다.
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC    Open [Unity Catalog notebook]($./02-Data-governance/02-UC-data-governance-security-churn) to see how to setup ACL and explore lineage with the Data Explorer.
-- MAGIC   

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 3/ 고객 이탈 분석  (BI / Data warehousing / SQL) 
-- MAGIC 
-- MAGIC <img width="300px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-dashboard.png"  style="float: right; margin: 100px 10px 10px;"/>
-- MAGIC 
-- MAGIC 
-- MAGIC <img width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-3.png"  style="float: left; margin-right: 10px"/>
-- MAGIC  
-- MAGIC <br><br><br>
-- MAGIC 우리의 데이터 세트는 이제 고품질로 적절하게 수집되고 보호되며 조직 내에서 쉽게 검색할 수 있습니다.
-- MAGIC 
-- MAGIC **데이터 분석가**는 이제 즉각적인 중지 및 시작을 제공하는 Serverless Datawarehouse를 포함하여 대기 시간이 짧고 처리량이 높은 BI 대화형 쿼리를 실행할 준비가 되었습니다.
-- MAGIC 
-- MAGIC PowerBI, Tableau 등과 같은 외부 BI 솔루션을 포함하여 Databricks를 사용하여 데이터 웨어하우징을 수행하는 방법을 살펴보겠습니다!

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Open the [Datawarehousing notebook]($./03-BI-data-warehousing/03-BI-Datawarehousing) to start running your BI queries or access or directly open the <a href="/sql/dashboards/19394330-2274-4b4b-90ce-d415a7ff2130" target="_blank">Churn Analysis Dashboard</a>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 4/ Data Science 및 Auto-ML 을 이용한 이탈 예측
-- MAGIC 
-- MAGIC <img width="400px" style="float: left; margin-right: 10px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-4.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC 과거 데이터에 대한 분석을 실행할 수 있어 이미 비즈니스를 추진할 수 있는 많은 통찰력을 얻었습니다. 어떤 고객이 이탈하는지 더 잘 이해할 수 있고 이탈 영향을 평가할 수 있습니다.
-- MAGIC 
-- MAGIC 그러나 이탈이 있다는 사실을 아는 것만으로는 충분하지 않습니다. 이제 이를 한 단계 더 발전시켜 이탈 위험이 있는 고객을 파악하고 수익을 늘리기 위한 **예측 모델** 을 구축해야 합니다.
-- MAGIC 
-- MAGIC 여기에서 Lakehouse의 가치가 나타납니다. 동일한 플랫폼 내에서 누구나 **AutoML**을 사용한 로우 코드 솔루션을 포함하여 이러한 분석을 실행하기 위해 ML 모델 구축을 시작할 수 있습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's see how to train an ML model within 1 click with the [04.1-automl-churn-prediction notebook]($./04-Data-Science-ML/04.1-automl-churn-prediction)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 예측을 기반으로 이탈을 줄이기 위한 작업 자동화
-- MAGIC 
-- MAGIC 
-- MAGIC <img style="float: right;margin-left: 10px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-dbsql-prediction-dashboard.png">
-- MAGIC 
-- MAGIC 이제 고객 이탈을 분석하고 예측하는 엔드 투 엔드 데이터 파이프라인이 있습니다. 이제 비즈니스를 기반으로 이탈을 줄이기 위한 조치를 쉽게 트리거할 수 있습니다.
-- MAGIC 
-- MAGIC - 이탈 가능성이 가장 높은 고객에게 타겟팅 이메일 캠페인 발송
-- MAGIC - 고객과 논의하고 진행 상황을 이해하기 위한 전화 캠페인
-- MAGIC - 당사 제품 라인의 문제점 파악 및 수정
-- MAGIC 
-- MAGIC 이러한 작업은 이 데모의 범위를 벗어나며 단순히 ML 모델의 Churn 예측 필드를 활용합니다.
-- MAGIC 
-- MAGIC ## 다음 달 이탈 영향 및 캠페인 영향 추적
-- MAGIC 
-- MAGIC 물론 이 이탈 예측은 대시보드에서 재사용하여 향후 이탈을 분석하고 이탈 감소를 측정할 수 있습니다.
-- MAGIC 
-- MAGIC Lakehouse로 생성된 파이프라인은 강력한 ROI를 제공할 것입니다. 이 파이프라인 엔드 투 엔드를 설정하는 데 몇 시간이 걸렸고 월 $129,914의 잠재적인 이익을 얻었습니다!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Open the <a href='/sql/dashboards/1e236ef7-cf58-4bfc-b861-5e6a0c105e51' target="_blank">Churn prediction DBSQL dashboard</a> to have a complete view of your business, including churn prediction and proactive analysis.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 5/ 전체 워크플로 배포 및 오케스트레이션
-- MAGIC 
-- MAGIC <img style="float: left; margin-right: 10px" width="400px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-5.png" />
-- MAGIC 
-- MAGIC <br><br><br>
-- MAGIC 데이터 파이프라인이 거의 완성되었지만 프로덕션에서 전체 워크플로를 오케스트레이션 하는 마지막 단계가 누락되었습니다.
-- MAGIC 
-- MAGIC Databricks Lakehouse를 사용하면 작업을 실행하기 위해 외부 오케스트레이터를 관리할 필요가 없습니다. **Databricks Workflows**는 고급 경고, 모니터링, 분기 옵션 등을 통해 모든 작업을 단순화합니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Open the [workflow and orchestration notebook]($./05-Workflow-orchestration/05-Workflow-orchestration-churn) to schedule our pipeline (data ingetion, model re-training etc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## 결론
-- MAGIC 
-- MAGIC 우리는 통합되고 안전한 단일 플랫폼을 사용하여 Lakehouse로 엔드 투 엔드 파이프라인을 구현하는 방법을 시연했습니다.
-- MAGIC 
-- MAGIC - 데이터 수집
-- MAGIC - 데이터 분석 / DW / BI
-- MAGIC - 데이터 사이언스/ML
-- MAGIC - 워크플로우 및 오케스트레이션
-- MAGIC 
-- MAGIC 그 결과 분석가 팀은 단순히 시스템을 구축하여 향후 변동을 이해하고 예측하고 그에 따라 조치를 취할 수 있었습니다.
-- MAGIC 
-- MAGIC 이것은 Databricks 플랫폼에 대한 소개일 뿐입니다. 보더 자세한 내용은 영업팀에 문의하고 `dbdemos.list()`로 더 많은 데모를 살펴보십시오.

-- COMMAND ----------


