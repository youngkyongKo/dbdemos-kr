-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC # C360 레이크하우스의 거버넌스 및 보안 보장
-- MAGIC 
-- MAGIC 완전한 데이터 플랫폼의 경우 데이터 거버넌스와 보안이 어렵습니다. 테이블에 대한 SQL GRANT로는 충분하지 않으며 여러 데이터 자산(대시보드, 모델, 파일 등)에 대해 보안을 적용해야 합니다.
-- MAGIC 
-- MAGIC 위험을 줄이고 혁신을 추진하기 위해 Emily의 팀은 다음을 수행해야 합니다.
-- MAGIC 
-- MAGIC - 모든 데이터 자산(테이블, 파일, ML 모델, 기능, 대시보드, 쿼리) 통합
-- MAGIC - 여러 팀의 온보딩 데이터
-- MAGIC - 외부 조직과 자산 공유 및 수익화
-- MAGIC 
-- MAGIC <style>
-- MAGIC .box{
-- MAGIC   box-shadow: 20px -20px #CCC; height:300px; box-shadow:  0 0 10px  rgba(0,0,0,0.3); padding: 5px 10px 0px 10px;}
-- MAGIC .badge {
-- MAGIC   clear: left; float: left; height: 30px; width: 30px;  display: table-cell; vertical-align: middle; border-radius: 50%; background: #fcba33ff; text-align: center; color: white; margin-right: 10px}
-- MAGIC .badge_b { 
-- MAGIC   height: 35px}
-- MAGIC </style>
-- MAGIC <link href='https://fonts.googleapis.com/css?family=DM Sans' rel='stylesheet'>
-- MAGIC <div style="padding: 20px; font-family: 'DM Sans'; color: #1b5162">
-- MAGIC   <div style="width:200px; float: left; text-align: center">
-- MAGIC     <div class="box" style="">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team A</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/da.png" style="" width="60px"> <br/>
-- MAGIC         데이터 분석가<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/ds.png" style="" width="60px"> <br/>
-- MAGIC         데이터 과학자<br/>
-- MAGIC         <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/de.png" style="" width="60px"> <br/>
-- MAGIC         데이터 엔지니어
-- MAGIC       </div>
-- MAGIC     </div>
-- MAGIC     <div class="box" style="height: 80px; margin: 20px 0px 50px 0px">
-- MAGIC       <div style="font-size: 26px;">
-- MAGIC         <strong>Team B</strong>
-- MAGIC       </div>
-- MAGIC       <div style="font-size: 13px">...</div>
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   <div style="float: left; width: 400px; padding: 0px 20px 0px 20px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">쿼리, 대시보드에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">테이블 , 컬럼, 로우에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">피쳐, ML 모델, 엔드포인트, 노트북 ... 에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC     <div style="margin: 20px 0px 0px 20px">파일, 잡에 대한 권한</div>
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/horizontal-arrow-dash.png" style="width: 400px">
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   <div class="box" style="width:550px; float: left">
-- MAGIC     <img src="https://github.com/databricks-demos/dbdemos-resources/raw/main/images/gov.png" style="float: left; margin-right: 10px;" width="80px"> 
-- MAGIC     <div style="float: left; font-size: 26px; margin-top: 0px; line-height: 17px;"><strong>Emily</strong> <br />거버넌스 및 보안</div>
-- MAGIC     <div style="font-size: 18px; clear: left; padding-top: 10px">
-- MAGIC       <ul style="line-height: 2px;">
-- MAGIC         <li>중앙 카탈로그 - 모든 데이터 자산</li>
-- MAGIC         <li>데이터 탐색 & 새로운 사용 사례를 발견 </li>
-- MAGIC         <li>팀간의 권한</li>
-- MAGIC         <li>오딧 로그로 위험 감소</li>
-- MAGIC         <li>lineage(계보)로 영향도 분석</li>
-- MAGIC       </ul>
-- MAGIC       + 수익 창출 및 외부 조직과 데이터 공유(Delta Sharing)
-- MAGIC     </div>
-- MAGIC   </div>
-- MAGIC   
-- MAGIC   
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # Unity Catalog로 글로벌 데이터 거버넌스 및 보안 구현
-- MAGIC 
-- MAGIC <img style="float: right; margin-top: 30px" width="500px" src="https://raw.githubusercontent.com/dongwkim/field-demos-kr/markdown-korean/field-demo/images/retail/lakehouse-chrun/lakehouse-retail-c360-churn-2.png" />
-- MAGIC 
-- MAGIC Lakehouse가 Unity Catalog를 활용하여 이 문제를 어떻게 해결할 수 있는지 살펴보겠습니다.
-- MAGIC 
-- MAGIC 우리의 데이터는 데이터 엔지니어링 팀에 의해 델타 테이블로 저장되었습니다. 다음 단계는 팀 간 액세스를 허용하면서 이 데이터를 보호하는 것입니다. <br>
-- MAGIC 일반적인 설정은 다음과 같습니다.
-- MAGIC 
-- MAGIC * 데이터 엔지니어/Job은 기본 데이터/스키마(ETL 부분)를 읽고 업데이트할 수 있습니다.
-- MAGIC * 데이터 과학자는 최종 테이블을 읽고 피쳐 테이블을 업데이트할 수 있습니다.
-- MAGIC * 데이터 분석가는 데이터 엔지니어링 및 피쳐 테이블에 대한 읽기 액세스 권한이 있으며 별도의 스키마에서 추가 데이터를 수집/변환할 수 있습니다.
-- MAGIC * 데이터는 각 사용자 액세스 수준에 따라 동적으로 마스킹/익명화됩니다.
-- MAGIC 
-- MAGIC 이것은 Unity Catalog로 가능합니다. 테이블이 Unity 카탈로그에 저장되면 전체 조직, 교차 워크스페이스 및 교차 사용자가 테이블에 액세스할 수 있습니다.
-- MAGIC 
-- MAGIC Unity Catalog는 데이터 제품을 생성하거나 datamesh를 중심으로 팀을 구성하는 것을 포함하여 데이터 거버넌스의 핵심입니다. 다음을 제공합니다.
-- MAGIC 
-- MAGIC * 세분화된 ACL
-- MAGIC * 감사 로그
-- MAGIC * 데이터 계보
-- MAGIC * 데이터 탐색 및 발견
-- MAGIC * 외부 기관과 데이터 공유(Delta Sharing)

-- COMMAND ----------

-- MAGIC %run ../_resources/00-setup-uc $reset_all_data=false

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## Customer360 데이터베이스 탐색
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/uc-base-1.png" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC 생성된 데이터를 검토해 보겠습니다.
-- MAGIC 
-- MAGIC Unity Catalog는 3개의 레이어로 작동합니다.
-- MAGIC 
-- MAGIC * CATALOG
-- MAGIC * SCHEMA (or DATABASE)
-- MAGIC * TABLE
-- MAGIC 
-- MAGIC 모든 통합 카탈로그는 SQL에서 사용 가능 (`CREATE CATALOG IF NOT EXISTS my_catalog` ...)
-- MAGIC 
-- MAGIC 하나의 테이블에 액세스하려면 전체 경로를 지정하면 됩니다.: `SELECT * FROM &lt;CATALOG&gt;.&lt;SCHEMA&gt;.&lt;TABLE&gt;`

-- COMMAND ----------

--CREATE CATALOG IF NOT EXISTS dongwook_demos;
USE CATALOG dongwook_demos;
SELECT CURRENT_CATALOG();

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 스키마 아래에서 생성한 테이블을 검토해 보겠습니다.
-- MAGIC 
-- MAGIC <img src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-data-explorer.gif" style="float: right" width="800px"/> 
-- MAGIC 
-- MAGIC Unity Catalog는 왼쪽 메뉴에서 액세스할 수 있는 포괄적인 Data Explorer를 제공합니다.
-- MAGIC 
-- MAGIC 모든 테이블을 찾을 수 있으며 이를 사용하여 테이블에 액세스하고 관리할 수 있습니다.
-- MAGIC 
-- MAGIC 이 스키마에 추가 테이블을 만들 수 있습니다.
-- MAGIC 
-- MAGIC ### 스키마 검색 가능
-- MAGIC 
-- MAGIC 또한 Unity 카탈로그는 스키마 탐색과 찾기도 제공합니다.
-- MAGIC 
-- MAGIC 테이블에 액세스할 수 있는 사람은 누구나 테이블을 검색하고 주요 용도를 분석할 수 있습니다. <br>
-- MAGIC 검색 메뉴(⌘ + P)를 사용하여 데이터 자산(테이블, 노트북, 쿼리...)을 탐색할 수 있습니다..)

-- COMMAND ----------

-- DBTITLE 1,테이블은 카탈로그상에서 사용할 수 있습니다.
CREATE SCHEMA IF NOT EXISTS lakehouse_c360;
USE lakehouse_c360;
SHOW TABLES IN lakehouse_c360;

-- COMMAND ----------

-- DBTITLE 1,분석가 및 데이터 엔지니어에게 액세스 권한 부여
-- 분석가에서 특정 테이블에 대한 조회 권한 부여 
GRANT SELECT ON TABLE dongwook_demos.lakehouse_c360.churn_users TO `analysts`;
GRANT SELECT ON TABLE dongwook_demos.lakehouse_c360.churn_app_events TO `analysts`;
GRANT SELECT ON TABLE dongwook_demos.lakehouse_c360.churn_orders TO `analysts`;

-- 데이터 엔지니어에게 스키마(데이터베이스)에 대한 조회 및 수정 권한 부여  
GRANT SELECT, MODIFY ON SCHEMA dongwook_demos.lakehouse_c360 TO `dataengineers`;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC ## 데이터 거버넌스 및 보안으로 더 나아가기
-- MAGIC 
-- MAGIC 모든 데이터 자산을 통합함으로써 Unity Catalog를 사용하면 팀을 확장하는 데 도움이 되는 완전하고 간단한 거버넌스를 구축할 수 있습니다.
-- MAGIC 
-- MAGIC Unity Catalog는 단순한 GRANT에서 완전한 데이터 메시 조직 구축까지 활용할 수 있습니다.
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/uc/lineage/lineage-table.gif" style="float: right; margin-left: 10px"/>
-- MAGIC 
-- MAGIC ### Fine-grained ACL
-- MAGIC 
-- MAGIC 고급 제어가 필요하십니까? 사용자 권한에 따라 테이블 조회을 동적으로 변경하도록 선택할 수 있습니다
-- MAGIC 
-- MAGIC ### Secure external location (S3/ADLS/GCS)
-- MAGIC 
-- MAGIC Unity Catalog를 사용하면 관리 테이블뿐 아니라 외부 저장소 위치도 보호할 수 있습니다
-- MAGIC 
-- MAGIC ### Lineage 
-- MAGIC 
-- MAGIC UC는 테이블 종속성을 자동으로 캡처하고 행 수준을 포함하여 데이터가 사용되는 방식을 추적할 수 있습니다.
-- MAGIC 
-- MAGIC 이를 통해 다운스트림 영향을 분석하거나 전체 조직(GDPR)에서 민감한 정보를 모니터링할 수 있습니다.
-- MAGIC 
-- MAGIC ### Audit log
-- MAGIC 
-- MAGIC UC는 모든 이벤트를 캡처합니다. 누가 어떤 데이터에 액세스하는지 확인가능합니다 
-- MAGIC 
-- MAGIC 이를 통해 다운스트림 영향을 분석하거나 전체 조직(GDPR)에서 민감한 정보를 모니터링할 수 있습니다.
-- MAGIC 
-- MAGIC ### Upgrading to UC
-- MAGIC 
-- MAGIC 이미 UC 없이 Databricks를 사용 중이신가요? Unity Catalog의 이점을 활용하기 위해 테이블을 업그레이드하는 것은 간단합니다.
-- MAGIC 
-- MAGIC ### Sharing data with external organization
-- MAGIC 
-- MAGIC Databricks 사용자 외부에서 데이터를 공유하는 것은 델타 공유를 통해 간단하며 데이터 소비자가 Databricks를 사용할 필요가 없습니다.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Next: Databricks SQL로 분석 구축 시작
-- MAGIC 
-- MAGIC 이제 이러한 테이블을 Lakehouse에서 사용할 수 있고 보안이 유지되었으므로 Data Analyst 팀이 이를 활용하여 BI 워크로드를 실행하는 방법을 살펴보겠습니다.
-- MAGIC 
-- MAGIC Jump to the [BI / Data warehousing notebook]($../03-BI-data-warehousing/03-BI-Datawarehousing) or [Go back to the introduction]($../00-churn-introduction-lakehouse)

-- COMMAND ----------


