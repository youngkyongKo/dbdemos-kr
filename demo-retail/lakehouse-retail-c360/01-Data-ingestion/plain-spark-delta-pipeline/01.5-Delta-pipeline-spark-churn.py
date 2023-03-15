# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Ingesting and transforming churn data with Delta Lake and Spark API
# MAGIC 
# MAGIC <img style="float: right" width="300px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-2.png" />
# MAGIC 
# MAGIC In this notebook, we'll show you an alternative to Delta Live Table: building an ingestion pipeline with the Spark API.
# MAGIC 
# MAGIC As you'll see, this implementation is lower level than the Delta Live Table pipeline, and you'll have control over all the implementation details (handling checkpoints, data quality etc).
# MAGIC 
# MAGIC Lower level also means more power. Using Spark API, you'll have unlimited capabilities to ingest data in Batch or Streaming.
# MAGIC 
# MAGIC If you're unsure what to use, start with Delta Live Table!
# MAGIC 
# MAGIC *Remember that Databricks workflow can be used to orchestrate a mix of Delta Live Table pipeline with standard Spark pipeline.*
# MAGIC 
# MAGIC As reminder, we have multiple data sources coming from different system:
# MAGIC 
# MAGIC - Customer profile data *(name, age, adress etc)*
# MAGIC - Orders history *(what our customer bough over time)*
# MAGIC - Events from our application *(when was the last time customers used the application, typically this could be a stream from a Kafka queue)*
# MAGIC 
# MAGIC 
# MAGIC Leveraging Spark and Delta Lake makes such an implementation easy.
# MAGIC 
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Flakehouse_churn%2Fdelta_pipeline&dt=LAKEHOUSE_RETAIL_CHURN">

# COMMAND ----------

# MAGIC %run ../../_resources/00-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Building a Spark Data pipeline with Delta Lake
# MAGIC 
# MAGIC In this example, we'll implement a end 2 end pipeline consuming our customers information. We'll use the medaillon architecture but could build star schema, data vault or any other modelisation.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC This can be challenging with traditional systems due to the following:
# MAGIC  * Data quality issue
# MAGIC  * Running concurrent operation
# MAGIC  * Running DELETE/UPDATE/MERGE over files
# MAGIC  * Governance & schema evolution
# MAGIC  * Performance ingesting millions of small files on cloud buckets
# MAGIC  * Processing & analysing unstructured data (image, video...)
# MAGIC  * Switching between batch or streaming depending of your requirement...
# MAGIC 
# MAGIC ## Solving these challenges with Delta Lake
# MAGIC 
# MAGIC <div style="float:left">
# MAGIC 
# MAGIC **What's Delta Lake? It's a new OSS standard to bring SQL Transactional database capabilities on top of parquet files!**
# MAGIC 
# MAGIC Used as a new Spark format, built on top of Spark API / SQL
# MAGIC 
# MAGIC * **ACID transactions** (Multiple writers can simultaneously modify a data set)
# MAGIC * **Full DML support** (UPDATE/DELETE/MERGE)
# MAGIC * **BATCH and STREAMING** support
# MAGIC * **Data quality** (expectatiosn, Schema Enforcement, Inference and Evolution)
# MAGIC * **TIME TRAVEL** (Look back on how data looked like in the past)
# MAGIC * **Performance boost** with ZOrder, data skipping and Caching, solves small files issue 
# MAGIC </div>
# MAGIC 
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo.png" style="height: 200px"/>
# MAGIC 
# MAGIC <br style="clear: both">
# MAGIC 
# MAGIC We'll incrementally load new data with the autoloader, enrich this information and then load a model from MLFlow to perform our customer churn prediction.
# MAGIC 
# MAGIC This information will then be used to build our DBSQL dashboard to track customer behavior and churn.
# MAGIC 
# MAGIC Let'simplement the following flow: 
# MAGIC  
# MAGIC <div><img width="1100px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta.png"/></div>
# MAGIC 
# MAGIC *Note that we're including the ML model our [Data Scientist built](TODO) using Databricks AutoML to predict the churn.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 1/ Explore the dataset
# MAGIC 
# MAGIC Let's review the files being received

# COMMAND ----------

# MAGIC %fs ls /demos/retail/churn/users

# COMMAND ----------

# DBTITLE 1,Review the raw user data received as JSON
# MAGIC %sql
# MAGIC USE catalog DONGWOOK_demos;
# MAGIC use schema lakehouse_c360 ;
# MAGIC -- SELECT * FROM json.`/demos/retail/churn/users`

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
# MAGIC <div style="float:right">
# MAGIC   <img width="700px" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-1.png"/>
# MAGIC </div>
# MAGIC   
# MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
# MAGIC 
# MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
# MAGIC 
# MAGIC Let's use it to [create our pipeline](https://e2-demo-field-eng.cloud.databricks.com/?o=1444828305810485#joblist/pipelines/95f28631-1884-425e-af69-05c3f397dd90) and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/retail/churn/...`. 

# COMMAND ----------

# DBTITLE 1,We'll store the raw data in a USER_BRONZE DELTA table, supporting schema evolution and incorrect data
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS user_bronze;
# MAGIC -- Note: tables are automatically created during  .writeStream.table("user_bronze") operation, but we can also use plain SQL to create them:
# MAGIC CREATE TABLE IF NOT EXISTS user_bronze (
# MAGIC      id                 STRING,
# MAGIC      email              STRING,
# MAGIC      creation_date      TIMESTAMP,
# MAGIC      last_activity_date TIMESTAMP,
# MAGIC      firstname          STRING,
# MAGIC      lastname           STRING,
# MAGIC      address            STRING,
# MAGIC      age_group          INT,
# MAGIC      canal              STRING,
# MAGIC      churn              INT,
# MAGIC      country            STRING,
# MAGIC      gender             INT
# MAGIC      
# MAGIC   ) using delta tblproperties (
# MAGIC      delta.autooptimize.optimizewrite = TRUE,
# MAGIC      delta.autooptimize.autocompact   = TRUE ); 
# MAGIC -- With these 2 last options, Databricks engine will solve small files & optimize write out of the box!

# COMMAND ----------

def ingest_folder(folder, data_format, table):
  bronze_products = (spark.readStream
                              .format("cloudFiles")
                              .option("cloudFiles.format", data_format)
                              .option("cloudFiles.inferColumnTypes", "true")
                              .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/schema/{table}") #Autoloader will automatically infer all the schema & evolution
                              .load(folder))

  return (bronze_products.writeStream
                    .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/{table}") #exactly once delivery on Delta tables over restart/kill
                    .option("mergeSchema", "true") #merge any new column dynamically
                    .trigger(once = True) #Remove for real time streaming
                    .table(table)) #Table will be created if we haven't specified the schema first
  
ingest_folder('/demos/retail/churn/orders', 'json', 'churn_orders_bronze')
ingest_folder('/demos/retail/churn/events', 'csv', 'churn_app_events')
ingest_folder('/demos/retail/churn/users', 'json',  'churn_users_bronze').awaitTermination()

# COMMAND ----------

# DBTITLE 1,Our user_bronze Delta table is now ready for efficient query
# MAGIC %sql 
# MAGIC -- Note the "_rescued_data" column. If we receive wrong data not matching existing schema, it'll be stored here
# MAGIC select * from churn_users_bronze;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC ## ![](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) 2/ Silver data: anonimized table, date cleaned
# MAGIC 
# MAGIC <img width="700px" style="float:right" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-2.png"/>
# MAGIC 
# MAGIC We can chain these incremental transformation between tables, consuming only new data.
# MAGIC 
# MAGIC This can be triggered in near realtime, or in batch fashion, for example as a job running every night to consume daily data.

# COMMAND ----------

(spark.readStream 
        .table("churn_users_bronze")
        .withColumnRenamed("id", "user_id")
        .withColumn("email", sha1(col("email")))
        .withColumn("creation_date", to_timestamp(col("creation_date"), "MM-dd-yyyy H:mm:ss"))
        .withColumn("last_activity_date", to_timestamp(col("last_activity_date"), "MM-dd-yyyy HH:mm:ss"))
        .withColumn("firstname", initcap(col("firstname")))
        .withColumn("lastname", initcap(col("lastname")))
        .withColumn("age_group", col("age_group").cast('int'))
        .withColumn("gender", col("gender").cast('int'))
        .withColumn("churn", col("churn").cast('int'))
        .drop(col("_rescued_data"))
     .writeStream
        .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/users")
        .trigger(once=True)
        .table("churn_users").awaitTermination())

# COMMAND ----------

# MAGIC %sql select * from churn_users;

# COMMAND ----------

(spark.readStream 
        .table("churn_orders_bronze")
        .withColumnRenamed("id", "order_id")
        .withColumn("amount", col("amount").cast('int'))
        .withColumn("item_count", col("item_count").cast('int'))
        .drop(col("_rescued_data"))
     .writeStream
        .option("checkpointLocation", f"{cloud_storage_path}/checkpoint/orders")
        .trigger(once=True)
        .table("churn_orders").awaitTermination())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### 3/ Aggregate and join data to create our ML features
# MAGIC 
# MAGIC <img width="700px" style="float:right" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-3.png"/>
# MAGIC 
# MAGIC 
# MAGIC We're now ready to create the features required for our Churn prediction.
# MAGIC 
# MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, sucj as:
# MAGIC 
# MAGIC * last command date
# MAGIC * number of item bought
# MAGIC * number of actions in our website
# MAGIC * device used (ios/iphone)
# MAGIC * ...

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TABLE churn_features AS
      WITH 
          churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item
            FROM churn_orders GROUP BY user_id),  
          churn_app_events_stats as (
            SELECT first(platform) as platform, user_id, count(*) as event_count, count(distinct session_id) as session_count, max(to_timestamp(date, "MM-dd-yyyy HH:mm:ss")) as last_event
              FROM churn_app_events GROUP BY user_id)
        SELECT *, 
           datediff(now(), creation_date) as days_since_creation,
           datediff(now(), last_activity_date) as days_since_last_activity,
           datediff(now(), last_event) as days_last_event
           FROM churn_users
             INNER JOIN churn_orders_stats using (user_id)
             INNER JOIN churn_app_events_stats using (user_id)""")
     
display(spark.table("churn_features"))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## 5/ Enriching the gold data with a ML model
# MAGIC 
# MAGIC <img width="700px" style="float:right" src="https://raw.githubusercontent.com/QuentinAmbard/databricks-demo/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-delta-5.png"/>
# MAGIC 
# MAGIC Our Data scientist team has build a churn prediction model using Auto ML and saved it into Databricks Model registry. 
# MAGIC 
# MAGIC One of the key value of the Lakehouse is that we can easily load this model and predict our churn right into our pipeline. 
# MAGIC 
# MAGIC Note that we don't have to worry about the model framework (sklearn or other), MLFlow abstract that for us.

# COMMAND ----------

# DBTITLE 1,Load the model as SQL function
# MAGIC %python
# MAGIC import mlflow
# MAGIC #                                                                              Stage/version    output
# MAGIC #                                                                 Model name         |            |
# MAGIC #                                                                     |              |            |
# MAGIC predict_churn_udf = mlflow.pyfunc.spark_udf(spark, "models:/dbdemos_customer_churn/Production", "int")

# COMMAND ----------

# DBTITLE 1,Call our model and predict churn in our pipeline
model_features = predict_churn_udf.metadata.get_input_schema().input_names()
predictions = spark.table('churn_features').withColumn('churn_prediction', predict_churn_udf(*model_features)).write.format("delta").mode("overwrite").saveAsTable("churn_prediction")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Simplify your operations with transactional DELETE/UPDATE/MERGE operations
# MAGIC 
# MAGIC Traditional Data Lake struggle to run these simple DML operations. Using Databricks and Delta Lake, your data is stored on your blob storage with transactional capabilities. You can issue DML operation on Petabyte of data without having to worry about concurrent operations.

# COMMAND ----------

# DBTITLE 1,We just realised we have to delete user created before 2016-01-01 for compliance, let's fix that
# MAGIC %sql DELETE FROM churn_users where creation_date < '2016-01-01T03:38:55.000+0000';

# COMMAND ----------

# DBTITLE 1,Delta Lake keeps history of the table operation
# MAGIC %sql describe history churn_users;

# COMMAND ----------

# DBTITLE 1,We can leverage the history to go back in time, restore or clone a table and enable CDC...
# MAGIC %sql 
# MAGIC  --also works with AS OF TIMESTAMP "yyyy-MM-dd HH:mm:ss"
# MAGIC select * from churn_users version as of 1 ;
# MAGIC 
# MAGIC -- You made the DELETE by mistake ? You can easily restore the table at a given version / date:
# MAGIC -- RESTORE TABLE churn_users_clone TO VERSION AS OF 1
# MAGIC 
# MAGIC -- Or clone it (SHALLOW provides zero copy clone):
# MAGIC -- CREATE TABLE user_gold_clone SHALLOW|DEEP CLONE user_gold VERSION AS OF 1
# MAGIC 
# MAGIC -- Turn on CDC to capture insert/update/delete operation:
# MAGIC -- ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# DBTITLE 1,Make sure all our tables are optimized
# MAGIC %sql
# MAGIC ALTER TABLE churn_users    SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );
# MAGIC ALTER TABLE churn_orders   SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );
# MAGIC ALTER TABLE churn_features SET TBLPROPERTIES (delta.autooptimize.optimizewrite = TRUE, delta.autooptimize.autocompact = TRUE );

# COMMAND ----------

# DBTITLE 1,Our user table will be requested by 3 field mostly, let's optimize the table for that.
# MAGIC %sql
# MAGIC OPTIMIZE churn_users ZORDER BY user_id, firstname, lastname

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Our finale tables are now ready to be used to build SQL Dashboards and ML models for customer classification!
# MAGIC <img style="float: right" width="400" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dashboard.png"/>
# MAGIC 
# MAGIC 
# MAGIC Switch to Databricks SQL to see how this data can easily be requested using <a href='/sql/dashboards/1e236ef7-cf58-4bfc-b861-5e6a0c105e51' target="_blank">Churn prediction DBSQL dashboard</a>, or an external BI tool. 
# MAGIC 
# MAGIC Creating a single flow was simple.  However, handling many data pipeline at scale can become a real challenge:
# MAGIC * Hard to build and maintain table dependencies 
# MAGIC * Difficult to monitor & enforce advance data quality
# MAGIC * Impossible to trace data lineage
# MAGIC * Difficult pipeline operations (observability, error recovery)
# MAGIC 
# MAGIC 
# MAGIC #### To solve these challenges, Databricks introduced **Delta Live Table**
# MAGIC A simple way to build and manage data pipelines for fresh, high quality data!

# COMMAND ----------

# MAGIC %md
# MAGIC # Next: secure and share data with Unity Catalog
# MAGIC 
# MAGIC Now that these tables are available in our Lakehouse, let's review how we can share them with the Data Scientists and Data Analysts teams.
# MAGIC 
# MAGIC Jump to the [Governance with Unity Catalog notebook]($../02-Data-governance/02-UC-data-governance-security-churn) or [Go back to the introduction]($../00-churn-introduction-lakehouse)
