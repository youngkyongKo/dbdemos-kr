# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

# COMMAND ----------

# MAGIC %run ../../../_resources/00-global-setup $reset_all_data=$reset_all_data $db_prefix=retail

# COMMAND ----------

catalog = "dongwook_demos"

catalog_exists = False
for r in spark.sql("SHOW CATALOGS").collect():
    if r['catalog'] == catalog:
        catalog_exists = True

#As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
if not catalog_exists:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
spark.sql(f"USE CATALOG {catalog}")

database = 'lakehouse_c360'

db_exists = spark._jsparkSession.catalog().databaseExists(f"`{catalog}`.`{database}`")

if not db_exists:
  print(f"creating {database} database")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
  spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
  spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")
  
spark.sql(f'use {database}')

# COMMAND ----------

import json
import time
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, sha1, col, initcap, to_timestamp

folder = "/databricks-dongwook/demos/retail/churn"

if reset_all_data or is_folder_empty(folder+"/orders") or is_folder_empty(folder+"/users") or is_folder_empty(folder+"/events"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("lakehouse-retail-churn"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
#   dbutils.notebook.run(prefix+"02-create-churn-tables", 600, {"catalog": catalog, "cloud_storage_path": "/demos/", "reset_all_data": reset_all_data})
  dbutils.notebook.run("/Repos/dongwook.kim@databricks.com/field-demos-kr/field-demo/demo-retail/lakehouse-retail-c360/_resources/02-create-churn-tables",600, {"catalog": catalog, "cloud_storage_path": "/demos/", "reset_all_data": reset_all_data})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")

# COMMAND ----------

spark.sql(f"ALTER TABLE {catalog}.{database}.churn_orders_bronze OWNER TO `account users`")
spark.sql(f"ALTER TABLE {catalog}.{database}.churn_app_events OWNER TO `account users`")
spark.sql(f"ALTER TABLE {catalog}.{database}.churn_users_bronze OWNER TO `account users`")
spark.sql(f"ALTER TABLE {catalog}.{database}.churn_orders OWNER TO `account users`")
spark.sql(f"ALTER TABLE {catalog}.{database}.churn_users OWNER TO `account users`")
spark.sql(f"ALTER TABLE {catalog}.{database}.churn_features OWNER TO `account users`")

# COMMAND ----------


