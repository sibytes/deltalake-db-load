# Databricks notebook source
dbutils.widgets.text(name="database", defaultValue="adventure-works", label="database")

# COMMAND ----------

import discover_modules
from pprint import pprint
discover_modules.go(spark)

# COMMAND ----------

from utilities import AppConfig

config = AppConfig(dbutils, spark)
config.connect_storage()

# COMMAND ----------


data = dbutils.widgets.get("database")
dbname = data.title().replace("-", "")

try:
  tables = spark.sql(f"show tables in {dbname}").collect()
except:
  dbutils.notebook.exit(f"Database {dbname} Doesn't Exist")
  
for t in tables:
  sql = f"DROP TABLE IF EXISTS {t[0]}.{t[1]}"
  print(sql)
  spark.sql(sql)
  
spark.sql(f"DROP DATABASE IF EXISTS {dbname}")


# COMMAND ----------


dbpath = f"{config.get_storage_account()}databricks/delta/{dbname.lower()}"

dbutils.fs.rm(dbpath, True)

