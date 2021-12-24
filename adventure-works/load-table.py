# Databricks notebook source
print("***************************************************************")

# COMMAND ----------

# dbutils.widgets.text(name="database", defaultValue="contosoretaildw", label="database")
# dbutils.widgets.text(name="schema", defaultValue="dbo", label="schema")
# dbutils.widgets.text(name="table", defaultValue="dimaccount", label="table")

# COMMAND ----------

# not compatible or needed on a passthrough AD cluster!
from fathom.ConnectStorage import connect_storage
from fathom import Configuration as config

connect_storage()
config.help()

# COMMAND ----------

schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")
dbname = dbutils.widgets.get("database")

root = f"{config.get_storage_account()}raw/{dbname.lower()}/data/2007/01/01/"
ext = "jsonl"
mask = f"*_{schema.lower()}{table.lower()}_*_*_*"
path = f"{root}{mask}.{ext}"
dbpath = f"{config.get_storage_account()}databricks/delta/{dbname.lower()}/{schema.lower()}/{table.lower()}"

print(path)
print(dbpath)

# COMMAND ----------

sql = f"CREATE DATABASE IF NOT EXISTS {dbname}"
print(sql)
spark.sql(sql)

# COMMAND ----------

sql = f"DROP TABLE IF EXISTS {dbname}.{table}"
print(sql)
spark.sql(sql)

# COMMAND ----------

df = spark.read.json(path)
print(sql)
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(dbpath)

# COMMAND ----------

sql = f"CREATE TABLE IF NOT EXISTS {dbname}.{table} USING DELTA LOCATION '{dbpath}'"
print(sql)
spark.sql(sql)


# COMMAND ----------

dbutils.notebook.exit("SUCCEEDED")
