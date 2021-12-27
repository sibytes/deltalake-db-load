# Databricks notebook source
dbutils.widgets.text(name="database", defaultValue="adventure-works", label="database")

# COMMAND ----------

import json
import os

data = dbutils.widgets.get("database")
metadata = os.getcwd()
metadata = f"{metadata}/{data}.json"

database = data.title().replace("-", "")

schema = "dbo"
with open(metadata, "r") as f:
  tables = json.load(f)
  
tables = tables["metadata"]

for table in tables:
  t = table["schema"]
  s = table["table"]
  
  table["database"] = database
  
  ret = dbutils.notebook.run("./load-table", 2600, table)
  print(f"loaded {database}.{t}.{s}")

  
