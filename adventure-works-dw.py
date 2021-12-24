# Databricks notebook source
database = "AdventureWorksDW"
schema = "dbo"			 		
tables = []

for table in tables:
  params = {"database":database, "schema":schema, "table":table}
  ret = dbutils.notebook.run("./load-table", 2600, params)
  print(f"load {database}.{schema}.{table} {ret}")
  
