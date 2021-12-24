# Databricks notebook source
database = "ContosoRetailDW"
schema = "dbo"			 		
tables = [
  "DimAccount",
  "DimChannel",
  "DimCurrency",
  "DimCustomer",
  "DimDate",
  "DimEmployee",
  "DimEntity",
  "DimGeography",
  "DimMachine",
  "DimOutage",
  "DimProduct",
  "DimProductCategory",
  "DimProductSubcategory",
  "DimPromotion",
  "DimSalesTerritory",
  "DimScenario",
  "DimStore",
  "FactExchangeRate",
  "FactInventory",
  "FactITMachine",
  "FactITSLA",
  "FactOnlineSales",
  "FactSales",
  "FactSalesQuota",
  "FactStrategyPlan"
]

for table in tables:
  params = {"database":database, "schema":schema, "table":table}
  ret = dbutils.notebook.run("./load-table", 2600, params)
  print(f"load {database}.{schema}.{table} {ret}")
  

# COMMAND ----------

dbutils.notebook.run("./data-modelling", 2600, {})
dbutils.notebook.run("./dax-book", 2600, {})
dbutils.notebook.run("./contoso-bi", 2600, {})

