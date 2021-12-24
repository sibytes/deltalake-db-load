-- Databricks notebook source
-- DBTITLE 1,Create the DB
create database if not exists jaffle_shop;
create database if not exists stripe;

-- COMMAND ----------

-- DBTITLE 1,Mount Storage
-- MAGIC %python
-- MAGIC # not compatible or needed on a passthrough AD cluster!
-- MAGIC from fathom.ConnectStorage import connect_storage
-- MAGIC 
-- MAGIC connect_storage()

-- COMMAND ----------

-- DBTITLE 1,Show Configuration Functions
-- MAGIC %python 
-- MAGIC from fathom.Configuration import help, get_storage_account
-- MAGIC 
-- MAGIC displayHTML(help(True))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC root = get_storage_account()
-- MAGIC system = "jaffleshop"
-- MAGIC source_layer = "raw"
-- MAGIC period_partition = "2018/01/01"
-- MAGIC 
-- MAGIC path = f"{root}{source_layer}/{system}/data/{period_partition}"
-- MAGIC display(dbutils.fs.ls(path))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC 
-- MAGIC def load_table(destination_root:str, source_root:str, table_name:str, destination_database:str):
-- MAGIC   
-- MAGIC   source_data_dir = "data"
-- MAGIC   source_database = "jaffleshop"
-- MAGIC   source_layer = "raw"
-- MAGIC   source_partition = "*/*/*"
-- MAGIC   source_table = table_name
-- MAGIC   source_format = "csv"
-- MAGIC   source_options = {
-- MAGIC     "header": True,
-- MAGIC     "delimiter": ",",
-- MAGIC     "inferSchema":True,
-- MAGIC     "escape":"\'",
-- MAGIC     "quote":'"',
-- MAGIC   }
-- MAGIC   source_table_path = f"{source_root}{source_layer}/{source_database}/{source_data_dir}/{source_partition}/{source_table}_*_*_*"
-- MAGIC 
-- MAGIC   destination_data_dir = "delta"
-- MAGIC   destination_database = destination_database
-- MAGIC   destination_layer = None
-- MAGIC   destination_partition = None
-- MAGIC   destination_table = table_name
-- MAGIC   destination_format = "delta"
-- MAGIC   destination_mode = "overwrite"
-- MAGIC   destination_table_path = f"{destination_root}{destination_data_dir}/{destination_database}/{destination_layer}/{source_table}"
-- MAGIC   
-- MAGIC   if destination_layer:
-- MAGIC     destination_table = f"{destination_layer}_{destination_table}"
-- MAGIC 
-- MAGIC 
-- MAGIC   print(source_table_path)
-- MAGIC   print(destination_table_path)
-- MAGIC   options = {
-- MAGIC     "header": True,
-- MAGIC     "delimiter": ",",
-- MAGIC     "inferSchema":True,
-- MAGIC     "escape":"\'",
-- MAGIC     "quote":'"',
-- MAGIC   }
-- MAGIC 
-- MAGIC   df = (spark.read
-- MAGIC           .format(source_format)
-- MAGIC           .options(**source_options) 
-- MAGIC           .load(source_table_path))
-- MAGIC 
-- MAGIC   df.write.format(destination_format).mode(destination_mode).save(destination_table_path)
-- MAGIC 
-- MAGIC   sql = f"DROP TABLE IF EXISTS {destination_database}.{destination_table}"
-- MAGIC   print(sql)
-- MAGIC   spark.sql(sql)
-- MAGIC 
-- MAGIC   sql = f"""
-- MAGIC   CREATE TABLE IF NOT EXISTS {destination_database}.{destination_table} 
-- MAGIC   USING {destination_format} 
-- MAGIC   LOCATION '{destination_table_path}'
-- MAGIC   """
-- MAGIC   print(sql)
-- MAGIC   spark.sql(sql)
-- MAGIC 
-- MAGIC   sql = f"""SELECT COUNT(1) from {destination_database}.{destination_table}"""
-- MAGIC   row_count = spark.sql(sql).first()[0]
-- MAGIC   print(f"Loaded {row_count} rows into table {destination_database}.{destination_table}")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC root = get_storage_account()
-- MAGIC system = "jaffleshop"
-- MAGIC source_layer = "raw"
-- MAGIC period_partition = "2018/01/01"
-- MAGIC 
-- MAGIC path = f"{root}{source_layer}/{system}/data/{period_partition}"
-- MAGIC names = [t.name.split("_")[0] for t in dbutils.fs.ls(path)]
-- MAGIC 
-- MAGIC for n in names:
-- MAGIC   print(f"loading table {n}")
-- MAGIC   if (n=="payments"):
-- MAGIC     load_table("/", root, n, "stripe")
-- MAGIC   else:
-- MAGIC     load_table("/", root, n, "jaffle_shop")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.notebook.exit("succeeded")

-- COMMAND ----------

-- DBTITLE 1,Clean UP
-- MAGIC %python
-- MAGIC 
-- MAGIC source_root = get_storage_account()
-- MAGIC destination_root = "/"
-- MAGIC system = "jaffleshop"
-- MAGIC source_layer = "raw"
-- MAGIC period_partition = "2018/01/01"
-- MAGIC 
-- MAGIC 
-- MAGIC destination_data_dir = "delta"
-- MAGIC destination_database = "jaffle_shop"
-- MAGIC destination_layer = "raw"
-- MAGIC destination_database_path = f"{destination_root}{destination_data_dir}/{destination_database}"
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC path = f"{source_root}{source_layer}/{system}/data/{period_partition}"
-- MAGIC names = [t.name.split("_")[0] for t in dbutils.fs.ls(path)]
-- MAGIC 
-- MAGIC for n in names:
-- MAGIC   sql = f"""DROP TABLE IF EXISTS {destination_database}.{destination_layer}_{n}"""
-- MAGIC   print(sql)
-- MAGIC   spark.sql(sql)
-- MAGIC   
-- MAGIC print(f"delete data at path {destination_database_path}")
-- MAGIC dbutils.fs.rm(destination_database_path, True)
-- MAGIC   

-- COMMAND ----------

describe table extended jaffle_shop.dim_customers
