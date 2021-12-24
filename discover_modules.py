from pyspark.sql import SparkSession
import sys, os

def go(spark: SparkSession):
  spark.conf.set("spark.databricks.userInfoFunctions.enabled", True)
  username = spark.sql("select current_user()").first()[0]
  repo = "databricks-utilities"
  root_path = os.path.abspath(f"/Workspace/Repos/{username}/{repo}")
  sys.path.append(root_path)