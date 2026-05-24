# Databricks notebook source
# DBTITLE 1,Import  the required Library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create catalog schema and volume
spark.sql("CREATE CATAlOG IF NOT EXISTS workspace")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.default")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project")

base_path = "/Volumes/workspace/default/cricket_api_project"

# COMMAND ----------

# DBTITLE 1,Calling Cricket API
API_KEY = 'e3d54ae9-377b-4558-b83a-f39970992ef4'
api_url = f"https://api.cricapi.com/v1/countries?apikey=e3d54ae9-377b-4558-b83a-f39970992ef4&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent=2)[:2000])              

# COMMAND ----------

# DBTITLE 1,Save raw API response in the volumes
raw_file_path = f'{base_path}/current_matches_raw.json'

with open(raw_file_path, 'w') as file:
    json.dump(api_data, file)

print("Raw API data is save at the :", raw_file_path)

# COMMAND ----------

# DBTITLE 1,Create Bronze layes Dataframe/table
bronze_data=[{
    "source_api" : api_url,
    "raw_json" : json.dumps(api_data),
    "ingestion_time" : None
}]

bronze_schema  = StructType([
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])

bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)\
    .withColumn("ingestion_time", current_timestamp())

display(bronze_df)


# COMMAND ----------

# DBTITLE 1,Save the Bronze Table
bronze_df.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable("workspace.default.Cricket_bronze_current_matches")

print("Bronze table is created successfully")


# COMMAND ----------

# DBTITLE 1,Query Bronze Table
# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.cricket_bronze_current_matches
