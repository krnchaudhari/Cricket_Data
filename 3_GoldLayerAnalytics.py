# Databricks notebook source
# DBTITLE 1,Importing libraries
from pyspark.sql.functions import *


# COMMAND ----------

# DBTITLE 1,Read Silver Layer
silver_df = spark.table("workspace.default.cricket_silver_current_matches")

display(silver_df)


# COMMAND ----------

# DBTITLE 1,Match Type Distribution
gold_matchtype_df = silver_df.groupBy("match_type").agg(count("*").alias("total_matches"))

display(gold_matchtype_df) 

# COMMAND ----------

# DBTITLE 1,Venue wise Match Count
gold_venue_df = silver_df.groupBy("venue").agg(count("*").alias("total_matches"))

display(gold_venue_df) 

# COMMAND ----------

# DBTITLE 1,Team wise Match Count
team1_df=silver_df.select(col("team_1").alias("team"))
team2_df=silver_df.select(col("team_2").alias("team"))

allteam_df = team1_df.union(team2_df)

gold_team_df = allteam_df.groupBy("team").agg(count("*").alias("matches_played"))
display(gold_team_df)


# COMMAND ----------

# DBTITLE 1,Final Analytics Queries
#Match_overview

display(spark.sql("""select count(*) as total_matches,
count(distinct match_type) as total_match_types,
count(distinct venue) as total_venues
from workspace.default.cricket_silver_current_matches"""))

