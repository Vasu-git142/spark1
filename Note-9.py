# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract ,Transform, Load

# COMMAND ----------

# MAGIC %run "./Note-8"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.option("multiline", True).json("dbfs:/FileStore/json/cricket.json")

# COMMAND ----------

df =spark.read.csv("multiline",True).json(f"(input)cricket.json")

# COMMAND ----------

display(df)

# COMMAND ----------


