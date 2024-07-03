# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

stu=spark.read.option("header",True).option("inferSchema",True).csv("/FileStore/tables/csv/school.csv")

# COMMAND ----------

display(stu)

# COMMAND ----------

stu1=stu.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(stu1)

# COMMAND ----------


