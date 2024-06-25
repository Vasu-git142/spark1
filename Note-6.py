# Databricks notebook source
# MAGIC %md
# MAGIC ## Raw data to table

# COMMAND ----------

df=spark.read.json("/FileStore/tables/json/sample_json.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.drop("friuit")

# COMMAND ----------

display(df1)

# COMMAND ----------

df2=df1.fillna(value="mango",subset=["fruit"])

# COMMAND ----------

display(df2)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

display(df2.withColumn("current_time", current_timestamp()))

# COMMAND ----------


