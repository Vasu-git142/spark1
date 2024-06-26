# Databricks notebook source
# MAGIC %md
# MAGIC ### End to End stream

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/Streamdata/input/march.csv")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating user define schema

# COMMAND ----------

from pyspark.sql.types import *


# COMMAND ----------

users=StructType([StructField("Name",StringType()),StructField("Age",StringType()),StructField("City",StringType())])


# COMMAND ----------

df1=spark.readStream.schema(users).option("header",True).csv("dbfs:/FileStore/tables/Streamdata/input")
display(df1)

# COMMAND ----------

df1.writeStream.option("format","parquet").outputMode("append").option("checkpointLocation","/FileStore/tables/Streamdata/checkpoint").option("path","/FileStore/tables/Streamdata/output").start()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verfying o/p stream

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dfnew=spark.read.format("delta").load("/FileStore/tables/Streamdata/output")
display(dfnew)

# COMMAND ----------


