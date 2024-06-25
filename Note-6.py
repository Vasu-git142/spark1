# Databricks notebook source
# MAGIC %md
# MAGIC ## Json file convert into parquet file

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

df3=df2.withColumn("current_time", current_timestamp())

# COMMAND ----------

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  write DF in parquet

# COMMAND ----------

df3.write.parquet("dbfs:/FileStore/tables/json/out_put1")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  verify your parquet file

# COMMAND ----------

df4=spark.read.parquet("dbfs:/FileStore/tables/json/out_put1")

# COMMAND ----------

display(df4)

# COMMAND ----------


