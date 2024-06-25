# Databricks notebook source
# MAGIC %md
# MAGIC ### Read the Json file

# COMMAND ----------


from pyspark.sql.types import *
schema = StructType([StructField("fruit", StringType()),StructField("size", StringType()),StructField("color", StringType())])


# COMMAND ----------

df=spark.read.schema(schema).json("/FileStore/tables/json/sample_json.json").cache()

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1=df.fillna(value ="mango",subset =["fruit"])

# COMMAND ----------

display(df1)

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

df2 = df1.withColumn("current_time", current_timestamp()).withColumn("current_timezone", current_timezone())

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the json file

# COMMAND ----------

df2.write.json("/FileStore/tables/json/output2")

# COMMAND ----------

df.write.parquet("/FileStore/tables/parquet/outputparquet")

# COMMAND ----------

df.write.parquet("/FileStore/tables/json/outputparquet2")

# COMMAND ----------

df.write.saveAsTable("fruit")

# COMMAND ----------


