# Databricks notebook source
# MAGIC %md
# MAGIC ## list the filesystem

# COMMAND ----------

# MAGIC %fs ls
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights/departuredelays.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/flights/departuredelays.csv

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/flights/departuredelays.csv").show()

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/flights/departuredelays.csv")
display(df)                                           
df.printSchema

# COMMAND ----------



# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.select("delay","distance"))

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

display(df.select(col("delay"),"distance"))

# COMMAND ----------

display(df.select(col("distance").alias("dist")))

# COMMAND ----------

display(df.tail(50))

# COMMAND ----------

display(df.take(30))

# COMMAND ----------

display(df.describe())

# COMMAND ----------

display(df.describe(['delay']))

# COMMAND ----------

help(df.describe)

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### WithColumn

# COMMAND ----------

display(df.withColumn("distance",df.distance/60))

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

display(df.withColumn("distance/60",col("distance")/60))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### withColumnRename or Alias

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

display(df.withColumnRenamed("delay","late"))

# COMMAND ----------


