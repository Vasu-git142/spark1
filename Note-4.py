# Databricks notebook source
# MAGIC %md
# MAGIC ### File Formats
# MAGIC 1)Row based - csv,tsv,avro
# MAGIC it is a human readable format
# MAGIC
# MAGIC 2)Columnar - parquet,delta,ORC
# MAGIC it is not human readable format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and write the CSV file

# COMMAND ----------

df= spark.read.option("header", True).option("inferSchema", True).csv("dbfs:/FileStore/csv/business.csv")

# COMMAND ----------

display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### change column names 
# MAGIC
# MAGIC ##add a new column(F1 Data) 
# MAGIC

# COMMAND ----------

display(df.withColumnRenamed("Industry_code_NZSIOC","code_NZSIOC"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concat the Columns

# COMMAND ----------

df1=df.withColumn("variable_name&variable_category", concat("variable_name",lit (" - "), "variable_category"))

# COMMAND ----------

display(df1)

# COMMAND ----------

display(df1.drop("variable_name","variable_category"))

# COMMAND ----------

display(df1.withColumn("chemical_name",lit("HCL")).withColumn("current_time",current_timestamp()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the csv file

# COMMAND ----------

df1.write.csv("dbfs:/Filterstore/csv/df1/output")

# COMMAND ----------

dfcheck= spark.read.csv("dbfs:/Filterstore/csv/df1/output/part-00000-tid-7709896595430972088-d65271f1-f335-481d-928f-d3ddef49890d-138-1-c000.csv")

# COMMAND ----------

display(dfcheck)

# COMMAND ----------

df.write.option("header", True).mode("overwrite").csv("dbfs:/FileStore/csv/output")

# COMMAND ----------

dfcheck=spark.read.csv("/FileStore/csv/output")

# COMMAND ----------

display(dfcheck)

# COMMAND ----------


