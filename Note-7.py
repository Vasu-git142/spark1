# Databricks notebook source
# MAGIC %md 
# MAGIC ### CSV file convert into parquet file
# MAGIC
# MAGIC 1) read the csv into a dataframe named df 
# MAGIC
# MAGIC 2)write the df into destination location as a parquet format
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV file into Delta table

# COMMAND ----------

df=spark.read.csv(" /FileStore/tables/csv/student.csv/student.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database school

# COMMAND ----------

# MAGIC %sql
# MAGIC use school

# COMMAND ----------

df.write.saveAsTable("student")

# COMMAND ----------

# MAGIC %md
# MAGIC ### parquet file into delta table

# COMMAND ----------

df1=df.write.parquet("/FileStore/tables/csv/out_put1")

# COMMAND ----------

df2=spark.read.parquet("/FileStore/tables/csv/out_put1")

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database scl

# COMMAND ----------

# MAGIC %sql
# MAGIC use scl

# COMMAND ----------

df2.write.saveAsTable("csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database data1

# COMMAND ----------

# MAGIC %md
# MAGIC ### parquet 
