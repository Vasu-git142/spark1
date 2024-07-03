# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create table d_b.stu(
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   age INT,
# MAGIC   gender STRING,
# MAGIC   course STRING,
# MAGIC   grade INT,
# MAGIC   ingestiondate TIMESTAMP
# MAGIC )

# COMMAND ----------

d_b.stu.write.saveAsTable("stu")

# COMMAND ----------

stu1=stu.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

display(stu1)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database d_b

# COMMAND ----------

stu1.write.saveAsTable("db.student_managed")

# COMMAND ----------


