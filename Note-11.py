# Databricks notebook source
# MAGIC %sql
# MAGIC create database deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC use deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC create table people3(
# MAGIC   id INT,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   salary INT
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from people3

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended people3

# COMMAND ----------

# MAGIC %md
# MAGIC ###external table we specify the location

# COMMAND ----------

# MAGIC %sql
# MAGIC create table people4(
# MAGIC   id INT,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT
# MAGIC ) LOCATION "dbfs:/user/hive/warehouse/externaldb"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended people4

# COMMAND ----------


