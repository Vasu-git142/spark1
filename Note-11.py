# Databricks notebook source
# MAGIC %sql
# MAGIC create database deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC use deltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC create table people(
# MAGIC   id INT,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   salary INT
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from people

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended people

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people values(1,"vasu","d","f",23,20000)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from people

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table people
# MAGIC

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

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO people4 VALUES (1, 'vasu', 'd', 'f',20000)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table people4

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into people4 values(1,"vasu","d","f",20000)

# COMMAND ----------


