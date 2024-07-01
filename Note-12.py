# Databricks notebook source
# MAGIC %md
# MAGIC ##Internal delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC create database lake

# COMMAND ----------

# MAGIC %sql
# MAGIC use lake

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table  people_1(
# MAGIC   id INT,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   salary INT
# MAGIC ) location "dbfs:/user/hive/warehouse/external"

# COMMAND ----------


