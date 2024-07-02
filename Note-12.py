# Databricks notebook source
# MAGIC %md
# MAGIC ##Internal delta lake

# COMMAND ----------

# MAGIC %sql
# MAGIC create database lake1

# COMMAND ----------

# MAGIC %sql
# MAGIC use lake1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table  lake1.people(
# MAGIC   id INT,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   gender STRING,
# MAGIC   age INT,
# MAGIC   salary INT
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended lake1.people

# COMMAND ----------

# MAGIC %md
# MAGIC parquet for every operation
# MAGIC ### Log file
# MAGIC 1.Json Transcation Log file
# MAGIC
# MAGIC 2.CRC -- Cyclic Redundant Check
# MAGIC
# MAGIC 3.Parquet Checkpoint File

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO  lake1.people (id, firstname, lastname, gender, age, salary) VALUES
# MAGIC (1, 'sachin', 'd', 'M', 22, 18000),
# MAGIC (2, 'dhoni', 'a', 'M', 32, 19000),
# MAGIC (3, 'divi', 'c', 'F', 25, 20000),
# MAGIC (4, 'joe', 'm', 'M', 27, 16000),
# MAGIC (5, 'john', 'g', 'M', 29, 13000),
# MAGIC (6, 'vasu', 't', 'F', 30, 22000);
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from lake1.people

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/lake1.db/
# MAGIC

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/00000000000000000000.json")

# COMMAND ----------

dbutils.fs.head("dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/00000000000000000001.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history lake1.people

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lake1.people (id, firstname, lastname, gender, age, salary) VALUES
# MAGIC (7, 'jaya', 'v', 'M', 24, 20000),(8,'lake','v','F',23,22000);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lake1.people

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history lake1.people

# COMMAND ----------

# MAGIC %sql
# MAGIC update lake1.people
# MAGIC set  salary=salary+6000
# MAGIC where id=7

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lake1.people

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history lake1.people

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from lake1.people where id=8

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lake1.people

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history lake1.people

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from lake1.people where id=7

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/

# COMMAND ----------

# MAGIC %sql
# MAGIC update lake1.people
# MAGIC set  salary=salary+9000
# MAGIC where id=4

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lake1.people

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/lake1.db/people/_delta_log/

# COMMAND ----------


