# Databricks notebook source
# MAGIC %md
# MAGIC ##views
# MAGIC 1)standard view
# MAGIC 2)temp view
# MAGIC 3)Global view

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/bikeSharing/data-001/hour.csv

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/bikeSharing/data-001/hour.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.saveAsTable("bikes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikes

# COMMAND ----------


