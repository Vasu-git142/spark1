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
# MAGIC select * from bikes where season = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC create view bikeseason3 as select * from bikes where season = 3

# COMMAND ----------

# MAGIC %md
# MAGIC Standard View

# COMMAND ----------

# MAGIC %sql
# MAGIC create view bikehours as select * from bikes where hr = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Temp View or Dataframe view

# COMMAND ----------

df.createOrReplaceTempView("bikesview")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikesview

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view  bikeview2 as  select * from bikes where dteday='2011-01-08'

# COMMAND ----------

# MAGIC %md
# MAGIC Global View

# COMMAND ----------

df.createGlobalTempView("globalview")

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from global_temp.globalview

# COMMAND ----------


