# Databricks notebook source
# MAGIC %md
# MAGIC ### Extract ,Transform, Load

# COMMAND ----------

# MAGIC %run "./Note-8"

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.option("multiline", True).json("dbfs:/FileStore/json/cricket.json")

# COMMAND ----------

df =spark.read.csv("multiline",True).json(f"(input)cricket.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumn("topping",explode("topping")).display()

# COMMAND ----------

df1= df.withColumn("topping_id", col("topping.id")).withColumn("topping_type",col("topping.type"))
display(df1)

# COMMAND ----------

### it merge the batters and batter
df2=df1.withColumn("batters",explode("batters.batter"))
display(df2)


# COMMAND ----------

df3 = df2.withColumn("batters_id", col("batters.id"))\
.withColumn("batters_type",col("batters.type"))
display(df3)

# COMMAND ----------

df_final=df3.write.mode("overwrite").parquet("dbfs:/mnt/demo-datasets/output")


# COMMAND ----------

df_final=spark.read.parquet("dbfs:/mnt/demo-datasets/output")
display(df_final)

# COMMAND ----------


