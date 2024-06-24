# Databricks notebook source
df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/flights/departuredelays.csv")
display(df)  

# COMMAND ----------

display(df.sort("destination"))

# COMMAND ----------

display(df.sort("date",ascending=False))

# COMMAND ----------

display(df.sort("date",ascending=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Groupby or aggregation 

# COMMAND ----------

simpleData = [("james","sales","NY",10000,22,11000),
              ("john","marketing","NY",12000,25,13000),
              ("joe","finance","CA",14000,27,15000),
              ("jaya","sales","Ny",16000,29,17000),
              ("vasu","doctor","CA",14000,27,15000),
              ("kumar","engineering","NY",14000,27,15000),
              ("jerry","finance","CA",18000,29,20000)
            ]
schema = ["name","designation","city","salary","age","bonus"]
df =spark.createDataFrame(data=simpleData,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

 display(df.groupBy("designation").sum("salary"))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

 display(df.groupBy("designation").agg(sum("salary").alias("sum_salary")))

# COMMAND ----------

 display(df.groupBy("designation").agg(sum("salary").alias("sum_salary"),avg("salary").alias("avg_salary")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation : it will changes the dataframe
# MAGIC 1)Narrow transformation
# MAGIC 2)Wide transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## fillna or na.fil

# COMMAND ----------

simpleData = [("james","sales",10000,22,None),
              ("john","marketing",None,25,13000),
              ("joe","finance",None,27,None),
              ("jaya","sales",16000,None,17000),
              ("vasu","doctor",14000,27,None),
              ("kumar","engineering",14000,None,15000),
              ("jerry","finance",None,None,None)
            ]
columns=["Employee","department","salary","age","bonus"]
df = spark.createDataFrame(simpleData,columns)
display(df)
df.printSchema()

# COMMAND ----------


