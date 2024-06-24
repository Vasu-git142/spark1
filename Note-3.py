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
# MAGIC ## fillna or na.fill

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

help(df.fillna)

# COMMAND ----------

df.fillna(30000).show()

# COMMAND ----------

display(df.fillna(value=32 , subset=['age']))

# COMMAND ----------

display(df.fillna(value=18000 ,subset =["bonus"]))

# COMMAND ----------

display(df.fillna({"age" : 27,"bonus" : 15000}))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### dropna or na.drop

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

simpleData = [("james","sales","NY",None,22,11000),
              ("john",None,"NY",12000,25,13000),
              (None,"finance",None,14000,27,15000),
              ("jaya","sales","Ny",None,29,None),
              ("vasu",None,"CA",14000,None,15000),
              (None,"engineering","NY",14000,27,15000),
              (None,None,None,None,None,None)
            ]
schema = ["name","designation","city","salary","age","bonus"]
df =spark.createDataFrame(data=simpleData,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.dropna(how="all"))

# COMMAND ----------

display(df.dropna(thresh=4))

# COMMAND ----------

display(df.na.drop(subset=["age", "name"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null With Replace

# COMMAND ----------

simpleData = [("james","sales","NY",None,22,11000),
              ("john",None,"NY",12000,25,13000),
              ("","finance",None,14000,27,15000),
              ("jaya","sales","Ny",None,29,None),
              ("vasu",None,"CA",14000,None,15000),
              ("","engineering","NY",14000,27,15000)
              
            ]
schema = ["name","designation","city","salary","age","bonus"]
df =spark.createDataFrame(data=simpleData,schema=schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.replace("","vasu").show()

# COMMAND ----------

display(df.replace(22,32))

# COMMAND ----------


