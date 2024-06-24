# Databricks notebook source
# MAGIC %md
# MAGIC ### Count ,Limit

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/databricks-datasets/flights/departuredelays.csv")
display(df)  

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

help(df.count)

# COMMAND ----------

display(df.count())

# COMMAND ----------

len(df.columns)

# COMMAND ----------

help(df.limit)

# COMMAND ----------

display(df.limit(14))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter

# COMMAND ----------

help(df.filter)

# COMMAND ----------

display(df.filter(df.distance<500))

# COMMAND ----------

display(df.filter(df.delay>100))

# COMMAND ----------

display(df.where(col("date") == 1061800))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering null values

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

import datetime
users = [
    {
        "id": 1,
        "name": "John",
        "gender": "male",
        "city" : None,
        "courses" : [3,2],
        "is_customer": True,
        "amount" : 1000.30,
        "customer_from" : datetime.date(2022, 3, 1),
        "last_update" : datetime.datetime(2022, 3, 1, 10, 30)
    },
    {
        "id": 2,
        "name": "sue",
        "gender": "male",
        "city" : "USA",
        "courses" : None,
        "is_customer": True,
        "amount" : None,
        "customer_from" : None,
        "last_update" : datetime.datetime(2022, 3, 1, 10, 30)
    },
    {
        "id": 3,
        "name": None,
        "gender": "male",
        "city" : None,
        "courses" : [3,2],
        "is_customer": None,
        "amount" : 1000.30,
        "customer_from" : datetime.date(2022, 3, 1),
        "last_update" : None
    },
    {
        "id": 4,
        "name": "fing",
        "gender": None,
        "city" : "New York",
        "courses" : None,
        "is_customer": True,
        "amount" : None,
        "customer_from" : None,
        "last_update" : datetime.datetime(2022, 3, 1, 10, 30)
    }
]
display(users)

# COMMAND ----------

df = spark.createDataFrame(users)

# COMMAND ----------

display(df.printSchema)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

display(users_df)

# COMMAND ----------

display(users_df.select("city").filter(col("city").isNull()))

# COMMAND ----------

display(users_df.select("courses").filter(col("courses").isNotNull()))

# COMMAND ----------

display(df.drop("amount"))

# COMMAND ----------


