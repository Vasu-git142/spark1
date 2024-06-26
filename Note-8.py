# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/
# MAGIC

# COMMAND ----------

input="dbfs:/mnt/demo-datasets/DE-Pro/bookstore/country_lookup/country_lookup.json"  

# COMMAND ----------

output= "dbfs:/mnt/demo-datasets/DE-Pro/bookstore/kafka-raw/01.json"

# COMMAND ----------

print(input)
print(output)

# COMMAND ----------


