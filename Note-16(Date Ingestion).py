# Databricks notebook source
Ingesting the data into your lakehouse
1) Auto Loader(streaming)
2) COPY INTO (SQL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AUTO LOADER

# COMMAND ----------

## Syntax of Auto loader
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format",<source_format>)
.load(<path>)
.writeStream
 .option("checkpointLocation", <checkpoint_directory>)
 .table(<table_name>)
