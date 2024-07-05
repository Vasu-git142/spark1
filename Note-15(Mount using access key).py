# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount using access key
# MAGIC 1)Access key
# MAGIC 2)service principal

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Key

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"<conf-key>":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source="wasbs://input@databricksproject1.blob.core.windows.net",
  mount_point="/mnt/input1/sample",
  extra_configs={"fs.azure.account.key.databricksproject1.blob.core.windows.net": "mnnc9hP1CU4GQe2mTy/ZayHqMTq+ZzKFy2utPttNInIqDD6IBCnzvq6G/s8lirAnqcMOO6eyJdvT+ASt1CX9lA=="}
)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/input1/sample/
# MAGIC

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True
                                           ).csv("dbfs:/mnt/input1/sample/dataset.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/input1/sample/parquet")

# COMMAND ----------


