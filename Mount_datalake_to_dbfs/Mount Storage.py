# Databricks notebook source
# MAGIC %md ###Replace Values 
# MAGIC <br/>
# MAGIC
# MAGIC 1. Replace Client Id, Client Secret, Tenant Id values in configs <br/>
# MAGIC 2. Replace container name and data lake name in mount function
# MAGIC
# MAGIC <i>Note: Make sure to remove 3 stars before and after values

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "03f56f06-c81a-404a-9c73-35ba7f0a2400",
           "fs.azure.account.oauth2.client.secret": " jJjjpG2+WUEH4O4HByBolcUJIz0qIcSpOmhgSyWHDpQ=",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/71f286f8-2991-4ba0-9861-5c563588f72f/oauth2/token"}

# Mount the Data Lake Gen2 account
dbutils.fs.mount(
  source = "abfss://taxidata@taxidatalake01kenne.dfs.core.windows.net/",
  mount_point = "/mnt/datalake",
  extra_configs = configs)
