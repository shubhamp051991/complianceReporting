# Databricks notebook source
# MAGIC %md
# MAGIC 1. Decryption would only work with the secret READ access and EXECUTE on the UDF function
# MAGIC 2. Asuming everyone will have access to the all_users_hr or the encrypted table

# COMMAND ----------

# dbutils.widgets.text("functionName","")
# dbutils.widgets.text("catalog","")
# dbutils.widgets.text("schema","")
# dbutils.widgets.text("table","")

catalog = dbutils.widgets.getArgument("catalog")
schema = dbutils.widgets.getArgument("schema")
table = dbutils.widgets.getArgument("table")
udfname = dbutils.widgets.getArgument("functionName")

# COMMAND ----------

# MAGIC %md validate if the user has access to read the encrypted data if not then add the user to the respective group

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog}.${schema}.${table}

# COMMAND ----------

# MAGIC %md user running decrypt UDF so that they can see actual values of ssn and zip code

# COMMAND ----------

# MAGIC %sql
# MAGIC select name,${catalog}.${schema}.${udfname}(ssn) as ssn,${catalog}.${schema}.${udfname}(zipcode) as zip from ${catalog}.${schema}.${table}