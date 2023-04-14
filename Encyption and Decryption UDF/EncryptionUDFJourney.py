# Databricks notebook source
# dbutils.widgets.text("catalog","")
# dbutils.widgets.text("schema","")
# dbutils.widgets.text("table","")
# dbutils.widgets.text("scope","")
# dbutils.widgets.text("key","")
# dbutils.widgets.text("functionName","")

catalog = dbutils.widgets.getArgument("catalog")
schema = dbutils.widgets.getArgument("schema")
table = dbutils.widgets.getArgument("table")
scope = dbutils.widgets.getArgument("scope")
key = dbutils.widgets.getArgument("key")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Encyption UDF Jounney
# MAGIC 1. Identify the dataset owned by the Data Owner and Encrypt it for the downstream users
# MAGIC 2. Hypothetical Scenario -> let's create a very simple delta table and assumed this table is part of the ETL framework and stored in the adls location accessible by production systems only

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists ${catalog};
# MAGIC use CATALOG ${catalog};
# MAGIC create schema if not exists ${schema};
# MAGIC create table if not exists ${schema}.${table} (name string,ssn string,zipcode string);
# MAGIC insert into ${schema}.${table} values ("shubham","897-987-7654","98012-67654"),("sam","765-000-8711","06119-89765");

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Register an Encyption UDF using a secret Key owned by the Data Owner
# MAGIC 2. Secret registration is done using the Databricks CLI -> please follow steps here -> https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes
# MAGIC 3. Scope -> encryptionkeys and key -> ssn

# COMMAND ----------

# MAGIC %sql
# MAGIC -- maintain permissions on this UDF to only the ingestion pipeline which will write encypted data to the delta table
# MAGIC drop function if exists ${catalog}.${schema}.${functionName};
# MAGIC create function ${catalog}.${schema}.encryptUDF_scheme1(value string)
# MAGIC RETURNS STRING
# MAGIC RETURN base64(aes_encrypt(value,secret(${scope},${key})));

# COMMAND ----------

# MAGIC %md
# MAGIC See the function definition and who created this

# COMMAND ----------

# MAGIC %sql
# MAGIC select routine_catalog,routine_Schema,routine_name,routine_owner,routine_definition,created,created_by,last_altered_by from system.information_schema.routines
# MAGIC where routine_name like "%encrypt%"

# COMMAND ----------

# MAGIC %md
# MAGIC change the owner of this function to the specific Data Owners -> currently changing to admind

# COMMAND ----------

# MAGIC %sql
# MAGIC Alter function ${catalog}.${schema}.encryptUDF_scheme1 owner to `shubham.pachori@databricks.com`

# COMMAND ----------

# MAGIC %md encrypt the sensitive columns using the specific schem and save it in delta table for downstream users
# MAGIC 1. Encrypted data should exist in the seperate schema as per the data category, for simplicity am storing it in the same schema

# COMMAND ----------

# writing data to the delta table as Encrypted
spark.sql(f"""
select name,{catalog}.{schema}.{functionName}(ssn) as ssn, {catalog}.{schema}.{functionName}(zipcode) as zipcode 
from {schema}.{table}
""").write.format("delta").saveAsTable(f"{catalog}.{schema}.{table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog}.${schema}.${table}

# COMMAND ----------

# MAGIC %md creating the decrypted UDF as well so that downstream users can access it

# COMMAND ----------

# MAGIC %sql
# MAGIC drop function if exists ${catalog}.${schema}.decryptUDF_scheme1;
# MAGIC create function ${catalog}.${schema}.decryptUDF_scheme1(value string)
# MAGIC RETURNS STRING
# MAGIC RETURN cast(aes_decrypt(unbase64(value),secret(${scope},${key})) as string);