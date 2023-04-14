# Databricks notebook source
# MAGIC %md
# MAGIC Input the final delta table name using the widgets

# COMMAND ----------


# dbutils.widgets.text("catalogName","")
# dbutils.widgets.text("schemaName","")
# dbutils.widgets.text("tableName","")
# dbutils.widgets.text("userName","")
# dbutils.widgets.text("token","")
# dbutils.widgets.text("workspace_url","")
catalogName = dbutils.widgets.getArgument("catalogName")
schemaName = dbutils.widgets.getArgument("schemaName")
tableName = dbutils.widgets.getArgument("tableName")
userName = dbutils.widgets.getArgument("userName")
token = dbutils.widgets.getArgument("token")
workspace_url = dbutils.widgets.getArgument("workspace_url")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE if exists ${catalogName}.${schemaName}.${tableName};
# MAGIC create table if not exists ${catalogName}.${schemaName}.${tableName} (identifier STRING,type STRING,tags ARRAY<String>,lastCheckedAt TIMESTAMP);
# MAGIC describe ${catalogName}.${schemaName}.${tableName};

# COMMAND ----------

import json
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

from pyspark.sql.functions import current_timestamp,explode,col,when,lit,split,regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import concurrent.futures
import multiprocessing

workspace_url = f"{workspace_url}" # workspace URL
endpoint = "api/2.1/unity-catalog/securable-tags"
un = f"{userName}" # username
pw = f"{token}" # token

# COMMAND ----------

# MAGIC %md
# MAGIC List Catalogs
# MAGIC 1. endpoint : /api/2.1/unity-catalog/catalogs/securable-tags

# COMMAND ----------

def addCatalogTags(catalogList:list) -> None:
  # looping over all catalogs present in the argument catalogList to find their Tags
  allCatalogTags = []
  for catalogName in catalogList:
    host = f" https://{workspace_url}/{endpoint}/CATALOG/{catalogName} "
    req = requests.get(host,auth=HTTPBasicAuth(un, pw))
    
    # checking if API call is successfull
    if req.status_code == 200:
      data = req.text
      jsonData = json.loads(data)

      # verify if Tags exist for the entity
      if "tags" in jsonData["tag_assignments"][0]:
        tags = jsonData["tag_assignments"][0]['tags']
        newCatalog = {"name":catalogName,"tags":tags}
        
        ## adding a new catalog tags
        allCatalogTags.append(newCatalog)
        
    else:
      print("API not responding",req.text)
  # Define the schema for the DataFrame
  schema = StructType([
      StructField("name", StringType(), True),
      StructField("tags", ArrayType(StringType()), True)
  ])

  # Create the DataFrame
  df = spark.createDataFrame(allCatalogTags, schema).withColumn("lastCheckedAt",current_timestamp())

  return df

# COMMAND ----------

def getAllCatalogs() -> list:
  allCatalogs = []
  host = f'https://{workspace_url}/api/2.1/unity-catalog/catalogs'
  req = requests.get(host,auth=HTTPBasicAuth(un, pw))
  if req.status_code == 200:
    data = req.text
    jsonData = json.loads(data)
    for catalogs in jsonData['catalogs']:
      if catalogs["catalog_type"] != "DELTASHARING_CATALOG":
        # local = {'catalogsName':catalogs['name'],'catalogOwner':catalogs['owner'],"type":catalogs['catalog_type'],'createdAt':catalogs['created_at']}
        allCatalogs.append(catalogs['name'])
  else:
    return req.text
  return allCatalogs

def getAllSchema(catalogName:str) -> list:
  allSchemas = []
  endpoint = "/api/2.1/unity-catalog/schemas"
  parameters = {"catalog_name":catalogName}
  host = f" https://{workspace_url}/api/2.1/unity-catalog/schemas"
  req = requests.get(host,auth=HTTPBasicAuth(un, pw),params = parameters)
  if req.status_code == 200:
        data = req.text
        jsonData = json.loads(data)['schemas']
        for schema in jsonData:
          allSchemas.append(schema['name'])
  return allSchemas

def getAllTables(catalogName:str,schemaName:str) -> list:
  allTables = []
  parameters = {"catalog_name":catalogName,"schema_name":schemaName}
  host = f" https://{workspace_url}/api/2.1/unity-catalog/tables"
  req = requests.get(host,auth=HTTPBasicAuth(un, pw),params = parameters)
  if req.status_code == 200:
        data = req.text
        jsonData = json.loads(data)
        
        if 'tables' in jsonData:
          for schema in jsonData['tables']:
            allTables.append(schema['name'])
            
  return allTables

# COMMAND ----------

def addSchemaTags(catalogList:list):
  allSchemaTags = []
  for catalogName in catalogList:
    db_local = getAllSchema(catalogName)
    for dbName in db_local:
      host = f"https://{workspace_url}/{endpoint}/SCHEMA/{catalogName}.{dbName} "
      req = requests.get(host,auth=HTTPBasicAuth(un, pw))
      # checking if API call is successfull
      if req.status_code == 200:
        data = req.text
        jsonData = json.loads(data)

        # verify if Tags exist for the entity
        if "tags" in jsonData["tag_assignments"][0]:
          tags = jsonData["tag_assignments"][0]['tags']
          newSchema = {"name":f"{catalogName}.{dbName}","tags":tags}
          
          ## adding a new catalog tags
          allSchemaTags.append(newSchema)
          
    
    # adding schema tags to the dataframe
    # Define the schema for the DataFrame
  schema = StructType([
      StructField("name", StringType(), True),
      StructField("tags", ArrayType(StringType()), True)
  ])

  # Create the DataFrame
  df = spark.createDataFrame(allSchemaTags, schema).withColumn("lastCheckedAt",current_timestamp())

  return df

# COMMAND ----------

def getTableTags(catalogName:str):
  
  newSchema = {}
  db_local = getAllSchema(catalogName)
  for dbName in db_local:
    tbl_local = getAllTables(catalogName,dbName)
    if len(tbl_local) > 0:
        for tblName in tbl_local:
          host = f" https://{workspace_url}/{endpoint}/TABLE/{catalogName}.{dbName}.{tblName} "
          req = requests.get(host,auth=HTTPBasicAuth(un, pw))
          # checking if API call is successfull
          if req.status_code == 200:
            data = req.text
            jsonData = json.loads(data)

            # verify if Tags exist for the entity
            if "tags" in jsonData["tag_assignments"][0]:
              tags = jsonData["tag_assignments"][0]['tags']
              newSchema = {"name":f"{catalogName}.{dbName}.{tblName}","tags":tags}
              

  return newSchema

# COMMAND ----------

def addTableTags(catalogList:list):
  # define a list to hold all returns 
  allTableTags = []

  with concurrent.futures.ThreadPoolExecutor() as executor:
    results = executor.map(getTableTags,catalogList)

    for res in results:
      if len(res.keys()) > 0:
        allTableTags.append(res)

    # adding schema tags to the dataframe
  # Define the schema for the DataFrame
  schema = StructType([
      StructField("name", StringType(), True),
      StructField("tags", ArrayType(StringType()), True)
  ])

  # Create the DataFrame
  df = spark.createDataFrame(allTableTags, schema).withColumn("lastCheckedAt",current_timestamp())
  return df

# COMMAND ----------

# MAGIC %md Calling all Unity assets Catalogs, Schema and Tables to get their tags

# COMMAND ----------

allCatalogs = getAllCatalogs()
catalogs_DF = addCatalogTags(allCatalogs)
schema_DF = addSchemaTags(allCatalogs)
table_DF = addTableTags(allCatalogs)

# unioning all the identifiers
data_DF = catalogs_DF.union(schema_DF).union(table_DF)
display(data_DF)

# COMMAND ----------

# adding some helper columns in my main dataframe

from pyspark.sql.functions import regexp_extract

df = data_DF.withColumn('catalog', regexp_extract('name', '^([^\.]+)', 1))
df = df.withColumn('schema', regexp_extract('name', '^([^\.]+)\.([^\.]+)', 2))
df = df.withColumn('table', regexp_extract('name', '^([^\.]+)\.[^\.]+\.([^\.]+)', 2))

# Add a new column to determine the pattern
df = df.withColumn('type', 
                   when(col('table') != '', lit("Table"))
                   .when(col('schema') != '',lit("Schema"))
                   .otherwise('Catalog')).drop(*["catalog","schema","table"]).select(col("name").alias("identifier"),col("type"),col("tags"),col("lastCheckedAt"))

df.createOrReplaceTempView("tagsinfo_local")

# COMMAND ----------

# MAGIC %md
# MAGIC Merge into final delta table on identifier(either Catalog/Schema or Table) and when update is being fetched and merge the final update to the delta table, then this delta table will be used to generate discovery insights

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into ${catalogName}.${schemaName}.${tableName} as master
# MAGIC USING tagsinfo_local as local
# MAGIC ON master.identifier = local.identifier
# MAGIC WHEN MATCHED and local.lastCheckedAt >= master.lastCheckedAt THEN update set master.tags = local.tags
# MAGIC when NOT MATCHED THEN INSERT *