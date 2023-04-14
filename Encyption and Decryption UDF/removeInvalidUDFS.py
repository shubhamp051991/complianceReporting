# Databricks notebook source
catalogName = dbutils.widgets.getArgument("catalogName")
schemaName = dbutils.widgets.getArgument("schemaName")
tableName = dbutils.widgets.getArgument("tableName")

# COMMAND ----------

# MAGIC %md all the configs down below

# COMMAND ----------

myValidFunctionList = ["aes_encrypt", "aes_decrypt"]
myInvalidFunctionName = ["hash", "mask", "md5", "sha", "sha1", "sha2", "crc32"]
myValidScopesKeys = {"scope1":["key1"],"scope2":["key2"],"scope3":["key3"],"encryptionkeys":["ssn"]}

# COMMAND ----------

from pyspark.sql.functions import udf,col,when,lit,concat_ws,current_timestamp
from pyspark.sql.types import StringType
import re

# COMMAND ----------

def validFunctions(routinedef : str,invalidFunctions : list = myInvalidFunctionName) -> str:
  regex = r"\b(" + "|".join(invalidFunctions) + r")\b"
  # Find all occurrences of the ["hash","mask","md5","sha","sha1","sha2","crc32"] invalid functions
  matches = re.findall(regex, routinedef)

  if (len(matches) > 0):
    return "invalidFunction"
  return "validFunction"

validFunctions_udf = udf(validFunctions,StringType())

def findValidSecret(routinesdef : str, validScopes:dict = myValidScopesKeys) -> str:
  pattern = r"secret\((['\"])(\w+)\1,\s*(['\"])(\w+)\3\)"
  # Search for the pattern in the text and extract the captured groups
  match = re.search(pattern, routinesdef)

  if match:
    # Extract the words before and after the comma
      word1 = match.group(2)
      word2 = match.group(4)
      validKeys = validScopes.keys()
      if word1 in validKeys and word2 in validScopes[word1]:
        return "validSecret"
  return "invalidSecret"

  
findValidSecret_udf = udf(findValidSecret,StringType())

# COMMAND ----------

# DBTITLE 1,Valid or Invalid Functions
# MAGIC %md
# MAGIC The system table routines which is the observability layer of the Unity Catalog, captures every function or UDF defined across all catalogs, we use this table to tag if defined function is following the norms or not

# COMMAND ----------

# MAGIC %md
# MAGIC Extracting function information which has either any masking or sha functions or specific function for encryption or decryption

# COMMAND ----------

data = spark.sql(f"""
select routine_catalog,routine_schema,routine_name,routine_owner,routine_definition,created_by,last_altered_by from system.information_schema.routines
where routine_definition is not null""")

display(data)

# COMMAND ----------

data = (data
.withColumn("isFunctionValid",validFunctions_udf(col("routine_definition")))
.withColumn("isSecretValid",findValidSecret_udf(col("routine_definition")))
.withColumn("isValid",when((col("isFunctionValid") == "validFunction") & (col("isSecretValid") == "validSecret"),True).otherwise(lit(False)))
.withColumn("markedAt",lit(current_timestamp()))
.withColumn("status",lit("notAudited"))
)

display(data)

# COMMAND ----------

# would be the audit table for function which got removed 
data.write.format("delta").mode("append").saveAsTable(f"{catalogName}.{schemaName}.{tableName}")

# COMMAND ----------

invalidFun = spark.read.table(f"{catalogName}.{schemaName}.{tableName}").filter(col("isValid") == False).select(concat_ws(".","routine_catalog","routine_schema","routine_name").alias("toRemove"),"markedAt",lit("remove"))

# COMMAND ----------

def dropSelectedFunctions(funcNames:list) -> bool:
  removestmt = f"DROP FUNCTION if exists "
  num = len(funcNames)
  i = 0
  for func in funcNames:
    spark.sql(f""" {removestmt}{func[0]} """)
    i = i+1
  if i == num:
    return True
  return False

# COMMAND ----------

funcNames = invalidFun.select("toRemove").collect()
status = dropSelectedFunctions(funcNames)

# COMMAND ----------

def updateAuditTable(status:bool,auditTable:str,invalidFunc):
  if status == True:
    audit = spark.read.table(auditTable)
    audit.createOrReplaceTempView("audit")
    invalidFun.createOrReplaceTempView("invalidfun")
    spark.sql(f""" merge into audit
    USING invalidfun
    ON concat_ws(".",audit.routine_catalog,audit.routine_schema,audit.routine_name) = invalidfun.toRemove
    AND audit.markedAt = invalidfun.markedAt
    WHEN matched then update set audit.status = invalidfun.remove
    """)

# COMMAND ----------

updateAuditTable(status,f"{catalogName}.{schemaName}.{tableName}",invalidFun)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee.sales.auditFunctions

# COMMAND ----------

display(spark.sql(f"""
select routine_catalog,routine_schema,routine_name,routine_owner,routine_definition,created_by,last_altered_by from system.information_schema.routines
where routine_definition is not null"""))