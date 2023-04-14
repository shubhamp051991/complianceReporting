# Databricks notebook source
functionName = dbutils.widgets.getArgument("functionName")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- this should be the gold level where we store all audit logs tables by their service names
# MAGIC show tables in demos.audit_log

# COMMAND ----------

# MAGIC %md How many encyption UDF's are in my entire metastore
# MAGIC 1. Check the system.information_schema.routines
# MAGIC 2. If user does not have access to the function, user won't be able to see the function_definition
# MAGIC 3. Data Definiton : https://docs.databricks.com/sql/language-manual/information-schema/routines.html#definition

# COMMAND ----------

# MAGIC %sql
# MAGIC select routine_catalog,routine_schema,routine_name,routine_owner,routine_definition,created_by,last_altered_by from system.information_schema.routines
# MAGIC where routine_definition is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select routine_catalog,routine_schema,routine_name,routine_owner,routine_definition,created_by,last_altered_by from system.information_schema.routines
# MAGIC where routine_definition is not null
# MAGIC AND ((lower(routine_name) like "%encrypt%" or lower(routine_name) like "%decrypt%") OR (lower(routine_definition) like "%encrypt%" or lower(routine_definition) like "%decrypt%"))

# COMMAND ----------

# MAGIC %md Check the priviledge status of all my encrypt and decrypt UDF
# MAGIC 1. system.information_schema.routine_privileges
# MAGIC 2. Data Definition : https://docs.databricks.com/sql/language-manual/information-schema/routine_privileges.html#definition

# COMMAND ----------

# MAGIC %sql
# MAGIC select grantor,grantee,routine_schema,routine_name,privilege_type,inherited_from from system.information_schema.routine_privileges
# MAGIC where ((lower(routine_name) like "%encrypt%" or lower(routine_name) like "%decrypt%"))

# COMMAND ----------

# MAGIC %md who has tried to read the secret

# COMMAND ----------

# MAGIC %sql
# MAGIC select requestParams.scope as scope,requestParams.key as key,actionName,response.result,email,date_time from demos.audit_log.secrets
# MAGIC where size(requestParams) > 1

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Session 30th March
# MAGIC 
# MAGIC Session 30th
# MAGIC 1. More information on the algorithm less than AES 256 - working example of the trigger if anything less than 256 or is caught
# MAGIC 2. Information on error codes on CommandSubmit - Done

# COMMAND ----------

# MAGIC %md 
# MAGIC #### who tried to access the encrypt /decrypt function and what message they got as a output

# COMMAND ----------

notebookLogs = spark.sql(f""" 
with notebookSource as
(select workspaceId,email,requestParams.path,requestParams.notebookId from demos.audit_log.notebook
where actionName IN ("attachNotebook","createNotebook")
)
select nbk.workspaceId,requestParams.commandText,requestParams.notebookId as sourceID,requestParams.status,response.errormessage,response.statuscode,nbk.email,date_time,notebookSource.path as notebook_path,"fromNotebook" as executionSource
from demos.audit_log.notebook nbk
inner join notebookSource
ON notebookSource.workspaceId = nbk.workspaceId
AND notebookSource.notebookId = nbk.requestParams.notebookId
where actionName = "runCommand"
AND lower(requestParams.commandText) like "%{functionName}%" """) # trying to search something like functionName in notebook cells

# COMMAND ----------


dbsqlLogs = spark.sql(f"""
with commandSubmit AS (select  submit.workspaceId,submit.email,submit.requestParams.warehouseId as warehouseid,submit.requestParams.commandId as commandId,submit.requestParams.commandText,submit.date_time
from demos.audit_log.databrickssql submit
where submit.actionName = "commandSubmit"
AND submit.requestParams.commandText like "%{functionName}%"),
commandFinish as (select  requestParams.warehouseId,requestParams.commandId,response.errormessage,response.result,response.statuscode from demos.audit_log.databrickssql
where actionName = "commandFinish")
select cs.workspaceId,cs.commandText,cs.commandId as sourceID,
case when cf.statuscode == 200 then "Success"
else "Error/Warning"
end as status
,cf.errormessage,cf.statuscode,cs.email,cs.date_time,"dbsql_path_not_exist","fromDbsql" as executionSource 
from commandSubmit cs
inner join commandFinish cf
WHERE cs.warehouseid = cf.warehouseId
AND cs.commandId=cf.commandId """)

# COMMAND ----------

allrunLogs = notebookLogs.unionAll(dbsqlLogs)
display(allrunLogs)