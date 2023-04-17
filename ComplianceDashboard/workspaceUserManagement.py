# Databricks notebook source
catalogName = dbutils.widgets.getArgument("catalogName")
schemaName = dbutils.widgets.getArgument("schemaName")
tableName = dbutils.widgets.getArgument("tableName")

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

# GET USERS AND GROUPS FROM API
import requests

url = "https://e2-demo-field-eng.cloud.databricks.com"
token = "<PersonalAccessToken>"

def get_api(endpoint_url, token):
    res = requests.get(endpoint_url, headers = {"Authentication": f"Bearer {token}"})
    return res.json()

def get_users(workspace_url, token):
    endpoint_url = workspace_url + "/api/2.0/preview/scim/v2/Users"
    res = get_api(endpoint_url, token)
    return res

def get_groups(workspace_url, token):
    endpoint_url = workspace_url + "/api/2.0/preview/scim/v2/Groups"
    res = get_api(endpoint_url, token)
    return res

users = get_users(url, token)
groups = get_groups(url, token)

# PARSE USERS AND GROUPS

def parse_user(user):
    return {
        "displayName": user.get("displayName"),
        "userName": user.get("userName"),
        "groups": [g.get("display") for g in user.get("groups", [])],
        "entitlements": [e.get("value") for e in user.get("entitlements", [])],
        "userId": user.get("id")
    }

def parse_group(group):
    return {
        "displayName": group.get("displayName"),
        "members": [m.get("display") for m in group.get("members", [])],
        "entitlements": [e.get("value") for e in group.get("entitlements", [])],
        "groupId": group.get("id")
    }

parsed_users = [parse_user(user) for user in users["Resources"]]
parsed_groups = [parse_group(group) for group in groups["Resources"]]

# GET ENTITLEMENTS

def get_inherited_entitlements(user, groups):
    user_groups = user.get("groups")
    return list({e
     for g in groups if g.get("displayName") in user_groups and g.get("entitlements")
     for e in g["entitlements"] })

for user in parsed_users:
    user["inherited_entitlements"] = get_inherited_entitlements(user, parsed_groups)
    user["final_entitlements"] = list(set(user["entitlements"] + user["inherited_entitlements"]))

# COMMAND ----------

data_df = (spark.createDataFrame(parsed_users)
    .withColumn("Groups",explode("groups"))
    .withColumn("entitlements",explode("final_entitlements"))
    .withColumn("inheritedEntitlements",explode("inherited_entitlements"))
    ).select("userName","Groups","entitlements")

data_df.distinct().write.mode("overwrite").saveAsTable(f"{catalogName}.{schemaName}.{tableName}")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists employee.metadata_demo.privilegeLookup;
# MAGIC create table employee.metadata_demo.privilegeLookup (type string,privilegeType STRING,privilegeInheritance string);

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee.metadata_demo.privilegeLookup
# MAGIC VALUES
# MAGIC ("Catalog","ALL_PRIVILEGES","USE CATALOG"),
# MAGIC ("Catalog","ALL_PRIVILEGES","USE SCHEMA"),
# MAGIC ("Catalog","ALL_PRIVILEGES","SELECT"),
# MAGIC ("Catalog","ALL_PRIVILEGES","MODIFY"),
# MAGIC ("Catalog","ALL_PRIVILEGES","EXECUTE"),
# MAGIC ("Catalog","ALL_PRIVILEGES","READ VOLUME"),
# MAGIC ("Catalog","ALL_PRIVILEGES","WRITE VOLUME"),
# MAGIC ("Catalog","ALL_PRIVILEGES","CREATE SCHEMA"),
# MAGIC ("Catalog","ALL_PRIVILEGES","CREATE TABLE"),
# MAGIC ("Catalog","ALL_PRIVILEGES","CREATE FUNCTION"),
# MAGIC ("Catalog","ALL_PRIVILEGES","CREATE VOLUME"),
# MAGIC ("Schema","ALL_PRIVILEGES","USE SCHEMA"),
# MAGIC ("Schema","ALL_PRIVILEGES","SELECT"),
# MAGIC ("Schema","ALL_PRIVILEGES","MODIFY"),
# MAGIC ("Schema","ALL_PRIVILEGES","EXECUTE"),
# MAGIC ("Schema","ALL_PRIVILEGES","READ VOLUME"),
# MAGIC ("Schema","ALL_PRIVILEGES","WRITE VOLUME"),
# MAGIC ("Schema","ALL_PRIVILEGES","CREATE TABLE"),
# MAGIC ("Schema","ALL_PRIVILEGES","CREATE FUNCTION"),
# MAGIC ("Schema","ALL_PRIVILEGES","CREATE VOLUME"),
# MAGIC ("Table","ALL_PRIVILEGES","SELECT"),
# MAGIC ("Table","ALL_PRIVILEGES","MODIFY"),
# MAGIC ("Function","ALL_PRIVILEGES","EXECUTE")

# COMMAND ----------

