-- Databricks notebook source
-- MAGIC %md Discovery Tagging

-- COMMAND ----------

with catalogOwner as
(select catalog_name,catalog_owner,"ALL_PRIVILEGES" as privilegeType 
from system.information_schema.catalogs),
schemaOwner as
(select catalog_name,schema_name,schema_owner,"ALL_PRIVILEGES" as privilegeType 
from system.information_schema.schemata),
catalogPriviledges as
(select 
CASE WHEN cp.grantor is null then catalogOwner.catalog_owner else cp.grantee end as principal_owner
,CASE WHEN cp.grantor is not null then cp.grantee else "No Explicit Permissions" end as principal
,CASE WHEN cp.privilege_type is null then catalogOwner.privilegeType else cp.privilege_type end as privileges_type
,catalogOwner.catalog_name 
from catalogOwner
left join system.information_schema.catalog_privileges cp
ON catalogOwner.catalog_name = cp.catalog_name),
schemaPriviledges as
(select 
CASE WHEN cp.grantor is null then schemaOwner.schema_owner else cp.grantee end as principal_owner
,CASE WHEN cp.grantor is not null then cp.grantee else "No Explicit Permissions" end as principal
,CASE WHEN cp.privilege_type is null then schemaOwner.privilegeType else cp.privilege_type end as privileges_type
,schemaOwner.catalog_name,schemaOwner.schema_name
from schemaOwner
left join system.information_schema.schema_privileges cp
ON schemaOwner.catalog_name = cp.catalog_name
AND schemaOwner.schema_name = cp.schema_name),
tableOwner as
(select table_catalog,table_schema,table_name,table_owner,"ALL_PRIVILEGES" as privilegeType from system.information_schema.tables),
tablePriviledges as
(select 
CASE WHEN cp.grantor is null then tableOwner.table_owner else cp.grantee end as principal_owner
,CASE WHEN cp.grantor is not null then cp.grantee else "No Explicit Permissions" end as principal
,CASE WHEN cp.privilege_type is null then tableOwner.privilegeType else cp.privilege_type end as privileges_type
,tableOwner.table_catalog,tableOwner.table_schema,tableOwner.table_name
from tableOwner
left join system.information_schema.table_privileges cp
ON tableOwner.table_catalog = cp.table_catalog
AND tableOwner.table_schema = cp.table_schema
AND tableOwner.table_name = cp.table_name),
lookup as
(select * from employee.metadata_demo.privilegeLookup),
master AS
(select master.*,explode(master.tags) as discoveryTags,
CASE 
WHEN master.type = "Catalog" THEN catalogPriviledges.principal_owner
WHEN master.type = "Schema" THEN schemaPriviledges.principal_owner
WHEN master.type = "Table" THEN tablePriviledges.principal_owner
end as principal_owner,
CASE 
WHEN master.type = "Catalog" THEN catalogPriviledges.principal
WHEN master.type = "Schema" THEN schemaPriviledges.principal
WHEN master.type = "Table" THEN tablePriviledges.principal
end as principal,
CASE
WHEN master.type = "Catalog" THEN catalogPriviledges.privileges_type
WHEN master.type = "Schema" THEN schemaPriviledges.privileges_type
WHEN master.type = "Table" THEN tablePriviledges.privileges_type
end as privilegeType
from employee.metadata_demo.tagsinfo master
left join catalogPriviledges
ON catalogPriviledges.catalog_name = master.identifier
left join schemaPriviledges
ON concat_ws(".",schemaPriviledges.catalog_name,schemaPriviledges.schema_name) = master.identifier
left join tablePriviledges
ON concat_ws(".",tablePriviledges.table_catalog,tablePriviledges.table_schema,tablePriviledges.table_name) = master.identifier)
select master.*,lkp.privilegeInheritance
from master
left join lookup lkp
ON lower(master.type) = lower(lkp.type)
AND master.privilegeType = lkp.privilegeType

-- COMMAND ----------

-- MAGIC %md All Unity Assets

-- COMMAND ----------

with allCatalogs AS 
(select catalog_name as identifier,"Catalog" as type,catalog_owner as principal,"ALL_PRIVILEGES" as privilegeType from 
system.information_schema.catalogs),
allSchemas AS 
(select concat_ws(".",catalog_name,schema_name) as identifier,"Schema" as type,schema_owner as principal,"ALL_PRIVILEGES" as privilegeType 
from system.information_schema.schemata),
allTables AS 
(select concat_ws(".",table_catalog,table_schema,table_name) as identifier,"Table" as type,table_owner as principal,"ALL_PRIVILEGES" as privilegeType 
from system.information_schema.tables),
allCatalogPrivileges AS
(select catalog_name as identifier,"Catalog" as type,grantee as principal,privilege_type as privilegeType 
from system.information_schema.catalog_privileges),
allSchemaPrivileges AS
(select concat_ws(".",catalog_name,schema_name) as identifier,"Schema" as type,grantee as principal,privilege_type as privilegeType 
from system.information_schema.schema_privileges),
allTablePrivileges AS
(select concat_ws(".",table_catalog,table_schema,table_name) as identifier,"Table" as type,grantee as principal,privilege_type as privilegeType 
from system.information_schema.table_privileges),
master as 
(select * from allCatalogs c
union 
select * from allSchemas s
union 
select * from allTables t
union 
select * from allCatalogPrivileges ac
union 
select * from allSchemaPrivileges as
union 
select * from allTablePrivileges at),
lookup as
(select * from employee.metadata_demo.privilegeLookup)
select master.*,lkp.privilegeInheritance
from master
left join lookup lkp
ON lower(master.type) = lower(lkp.type)
AND master.privilegeType = lkp.privilegeType

-- COMMAND ----------

-- MAGIC %md Functions in Unity Catalog

-- COMMAND ----------

with routineOwner AS 
(select concat_ws(".",routine_catalog,routine_schema,routine_name) as identifier,routine_owner as principal,"ALL_PRIVILEGES" as ownerPriviledge,routine_type,routine_body,routine_definition
from system.information_schema.routines),
routinePriviledges AS 
(select concat_ws(".",routine_catalog,routine_schema,routine_name) as identifier,grantee as permissionTo,grantor as permissionFrom,privilege_type as priviledgeType
from system.information_schema.routine_privileges),
master as
(select routineOwner.identifier,
routineOwner.principal as principalOwner,
case when routinePriviledges.permissionTo is null then "ALL_PRIVILEGES"
else routinePriviledges.priviledgeType
end as privilegeType,
routineOwner.routine_type,routineOwner.routine_body,routineOwner.routine_definition,
routinePriviledges.permissionTo,routinePriviledges.permissionFrom,"Function" as type
from routineOwner
left join routinePriviledges
ON routineOwner.identifier = routinePriviledges.identifier),
lookup as
(select * from employee.metadata_demo.privilegeLookup)
select master.*,lkp.privilegeInheritance
from master
left join lookup lkp
ON lower(master.type) = lower(lkp.type)
AND master.privilegeType = lkp.privilegeType

-- COMMAND ----------

-- MAGIC %md Function Validity

-- COMMAND ----------

select auditFunctions.*,markedAt as 
lastCheckedAt from employee.sales.auditFunctions

-- COMMAND ----------

-- MAGIC %md workspaceUserManagement

-- COMMAND ----------

select userName as Principal,Groups,entitlements as workspaceAccess from <Delta Table in workspaceUserManagement (Notebook)>