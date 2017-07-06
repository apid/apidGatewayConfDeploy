SELECT a.id,
 a.organization_id,
 a.environment_id,
 a.bean_blob_id,
 a.resource_blob_id,
 a.type,
 a.name,
 a.revision,
 a.path,
 a.created_at,
 a.created_by,
 a.updated_at,
 a.updated_by
 FROM metadata_runtime_entity_metadata as a
 INNER JOIN edgex_blob_available as b
 ON a.resource_blob_id = b.id
 WHERE a.resource_blob_id != ""
 INTERSECT
 SELECT a.id,
 a.organization_id,
 a.environment_id,
 a.bean_blob_id,
 a.resource_blob_id,
 a.type,
 a.name,
 a.revision,
 a.path,
 a.created_at,
 a.created_by,
 a.updated_at,
 a.updated_by
 FROM metadata_runtime_entity_metadata as a
 INNER JOIN edgex_blob_available as b
 ON a.bean_blob_id = b.id
 WHERE a.resource_blob_id != ""
 UNION
 SELECT a.id,
 a.organization_id,
 a.environment_id,
 a.bean_blob_id,
 a.resource_blob_id,
 a.type,
 a.name,
 a.revision,
 a.path,
 a.created_at,
 a.created_by,
 a.updated_at,
 a.updated_by
 FROM metadata_runtime_entity_metadata as a
 INNER JOIN edgex_blob_available as b
 ON a.bean_blob_id = b.id
 WHERE a.resource_blob_id = ""
 )

 ;