-- Copyright 2017 Google Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
