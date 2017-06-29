// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package apiGatewayConfDeploy

import (
	"database/sql"
	"sync"

	"github.com/30x/apid-core"
)

var (
	gwBlobId int64
)

type DataDeployment struct {
	ID             string
	OrgID          string
	EnvID          string
	BlobID         string
	BlobResourceID string
	Type           string
	Name           string
	Revision       string
	Path           string
	Created        string
	CreatedBy      string
	Updated        string
	UpdatedBy      string
	BlobFSLocation string
}

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type dbManagerInterface interface {
	setDbVersion(string)
	initDb() error
	getUnreadyBlobs() ([]string, error)
	getReadyDeployments() ([]DataDeployment, error)
	updateLocalFsLocation(string, string) error
	getLocalFSLocation(string) (string, error)
}

type dbManager struct {
	data  apid.DataService
	db    apid.DB
	dbMux sync.RWMutex
}

func (dbc *dbManager) setDbVersion(version string) {
	db, err := dbc.data.DBVersion(version)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}
	dbc.dbMux.Lock()
	dbc.db = db
	dbc.dbMux.Unlock()
}

func (dbc *dbManager) getDb() apid.DB {
	dbc.dbMux.RLock()
	defer dbc.dbMux.RUnlock()
	return dbc.db
}

func (dbc *dbManager) initDb() error {
	_, err := dbc.getDb().Exec(`
	CREATE TABLE IF NOT EXISTS edgex_blob_available (
		id text primary key,
   		local_fs_location text NOT NULL
	);
	`)
	if err != nil {
		return err
	}

	log.Debug("Database table edgex_blob_available created.")
	return nil
}

// getUnreadyDeployments() returns array of resources that are not yet to be processed
// TODO make it work with new schema
func (dbc *dbManager) getUnreadyBlobs() (ids []string, err error) {

	// get unready blob id
	rows, err := dbc.getDb().Query(`
	SELECT a.bean_blob_id
		FROM metadata_runtime_entity_metadata as a
		LEFT JOIN edgex_blob_available as b
		ON a.bean_blob_id = b.id
		WHERE b.id IS NULL;
	`)
	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}

	// get unready resource id
	rows, err = dbc.getDb().Query(`
	SELECT a.resource_blob_id
		FROM metadata_runtime_entity_metadata as a
		LEFT JOIN edgex_blob_available as b
		ON a.resource_blob_id = b.id
		WHERE (b.id IS NULL AND a.resource_blob_id IS NOT NULL AND a.resource_blob_id != '');
	`)
	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		rows.Scan(&id)
		ids = append(ids, id)
	}

	log.Debugf("Unready blobId %v", ids)
	return
}

// getDeployments()
func (dbc *dbManager) getReadyDeployments() ([]DataDeployment, error) {

	rows, err := dbc.getDb().Query(`SELECT
	a.id,
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
	a.updated_by,
	b.local_fs_location
	FROM metadata_runtime_entity_metadata as a
	INNER JOIN edgex_blob_available as b
	ON (a.bean_blob_id = b.id OR a.resource_blob_id = b.id)
	;
	`)

	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return nil, err
	}
	defer rows.Close()

	deployments, err := dataDeploymentsFromRow(rows)
	if err != nil {
		return nil, err
	}

	log.Debugf("Configurations ready: %v", deployments)

	if len(deployments) == 0 {
		log.Debug("No resources ready to be deployed")
		err = sql.ErrNoRows
	}
	return deployments, err

}

func (dbc *dbManager) updateLocalFsLocation(blobId, localFsLocation string) error {

	stmt, err := dbc.getDb().Prepare(`
		INSERT OR IGNORE INTO edgex_blob_available (
		id,
		local_fs_location
		) VALUES (?, ?);`)
	if err != nil {
		log.Errorf("PREPARE updateLocalFsLocation failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(blobId, localFsLocation)
	if err != nil {
		log.Errorf("UPDATE edgex_blob_available id {%s} local_fs_location {%s} failed", localFsLocation, err)
		return err
	}

	log.Debugf("INSERT edgex_blob_available {%s} local_fs_location {%s} succeeded", blobId, localFsLocation)
	return nil

}

func (dbc *dbManager) getLocalFSLocation(blobId string) (localFsLocation string, err error) {

	log.Debugf("Getting the blob file for blobId {%s}", blobId)
	rows, err := dbc.getDb().Query("SELECT local_fs_location FROM edgex_blob_available WHERE id = '" + blobId + "'")
	if err != nil {
		log.Errorf("SELECT local_fs_location failed %v", err)
		return "", err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&localFsLocation)
		if err != nil {
			log.Errorf("Scan local_fs_location failed %v", err)
			return "", err
		}
		log.Debugf("Got the blob file {%s} for blobId {%s}", localFsLocation, blobId)
	}
	return
}

func dataDeploymentsFromRow(rows *sql.Rows) (deployments []DataDeployment, err error) {
	for rows.Next() {
		dep := DataDeployment{}
		err = rows.Scan(
			&dep.ID,
			&dep.OrgID,
			&dep.EnvID,
			&dep.BlobID,
			&dep.BlobResourceID,
			&dep.Type,
			&dep.Name,
			&dep.Revision,
			&dep.Path,
			&dep.Created,
			&dep.CreatedBy,
			&dep.Updated,
			&dep.UpdatedBy,
			&dep.BlobFSLocation,
		)
		if err != nil {
			return nil, err
		}
		deployments = append(deployments, dep)
	}
	return
}
