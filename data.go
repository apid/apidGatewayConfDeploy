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
	Type           string
	Name           string
	Revision       string
	BlobID         string
	GWBlobID       string
	BlobResourceID string
	Updated        string
	UpdatedBy      string
	Created        string
	CreatedBy      string
	BlobFSLocation string
	BlobURL        string
}

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type dbManagerInterface interface {
	setDbVersion(string)
	initDb() error
	getUnreadyDeployments() ([]DataDeployment, error)
	getReadyDeployments() ([]DataDeployment, error)
	updateLocalFsLocation(string, string, string) error
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
   		gwblobid integer primary key,
   		runtime_meta_id character varying NOT NULL,
   		local_fs_location character varying NOT NULL,
   		access_url character varying
	);
	`)
	if err != nil {
		return err
	}

	log.Debug("Database tables created.")
	return nil
}

// getUnreadyDeployments() returns array of resources that are not yet to be processed
func (dbc *dbManager) getUnreadyDeployments() (deployments []DataDeployment, err error) {

	rows, err := dbc.getDb().Query(`
	SELECT project_runtime_blob_metadata.id, org_id, env_id, name, revision, blob_id, resource_blob_id
		FROM project_runtime_blob_metadata
			LEFT JOIN edgex_blob_available
			ON project_runtime_blob_metadata.id = edgex_blob_available.runtime_meta_id
		WHERE edgex_blob_available.runtime_meta_id IS NULL;
	`)

	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		dep := DataDeployment{}
		rows.Scan(&dep.ID, &dep.OrgID, &dep.EnvID, &dep.Name, &dep.Revision, &dep.BlobID,
			&dep.BlobResourceID)
		deployments = append(deployments, dep)
		log.Debugf("New configurations to be processed Id {%s}, blobId {%s}", dep.ID, dep.BlobID)
	}
	if len(deployments) == 0 {
		log.Debug("No new resources found to be processed")
		err = sql.ErrNoRows
	}
	return

}

// getDeployments()
func (dbc *dbManager) getReadyDeployments() (deployments []DataDeployment, err error) {

	rows, err := dbc.getDb().Query(`
	SELECT a.id, a.org_id, a.env_id, a.name, a.type, a.revision, a.blob_id,
		a.resource_blob_id, a.created_at, a.created_by, a.updated_at, a.updated_by,
		b.local_fs_location, b.access_url, b.gwblobid
		FROM project_runtime_blob_metadata as a
			INNER JOIN edgex_blob_available as b
			ON a.id = b.runtime_meta_id
	`)

	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		dep := DataDeployment{}
		rows.Scan(&dep.ID, &dep.OrgID, &dep.EnvID, &dep.Name, &dep.Type, &dep.Revision, &dep.BlobID,
			&dep.BlobResourceID, &dep.Created, &dep.CreatedBy, &dep.Updated,
			&dep.UpdatedBy, &dep.BlobFSLocation, &dep.BlobURL, &dep.GWBlobID)
		deployments = append(deployments, dep)
		log.Debugf("New Configurations available Id {%s} BlobId {%s}", dep.ID, dep.BlobID)
	}
	if len(deployments) == 0 {
		log.Debug("No resources ready to be deployed")
		err = sql.ErrNoRows
	}
	return

}

func (dbc *dbManager) updateLocalFsLocation(depID, bundleId, localFsLocation string) error {

	access_url := getHttpHost() + blobEndpointPath + "/" + bundleId
	stmt, err := dbc.getDb().Prepare(`
		INSERT INTO edgex_blob_available (runtime_meta_id, gwblobid, local_fs_location, access_url)
			VALUES (?, ?, ?, ?)`)
	if err != nil {
		log.Errorf("PREPARE updatelocal_fs_location failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(depID, bundleId, localFsLocation, access_url)
	if err != nil {
		log.Errorf("UPDATE edgex_blob_available id {%s} local_fs_location {%s} failed: %v", depID, localFsLocation, err)
		return err
	}

	log.Debugf("INSERT edgex_blob_available {%s} local_fs_location {%s} succeeded", depID, localFsLocation)
	return nil

}

func (dbc *dbManager) getLocalFSLocation(blobId string) (locfs string, err error) {

	log.Debugf("Getting the blob file for blobId {%s}", blobId)
	rows, err := dbc.getDb().Query("SELECT local_fs_location FROM edgex_blob_available WHERE gwblobid = \"" + blobId + "\"")
	if err != nil {
		log.Errorf("SELECT local_fs_location failed %v", err)
		return "", err
	}

	defer rows.Close()
	for rows.Next() {
		rows.Scan(&locfs)
		log.Debugf("Got the blob file {%s} for blobId {%s}", locfs, blobId)
	}
	return
}
