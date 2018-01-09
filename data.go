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

	"github.com/apid/apid-core"
	"reflect"
)

const (
	InitLSN = "0.0.0"
)

var (
	gwBlobId int64
)

type Configuration struct {
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
}

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type dbManagerInterface interface {
	setDbVersion(string)
	initDb() error
	getUnreadyBlobs() ([]string, error)
	getAllConfigurations(typeFilter string) ([]Configuration, error)
	updateLocalFsLocation(string, string) error
	getLocalFSLocation(string) (string, error)
	getConfigById(string) (*Configuration, error)
	loadLsnFromDb() error
	updateLSN(LSN string) error
	getLSN() string
}

type dbManager struct {
	data     apid.DataService
	db       apid.DB
	dbMux    sync.RWMutex
	apidLSN  string
	lsnMutex sync.RWMutex
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
	if err := initTables(dbc.getDb()); err != nil {
		log.Errorf("error in initTables(): %v", err)
		return err
	}

	if err := addIndexes(dbc.getDb()); err != nil {
		// not on critical path, continue in case of error
		log.Errorf("error in addIndexes(): %v", err)
	}
	return nil
}

func (dbc *dbManager) getConfigById(id string) (config *Configuration, err error) {
	row := dbc.getDb().QueryRow(`
	SELECT 	a.id,
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
		FROM METADATA_RUNTIME_ENTITY_METADATA as a
		WHERE a.id = ?;
	`, id)
	config, err = configurationFromDbRow(row)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func (dbc *dbManager) getUnreadyBlobs() (ids []string, err error) {

	rows, err := dbc.getDb().Query(`
	SELECT id FROM (
			SELECT a.bean_blob_id as id
			FROM METADATA_RUNTIME_ENTITY_METADATA as a
			WHERE a.bean_blob_id NOT IN
			(SELECT b.id FROM APID_BLOB_AVAILABLE as b)
		UNION
			SELECT a.resource_blob_id as id
			FROM METADATA_RUNTIME_ENTITY_METADATA as a
			WHERE a.resource_blob_id NOT IN
			(SELECT b.id FROM APID_BLOB_AVAILABLE as b)
	)
	WHERE id IS NOT NULL AND id != ''
	;
	`)
	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		err = rows.Scan(&id)
		if err != nil {
			return
		}
		ids = append(ids, id)
	}

	log.Debugf("Unready blobId %v", ids)
	return
}

/*
func (dbc *dbManager) getReadyConfigurations(typeFilter string) ([]Configuration, error) {

	// An alternative statement is in get_ready_deployments.sql
	// Need testing with large data volume to determine which is better

	var rows *sql.Rows
	var err error
	if typeFilter == "" {
		rows, err = dbc.getDb().Query(`
		SELECT 	a.id,
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
		FROM METADATA_RUNTIME_ENTITY_METADATA as a
		WHERE a.id IN (
			SELECT
					a.id
				FROM METADATA_RUNTIME_ENTITY_METADATA as a
				INNER JOIN APID_BLOB_AVAILABLE as b
				ON a.resource_blob_id = b.id
				WHERE a.resource_blob_id IS NOT NULL AND a.resource_blob_id != ""
			INTERSECT
				SELECT
					a.id
				FROM METADATA_RUNTIME_ENTITY_METADATA as a
				INNER JOIN APID_BLOB_AVAILABLE as b
				ON a.bean_blob_id = b.id
				WHERE a.resource_blob_id IS NOT NULL AND a.resource_blob_id != ""

			UNION
				SELECT
					a.id
				FROM METADATA_RUNTIME_ENTITY_METADATA as a
				INNER JOIN APID_BLOB_AVAILABLE as b
				ON a.bean_blob_id = b.id
				WHERE a.resource_blob_id IS NULL OR a.resource_blob_id = ""
		)
	;
	`)
	} else {
		rows, err = dbc.getDb().Query(`
		SELECT 	a.id,
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
		FROM METADATA_RUNTIME_ENTITY_METADATA as a
		WHERE a.type = ?
		AND a.id IN (
			SELECT
					a.id
				FROM METADATA_RUNTIME_ENTITY_METADATA as a
				INNER JOIN APID_BLOB_AVAILABLE as b
				ON a.resource_blob_id = b.id
				WHERE a.resource_blob_id IS NOT NULL AND a.resource_blob_id != ""
			INTERSECT
				SELECT
					a.id
				FROM METADATA_RUNTIME_ENTITY_METADATA as a
				INNER JOIN APID_BLOB_AVAILABLE as b
				ON a.bean_blob_id = b.id
				WHERE a.resource_blob_id IS NOT NULL AND a.resource_blob_id != ""

			UNION
				SELECT
					a.id
				FROM METADATA_RUNTIME_ENTITY_METADATA as a
				INNER JOIN APID_BLOB_AVAILABLE as b
				ON a.bean_blob_id = b.id
				WHERE a.resource_blob_id IS NULL OR a.resource_blob_id = ""
		)
	;
	`, typeFilter)
	}

	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return nil, err
	}
	defer rows.Close()

	confs, err := configurationsFromDbRows(rows)
	if err != nil {
		return nil, err
	}

	//log.Debugf("Configurations ready: %v", confs)

	return confs, nil

}
*/
func (dbc *dbManager) getAllConfigurations(typeFilter string) ([]Configuration, error) {

	// An alternative statement is in get_ready_deployments.sql
	// Need testing with large data volume to determine which is better

	var rows *sql.Rows
	var err error
	if typeFilter == "" {
		rows, err = dbc.getDb().Query(`
		SELECT 	a.id,
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
		FROM METADATA_RUNTIME_ENTITY_METADATA as a
	;`)
	} else {
		rows, err = dbc.getDb().Query(`
		SELECT 	a.id,
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
		FROM METADATA_RUNTIME_ENTITY_METADATA as a
		WHERE a.type = ?
	;`, typeFilter)
	}

	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return nil, err
	}
	defer rows.Close()

	confs, err := configurationsFromDbRows(rows)
	if err != nil {
		return nil, err
	}
	return confs, nil

}

func (dbc *dbManager) updateLocalFsLocation(blobId, localFsLocation string) error {
	txn, err := dbc.getDb().Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	_, err = txn.Exec(`
		INSERT OR IGNORE INTO APID_BLOB_AVAILABLE (
		id,
		local_fs_location
		) VALUES (?, ?);`, blobId, localFsLocation)
	if err != nil {
		log.Errorf("INSERT APID_BLOB_AVAILABLE id {%s} local_fs_location {%s} failed", localFsLocation, err)
		return err
	}
	err = txn.Commit()
	if err != nil {
		log.Errorf("UPDATE APID_BLOB_AVAILABLE id {%s} local_fs_location {%s} failed", localFsLocation, err)
		return err
	}

	log.Debugf("INSERT APID_BLOB_AVAILABLE {%s} local_fs_location {%s} succeeded", blobId, localFsLocation)
	return nil

}

func (dbc *dbManager) getLocalFSLocation(blobId string) (string, error) {

	log.Debugf("Getting the blob file for blobId {%s}", blobId)
	localFsLocation := sql.NullString{}
	err := dbc.getDb().QueryRow("SELECT local_fs_location FROM APID_BLOB_AVAILABLE WHERE id = '" + blobId + "'").Scan(&localFsLocation)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Errorf("SELECT local_fs_location failed %v", err)
		}
		return "", err
	}
	if localFsLocation.Valid {
		return localFsLocation.String, nil
	}
	log.Warnf("local_fs_location for blob %s is null!", blobId)
	return "", nil
}

func (dbc *dbManager) loadLsnFromDb() error {
	var LSN sql.NullString
	ret := InitLSN

	// If there's LSN for configuration
	err := dbc.getDb().QueryRow("select lsn from APID_CONFIGURATION_LSN LIMIT 1").Scan(&LSN)
	if err != nil && err != sql.ErrNoRows {
		log.Errorf("Failed to select lsn from APID_CONFIGURATION_LSN: %v", err)
		return err
	}
	if LSN.Valid {
		ret = LSN.String
		log.Debugf("LSN from APID_CONFIGURATION_LSN: %s", LSN.String)
	}
	dbc.lsnMutex.Lock()
	defer dbc.lsnMutex.Unlock()
	dbc.apidLSN = ret
	return nil
}

func (dbc *dbManager) getLSN() string {
	dbc.lsnMutex.RLock()
	defer dbc.lsnMutex.RUnlock()
	return dbc.apidLSN
}

func (dbc *dbManager) updateLSN(LSN string) (err error) {

	tx, err := dbc.getDb().Begin()
	if err != nil {
		log.Errorf("getApidInstanceInfo: Unable to get DB tx Err: {%v}", err)
		return
	}
	defer tx.Rollback()
	_, err = tx.Exec("UPDATE APID_CONFIGURATION_LSN SET lsn=?;", LSN)
	if err != nil {
		log.Errorf("UPDATE APID_CONFIGURATION_LSN Failed: %v", err)
		return
	}
	log.Debugf("UPDATE APID_CONFIGURATION_LSN Success: %s", LSN)
	if err = tx.Commit(); err != nil {
		log.Errorf("Commit error in updateLSN: %v", err)
		return
	}
	dbc.lsnMutex.Lock()
	defer dbc.lsnMutex.Unlock()
	dbc.apidLSN = LSN
	return
}

func configurationsFromDbRows(rows *sql.Rows) ([]Configuration, error) {
	tmp, err := structFromRows(reflect.TypeOf((*Configuration)(nil)).Elem(), rows)
	if err != nil {
		return nil, err
	}
	return tmp.([]Configuration), nil
}

func structFromRows(t reflect.Type, rows *sql.Rows) (interface{}, error) {
	num := t.NumField()
	cols := make([]interface{}, num)
	slice := reflect.New(reflect.SliceOf(t)).Elem()
	for i := range cols {
		cols[i] = new(sql.NullString)
	}
	for rows.Next() {
		v := reflect.New(t).Elem()
		err := rows.Scan(cols...)
		if err != nil {
			return nil, err
		}
		for i := range cols {
			p := cols[i].(*sql.NullString)
			if p.Valid {
				v.Field(i).SetString(p.String)
			}
		}
		slice = reflect.Append(slice, v)
	}
	return slice.Interface(), nil
}

func configurationFromDbRow(row *sql.Row) (*Configuration, error) {
	tmp, err := structFromRow(reflect.TypeOf((*Configuration)(nil)).Elem(), row)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Errorf("Error in configurationFromDbRow: %v", err)
		}
		return nil, err
	}
	config := tmp.(Configuration)
	return &config, nil
}

func structFromRow(t reflect.Type, row *sql.Row) (interface{}, error) {
	num := t.NumField()
	cols := make([]interface{}, num)
	for i := range cols {
		cols[i] = new(sql.NullString)
	}
	v := reflect.New(t).Elem()
	err := row.Scan(cols...)
	if err != nil {
		return nil, err
	}
	for i := range cols {
		p := cols[i].(*sql.NullString)
		if p.Valid {
			v.Field(i).SetString(p.String)
		}
	}
	return v.Interface(), nil
}

func initTables(db apid.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS APID_BLOB_AVAILABLE (
		id text primary key,
   		local_fs_location text NOT NULL
	);
	`)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`
	CREATE TABLE IF NOT EXISTS APID_CONFIGURATION_LSN (
		lsn text primary key
	);
	`)
	if err != nil {
		return err
	}

	// insert a row if APID_CONFIGURATION_LSN is empty
	_, err = tx.Exec(`
	INSERT INTO APID_CONFIGURATION_LSN (lsn)
	SELECT '0.0.0'
	WHERE NOT EXISTS (SELECT * FROM APID_CONFIGURATION_LSN)
	`)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	log.Debug("Database table APID_BLOB_AVAILABLE, APID_CONFIGURATION_LSN created.")
	return nil
}

func addIndexes(db apid.DB) error {
	log.Debug("add index to sqlite")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// add indexes
	_, err = tx.Exec(`
	CREATE INDEX IF NOT EXISTS config_type on METADATA_RUNTIME_ENTITY_METADATA (type);
	`)
	if err != nil {
		return err
	}
	return tx.Commit()
}
