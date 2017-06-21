package apiGatewayDeploy

import (
	"database/sql"
	"sync"

	"github.com/30x/apid-core"

)

var (
	unsafeDB apid.DB
	dbMux    sync.RWMutex
)

type DataDeployment struct {
	ID                 string
	OrgID              string
	EnvID              string
	Type               string
	Name		   string
	Revision           string
	BlobID             string
	BlobResourceID     string
	Updated            string
	UpdatedBy          string
	Created		   string
	CreatedBy          string
	BlobFSLocation     string
	BlobURL            string
}

type SQLExec interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func InitDB(db apid.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS edgex_blob_available (
   		blob_id character varying NOT NULL,
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

func getDB() apid.DB {
	dbMux.RLock()
	db := unsafeDB
	dbMux.RUnlock()
	return db
}

// caller is responsible for calling dbMux.Lock() and dbMux.Unlock()
func SetDB(db apid.DB) {
	if unsafeDB == nil { // init API when DB is initialized
		go InitAPI()
	}
	unsafeDB = db
}

// getUnreadyDeployments() returns array of resources that are not yet to be processed
func getUnreadyDeployments() (deployments []DataDeployment, err error) {

	err = nil
	db := getDB()

	rows, err := db.Query(`
	SELECT id, org_id, env_id, name, revision, project_runtime_blob_metadata.blob_id, resource_blob_id
		FROM project_runtime_blob_metadata
			LEFT JOIN edgex_blob_available
			ON project_runtime_blob_metadata.blob_id = edgex_blob_available.blob_id
		WHERE edgex_blob_available.blob_id IS NULL;
	`)

	if err != nil {
		log.Errorf("DB Query for project_runtime_blob_metadata failed %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		dep := DataDeployment{}
		rows.Scan(&dep.ID, &dep.OrgID, &dep.EnvID,  &dep.Name, &dep.Revision, &dep.BlobID,
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
func getReadyDeployments() (deployments []DataDeployment, err error) {

	err = nil
	db := getDB()

	rows, err := db.Query(`
	SELECT a.id, a.org_id, a.env_id, a.name, a.type, a.revision, a.blob_id,
		a.resource_blob_id, a.created_at, a.created_by, a.updated_at, a.updated_by,
		b.local_fs_location, b.access_url
		FROM project_runtime_blob_metadata as a
			INNER JOIN edgex_blob_available as b
			ON a.blob_id = b.blob_id
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
			&dep.UpdatedBy, &dep.BlobFSLocation, &dep.BlobURL)
		deployments = append(deployments, dep)
		log.Debugf("New Configurations available Id {%s} BlobId {%s}", dep.ID, dep.BlobID)
	}
	if len(deployments) == 0 {
		log.Debug("No resources ready to be deployed")
		err = sql.ErrNoRows
	}
	return

}

func updatelocal_fs_location(depID, local_fs_location string) error {

	access_url :=  config.GetString("api_listen") + "/blob/" + depID
	stmt, err := getDB().Prepare(`
		INSERT INTO edgex_blob_available (blob_id, local_fs_location, access_url)
			VALUES (?, ?, ?)`)
	if err != nil {
		log.Errorf("PREPARE updatelocal_fs_location failed: %v", err)
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(depID, local_fs_location, access_url)
	if err != nil {
		log.Errorf("UPDATE edgex_blob_available id {%s} local_fs_location {%s} failed: %v", depID, local_fs_location, err)
		return err
	}

	log.Debugf("INSERT edgex_blob_available {%s} local_fs_location {%s} succeeded", depID, local_fs_location)
	return nil

}

func getLocalFSLocation (blobId string) (locfs string , err error) {

	db := getDB()

	rows, err := db.Query("SELECT local_fs_location FROM edgex_blob_available WHERE blob_id = " + blobId)
	if err != nil {
		log.Errorf("SELECT local_fs_location failed %v", err)
		return "", err
	}

	defer rows.Close()
	rows.Scan(&locfs)
	return
}
