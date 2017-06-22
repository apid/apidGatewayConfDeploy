package apiGatewayConfDeploy

import (
	"os"
	"time"

	"database/sql"
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	APIGEE_SYNC_EVENT     = "ApigeeSync"
	CONFIG_METADATA_TABLE = "project.runtime_blob_metadata"
)

func initListener(services apid.Services) {
	services.Events().Listen(APIGEE_SYNC_EVENT, &apigeeSyncHandler{})
}

type bundleConfigJson struct {
	Name         string `json:"name"`
	URI          string `json:"uri"`
	ChecksumType string `json:"checksumType"`
	Checksum     string `json:"checksum"`
}

type apigeeSyncHandler struct {
}

func (h *apigeeSyncHandler) String() string {
	return "gatewayDeploy"
}

func (h *apigeeSyncHandler) Handle(e apid.Event) {

	if changeSet, ok := e.(*common.ChangeList); ok {
		processChangeList(changeSet)
	} else if snapData, ok := e.(*common.Snapshot); ok {
		processSnapshot(snapData)
	} else {
		log.Debugf("Received invalid event. Ignoring. %v", e)
	}
}

func processSnapshot(snapshot *common.Snapshot) {

	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	db, err := data.DBVersion(snapshot.SnapshotInfo)
	if err != nil {
		log.Panicf("Unable to access database: %v", err)
	}

	// Update the DB pointer
	dbMux.Lock()
	SetDB(db)
	dbMux.Unlock()

	InitDB(db)
	startupOnExistingDatabase()
	log.Debug("Snapshot processed")
}

func startupOnExistingDatabase() {
	// start bundle downloads that didn't finish
	go func() {
		deployments, err := getUnreadyDeployments()

		if err != nil && err != sql.ErrNoRows {
			log.Panicf("unable to query database for unready deployments: %v", err)
		}
		log.Debugf("Queuing %d deployments for bundle download", len(deployments))
		for _, dep := range deployments {
			queueDownloadRequest(dep)
		}
	}()
}

func processChangeList(changes *common.ChangeList) {

	log.Debugf("Processing changes")
	// changes have been applied to DB
	var insertedDeployments, deletedDeployments []DataDeployment
	for _, change := range changes.Changes {
		switch change.Table {
		case CONFIG_METADATA_TABLE:
			switch change.Operation {
			case common.Insert:
				dep := dataDeploymentFromRow(change.NewRow)
				insertedDeployments = append(insertedDeployments, dep)
			case common.Delete:
				var id string
				change.OldRow.Get("id", &id)
				// only need these two fields to delete and determine bundle file
				dep := DataDeployment{
					ID: id,
				}
				deletedDeployments = append(deletedDeployments, dep)
			default:
				log.Errorf("unexpected operation: %s", change.Operation)
			}
		}
	}

	for _, d := range deletedDeployments {
		deploymentsChanged <- d.ID
	}

	for _, dep := range insertedDeployments {
		queueDownloadRequest(dep)
	}

	// clean up old bundles
	if len(deletedDeployments) > 0 {
		log.Debugf("will delete %d old bundles", len(deletedDeployments))
		go func() {
			// give clients a minute to avoid conflicts
			time.Sleep(bundleCleanupDelay)
			for _, dep := range deletedDeployments {
				bundleFile := getBundleFile(dep)
				log.Debugf("removing old bundle: %v", bundleFile)
				// TODO Remove from the Database table edgex_blob_available
				safeDelete(bundleFile)
			}
		}()
	}
}

func dataDeploymentFromRow(row common.Row) (d DataDeployment) {

	row.Get("id", &d.ID)
	row.Get("org_id", &d.OrgID)
	row.Get("env_id", &d.EnvID)
	row.Get("type", &d.Type)
	row.Get("name", &d.Name)
	row.Get("revision", &d.Revision)
	row.Get("blob_id", &d.BlobID)
	row.Get("resource_blob_id", &d.BlobResourceID)
	row.Get("updated_at", &d.Updated)
	row.Get("updated_by", &d.UpdatedBy)
	row.Get("created_at", &d.Created)
	row.Get("created_by", &d.CreatedBy)

	return
}

func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
