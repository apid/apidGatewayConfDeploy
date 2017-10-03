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
	"os"

	"github.com/apid/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	APIGEE_SYNC_EVENT     = "ApigeeSync"
	CONFIG_METADATA_TABLE = "metadata.runtime_entity_metadata"
)

func (h *apigeeSyncHandler) initListener(services apid.Services) {
	services.Events().Listen(APIGEE_SYNC_EVENT, h)
}

func (h *apigeeSyncHandler) stopListener(services apid.Services) {
	if !h.closed {
		services.Events().StopListening(APIGEE_SYNC_EVENT, h)
		h.closed = true
	}
}

type bundleConfigJson struct {
	Name         string `json:"name"`
	URI          string `json:"uri"`
	ChecksumType string `json:"checksumType"`
	Checksum     string `json:"checksum"`
}

type apigeeSyncHandler struct {
	dbMan     dbManagerInterface
	apiMan    apiManagerInterface
	bundleMan bundleManagerInterface
	configCache *ConfigurationsEtagCache
	closed    bool
}

func (h *apigeeSyncHandler) String() string {
	return "gatewayDeploy"
}

func (h *apigeeSyncHandler) Handle(e apid.Event) {

	if changeSet, ok := e.(*common.ChangeList); ok {
		h.processChangeList(changeSet)
	} else if snapData, ok := e.(*common.Snapshot); ok {
		h.processSnapshot(snapData)
	} else {
		log.Debugf("Received invalid event. Ignoring. %v", e)
	}
}

func (h *apigeeSyncHandler) processSnapshot(snapshot *common.Snapshot) {

	log.Debugf("Snapshot received. Switching to DB version: %s", snapshot.SnapshotInfo)

	h.dbMan.setDbVersion(snapshot.SnapshotInfo)
	h.apiMan.InitDistributeEvents()
	h.startupOnExistingDatabase()
	h.apiMan.InitAPI()
	log.Debug("Snapshot processed")
}

// TODO make it work with new schema
func (h *apigeeSyncHandler) startupOnExistingDatabase() {
	// start bundle downloads that didn't finish
	go func() {
		// create apid_blob_available table
		h.dbMan.initDb()
		configs, err := h.dbMan.getUnreadyConfigs()

		if err != nil {
			log.Panicf("unable to query database for unready deployments: %v", err)
		}

		log.Debugf("Queuing %d unready config downloads", len(configs))
		for _, c := range configs {
			go h.bundleMan.queueDownloadRequest(c)
		}
	}()
}

func (h *apigeeSyncHandler) processChangeList(changes *common.ChangeList) {

	log.Debugf("Processing changes")
	// changes have been applied to DB by apidApigeeSync
	var insertedConfigs, updatedNewConfigs []*pendingConfiguration
	var updatedConfigOldIds, deletedConfigIds []string
	var deletedBlobIds, updatedOldBlobIds []string
	for _, change := range changes.Changes {
		switch change.Table {
		case CONFIG_METADATA_TABLE:
			switch change.Operation {
			case common.Insert:
				dep := dataDeploymentFromRow(change.NewRow)
				insertedConfigs = append(insertedConfigs, &pendingConfiguration{
					dataDeployment: &dep,
				})
			case common.Delete:
				dep := dataDeploymentFromRow(change.OldRow)
				deletedConfigIds = append(deletedConfigIds, dep.ID)

				deletedBlobIds = append(deletedBlobIds, dep.BlobResourceID)
				deletedBlobIds = append(deletedBlobIds, dep.BlobID)
			case common.Update:
				depNew := dataDeploymentFromRow(change.NewRow)
				depOld := dataDeploymentFromRow(change.OldRow)

				updatedConfigOldIds = append(updatedConfigOldIds, depOld.ID)
				updatedNewConfigs = append(updatedNewConfigs, &pendingConfiguration{
					dataDeployment: &depNew,
				})

				updatedOldBlobIds = append(updatedOldBlobIds, depOld.BlobResourceID)
				updatedOldBlobIds = append(updatedOldBlobIds, depOld.BlobID)
			default:
				log.Errorf("unexpected operation: %s", change.Operation)
			}
		}
	}



	// update cache with deleted/updated configs
	log.Debugf("will delete %d configs from cache", len(deletedConfigIds))
	h.configCache.DeleteBunch(deletedConfigIds)
	log.Debugf("will delete %d updated old configs from cache", len(updatedConfigOldIds))
	h.configCache.DeleteBunch(updatedConfigOldIds)

	// TODO clean the old blobs
	h.bundleMan.deleteBlobs(deletedBlobIds)
	h.bundleMan.deleteBlobs(updatedOldBlobIds)

	// insert
	for _, c := range insertedConfigs {
		go h.bundleMan.queueDownloadRequest(c)
	}
	// update
	for _, c := range updatedNewConfigs {
		go h.bundleMan.queueDownloadRequest(c)
	}

}

func dataDeploymentFromRow(row common.Row) (d Configuration) {

	row.Get("id", &d.ID)
	row.Get("organization_id", &d.OrgID)
	row.Get("environment_id", &d.EnvID)
	row.Get("bean_blob_id", &d.BlobID)
	row.Get("resource_blob_id", &d.BlobResourceID)
	row.Get("type", &d.Type)
	row.Get("name", &d.Name)
	row.Get("revision", &d.Revision)
	row.Get("path", &d.Path)
	row.Get("created_at", &d.Created)
	row.Get("created_by", &d.CreatedBy)
	row.Get("updated_at", &d.Updated)
	row.Get("updated_by", &d.UpdatedBy)

	return
}

// TODO delete from file system
func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
