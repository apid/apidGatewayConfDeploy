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

	h.startupOnExistingDatabase()
	if lsn := h.dbMan.getLSN(); lsn != "" {
		h.dbMan.updateLSN(lsn)
	} else { //apid just started
		h.dbMan.loadLsnFromDb()
	}
	h.apiMan.InitAPI()
	log.Debug("Snapshot processed")
}

func (h *apigeeSyncHandler) startupOnExistingDatabase() {
	// start bundle downloads that didn't finish
	go func() {
		// create apid_blob_available table
		h.dbMan.initDb()
		blobIds, err := h.dbMan.getUnreadyBlobs()

		if err != nil {
			log.Panicf("unable to query database for unready deployments: %v", err)
		}

		log.Debugf("Queuing %d blob downloads", len(blobIds))
		for _, id := range blobIds {
			go h.bundleMan.enqueueRequest(h.bundleMan.makeDownloadRequest(id, nil))
		}
	}()
}

func (h *apigeeSyncHandler) processChangeList(changes *common.ChangeList) {

	log.Debugf("Processing changes")
	// changes have been applied to DB by apidApigeeSync
	var insertedConfigs, updatedNewConfigs, updatedOldConfigs, deletedConfigs []*Configuration
	isConfigChanged := false
	for _, change := range changes.Changes {
		switch change.Table {
		case CONFIG_METADATA_TABLE:
			isConfigChanged = true
			switch change.Operation {
			case common.Insert:
				dep := dataDeploymentFromRow(change.NewRow)
				insertedConfigs = append(insertedConfigs, &dep)
			case common.Delete:
				dep := dataDeploymentFromRow(change.OldRow)
				deletedConfigs = append(deletedConfigs, &dep)
			case common.Update:
				depNew := dataDeploymentFromRow(change.NewRow)
				depOld := dataDeploymentFromRow(change.OldRow)
				updatedNewConfigs = append(updatedNewConfigs, &depNew)
				updatedOldConfigs = append(updatedOldConfigs, &depOld)
			default:
				log.Errorf("unexpected operation: %s", change.Operation)
			}
		}
	}
	// delete old configs from FS
	if len(deletedConfigs)+len(updatedOldConfigs) > 0 {
		log.Debugf("will delete %d old blobs", len(deletedConfigs)+len(updatedOldConfigs))
		//TODO delete blobs for deleted configs
		go h.bundleMan.deleteBlobsFromConfigs(append(deletedConfigs, updatedOldConfigs...))
	}

	// download and expose new configs
	if isConfigChanged {
		h.dbMan.updateLSN(changes.LastSequence)
		h.bundleMan.downloadBlobsForChangeList(append(insertedConfigs, updatedNewConfigs...), changes.LastSequence)
	} else if h.dbMan.getLSN() == InitLSN {
		h.dbMan.updateLSN(changes.LastSequence)
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
