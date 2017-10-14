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
	err := h.dbMan.initDb()
	if err != nil {
		log.Panicf("unable to init DB: %v", err)
	}

	if lsn := h.dbMan.getLSN(); lsn != "" {
		h.dbMan.updateLSN(lsn)
	} else { //apid just started
		h.dbMan.loadLsnFromDb()
	}
	h.startupOnExistingDatabase()
	//h.apiMan.InitAPI()
	log.Debug("Snapshot processed")
}

func (h *apigeeSyncHandler) startupOnExistingDatabase() {
	// start bundle downloads that didn't finish

	go func() {
		// create apid_blob_available table

		blobIds, err := h.dbMan.getUnreadyBlobs()

		if err != nil {
			log.Panicf("unable to query database for unready configurations: %v", err)
		}

		log.Debugf("Queuing %d blob downloads", len(blobIds))

		// initialize API endpoints only after 1 round of download attempts is made
		h.bundleMan.downloadBlobsWithCallback(blobIds, func() {
			h.apiMan.InitAPI()
			h.apiMan.notifyNewChange()
		})

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
				conf := configurationFromRow(change.NewRow)
				insertedConfigs = append(insertedConfigs, &conf)
			case common.Delete:
				conf := configurationFromRow(change.OldRow)
				deletedConfigs = append(deletedConfigs, &conf)
			case common.Update:
				confNew := configurationFromRow(change.NewRow)
				confOld := configurationFromRow(change.OldRow)
				updatedNewConfigs = append(updatedNewConfigs, &confNew)
				updatedOldConfigs = append(updatedOldConfigs, &confOld)
			default:
				log.Errorf("unexpected operation: %s", change.Operation)
			}
		}
	}
	// delete old configs from FS
	if len(deletedConfigs)+len(updatedOldConfigs) > 0 {
		log.Debugf("will delete %d old blobs", len(deletedConfigs)+len(updatedOldConfigs))
		//TODO delete blobs for deleted configs
		blobIds := extractBlobsToDelete(append(deletedConfigs, updatedOldConfigs...))
		go h.bundleMan.deleteBlobs(blobIds)
	}

	// download and expose new configs
	if isConfigChanged {
		h.dbMan.updateLSN(changes.LastSequence)
		blobs := extractBlobsToDownload(append(insertedConfigs, updatedNewConfigs...))
		h.bundleMan.downloadBlobsWithCallback(blobs, h.apiMan.notifyNewChange)
	} else if h.dbMan.getLSN() == InitLSN {
		h.dbMan.updateLSN(changes.LastSequence)
	}

}

func extractBlobsToDownload(confs []*Configuration) (blobs []string) {
	//TODO: do not include already-downloaded blobs
	for _, conf := range confs {
		if conf.BlobID != "" {
			blobs = append(blobs, conf.BlobID)
		}
		if conf.BlobResourceID != "" {
			blobs = append(blobs, conf.BlobResourceID)
		}
	}
	return
}

func extractBlobsToDelete(confs []*Configuration) (blobs []string) {
	//TODO: do not include already-downloaded blobs
	for _, conf := range confs {
		if conf.BlobID != "" {
			blobs = append(blobs, conf.BlobID)
		}
		if conf.BlobResourceID != "" {
			blobs = append(blobs, conf.BlobResourceID)
		}
	}
	return
}

func configurationFromRow(row common.Row) (c Configuration) {

	row.Get("id", &c.ID)
	row.Get("organization_id", &c.OrgID)
	row.Get("environment_id", &c.EnvID)
	row.Get("bean_blob_id", &c.BlobID)
	row.Get("resource_blob_id", &c.BlobResourceID)
	row.Get("type", &c.Type)
	row.Get("name", &c.Name)
	row.Get("revision", &c.Revision)
	row.Get("path", &c.Path)
	row.Get("created_at", &c.Created)
	row.Get("created_by", &c.CreatedBy)
	row.Get("updated_at", &c.Updated)
	row.Get("updated_by", &c.UpdatedBy)

	return
}

// TODO delete from file system
func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
