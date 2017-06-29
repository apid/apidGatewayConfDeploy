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

	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
)

const (
	APIGEE_SYNC_EVENT     = "ApigeeSync"
	CONFIG_METADATA_TABLE = "project.runtime_blob_metadata"
)

var apiInitialized bool

func initListener(services apid.Services, dbMan dbManagerInterface, apiMan apiManagerInterface, bundleMan bundleManagerInterface) {
	handler := &apigeeSyncHandler{
		dbMan:     dbMan,
		apiMan:    apiMan,
		bundleMan: bundleMan,
	}

	services.Events().Listen(APIGEE_SYNC_EVENT, handler)
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
	if !apiInitialized {
		h.apiMan.InitAPI()
		apiInitialized = true
	}
	log.Debug("Snapshot processed")
}

// TODO make it work with new schema
func (h *apigeeSyncHandler) startupOnExistingDatabase() {
	// start bundle downloads that didn't finish
	go func() {
		// create edgex_blob_available table
		h.dbMan.initDb()
		blobIds, err := h.dbMan.getUnreadyBlobs()

		if err != nil {
			log.Panicf("unable to query database for unready deployments: %v", err)
		}

		log.Debugf("Queuing %d blob downloads", len(blobIds))
		for _, id := range blobIds {
			go h.bundleMan.enqueueRequest(h.bundleMan.makeDownloadRequest(id))
		}
	}()
}

func (h *apigeeSyncHandler) processChangeList(changes *common.ChangeList) {

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

	/*
		for _, d := range deletedDeployments {
			h.apiMan.addChangedDeployment(d.ID)
		}
	*/

	for _, dep := range insertedDeployments {
		go h.bundleMan.queueDownloadRequest(&dep)
	}

	// clean up old bundles
	if len(deletedDeployments) > 0 {
		log.Debugf("will delete %d old bundles", len(deletedDeployments))
		//TODO delete bundles for deleted deployments
		//h.bundleMan.deleteBundles(deletedDeployments)
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

// TODO delete from file system
func safeDelete(file string) {
	if e := os.Remove(file); e != nil && !os.IsNotExist(e) {
		log.Warnf("unable to delete file %s: %v", file, e)
	}
}
