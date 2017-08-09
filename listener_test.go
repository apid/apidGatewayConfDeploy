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
	"fmt"
	"github.com/30x/apid-core"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"reflect"
	"time"
)

var _ = Describe("listener", func() {
	var dummyDbMan *dummyDbManager
	var dummyApiMan *dummyApiManager
	var dummyBundleMan *dummyBundleManager
	var testHandler *apigeeSyncHandler
	var _ = BeforeEach(func() {
		// stop handler created by initPlugin()
		eventHandler.stopListener(services)

		dummyApiMan = &dummyApiManager{}
		dummyDbMan = &dummyDbManager{}
		dummyBundleMan = &dummyBundleManager{
			requestChan: make(chan *DownloadRequest),
			depChan:     make(chan *DataDeployment),
			delChan:     make(chan *DataDeployment),
			delBlobChan: make(chan string),
		}
		testHandler = &apigeeSyncHandler{
			dbMan:     dummyDbMan,
			apiMan:    dummyApiMan,
			bundleMan: dummyBundleMan,
		}
		testHandler.initListener(services)
		time.Sleep(100 * time.Millisecond)
	})

	var _ = AfterEach(func() {
		testHandler.stopListener(services)
	})
	Context("Snapshot", func() {

		It("Snapshot event should enqueue download requests for all unready blobs", func() {
			// init unready blob ids
			unreadyBlobIds := make([]string, 0)
			blobMap := make(map[string]int)
			for i := 0; i < 1+rand.Intn(10); i++ {
				id := GenerateUUID()
				blobMap[id] = 1
				unreadyBlobIds = append(unreadyBlobIds, id)
			}
			dummyDbMan.unreadyBlobIds = unreadyBlobIds

			// emit snapshot
			snapshot := &common.Snapshot{
				SnapshotInfo: fmt.Sprint(rand.Uint32()),
			}

			apid.Events().Emit(APIGEE_SYNC_EVENT, snapshot)

			for i := 0; i < len(unreadyBlobIds); i++ {
				req := <-dummyBundleMan.requestChan
				blobMap[req.blobId]++
			}

			// verify all unready blobids are enqueued
			for _, val := range blobMap {
				Expect(val).Should(Equal(2))
			}
		})

		It("Snapshot events should set db version, and should only init API endpoint once", func() {

			// emit snapshot
			for i := 0; i < 2+rand.Intn(5); i++ {
				version := fmt.Sprint(rand.Uint32())
				snapshot := &common.Snapshot{
					SnapshotInfo: version,
				}
				<-apid.Events().Emit(APIGEE_SYNC_EVENT, snapshot)
				Expect(dummyDbMan.version).Should(Equal(version))
			}

			// verify init API called
			Expect(dummyApiMan.initCalled).Should(BeTrue())
		})

	})

	Context("Change list", func() {

		It("Insert event should enqueue download requests for all inserted deployments", func() {
			// emit change event
			changes := make([]common.Change, 0)
			deployments := make(map[string]DataDeployment)
			for i := 0; i < 1+rand.Intn(10); i++ {
				dep := makeTestDeployment()
				change := common.Change{
					Operation: common.Insert,
					Table:     CONFIG_METADATA_TABLE,
					NewRow:    rowFromDeployment(dep),
				}
				changes = append(changes, change)
				deployments[dep.ID] = *dep
			}

			changeList := &common.ChangeList{
				Changes: changes,
			}

			apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < len(changes); i++ {
				dep := <-dummyBundleMan.depChan
				Expect(reflect.DeepEqual(deployments[dep.ID], *dep)).Should(BeTrue())
				delete(deployments, dep.ID)
			}
			Expect(len(deployments)).Should(BeZero())
		})

		It("Delete event should deliver to the bundle manager", func() {
			// emit change event
			changes := make([]common.Change, 0)
			deployments := make(map[string]bool)
			for i := 0; i < 1+rand.Intn(10); i++ {
				dep := makeTestDeployment()
				change := common.Change{
					Operation: common.Delete,
					Table:     CONFIG_METADATA_TABLE,
					OldRow:    rowFromDeployment(dep),
				}
				changes = append(changes, change)
				deployments[dep.ID] = true
			}

			changeList := &common.ChangeList{
				Changes: changes,
			}

			apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < len(changes); i++ {
				dep := <-dummyBundleMan.delChan
				Expect(deployments[dep.ID]).Should(BeTrue())
				delete(deployments, dep.ID)
			}
			Expect(len(deployments)).Should(BeZero())
		})

		It("Update event should enqueue download requests and delete old blobs", func() {

			changes := make([]common.Change, 0)
			blobIdNew := make(map[string]int)
			blobIdOld := make(map[string]int)
			for i := 0; i < 1+rand.Intn(10); i++ {
				depNew := makeTestDeployment()
				depNew.BlobID = GenerateUUID()
				depNew.BlobResourceID = GenerateUUID()

				depOld := makeTestDeployment()
				depOld.BlobID = GenerateUUID()
				depOld.BlobResourceID = GenerateUUID()

				change := common.Change{
					Operation: common.Update,
					Table:     CONFIG_METADATA_TABLE,
					NewRow:    rowFromDeployment(depNew),
					OldRow:    rowFromDeployment(depOld),
				}
				changes = append(changes, change)

				blobIdNew[depNew.BlobID]++
				blobIdNew[depNew.BlobResourceID]++
				blobIdOld[depOld.BlobID]++
				blobIdOld[depOld.BlobResourceID]++
			}

			// emit change event
			changeList := &common.ChangeList{
				Changes: changes,
			}

			apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < len(blobIdNew); i++ {
				req := <-dummyBundleMan.requestChan
				blobIdNew[req.blobId]++
				Expect(blobIdNew[req.blobId]).Should(Equal(2))
			}
			for i := 0; i < len(blobIdOld); i++ {
				blobId := <-dummyBundleMan.delBlobChan
				blobIdOld[blobId]++
				Expect(blobIdOld[blobId]).Should(Equal(2))
			}

		})

		It("Update event should only download/delete changed blobs", func() {
			changes := make([]common.Change, 0)
			blobIdChangedNew := make(map[string]int)
			blobIdChangedOld := make(map[string]int)

			for i := 0; i < 1+rand.Intn(10); i++ {
				depNew := makeTestDeployment()
				depNew.BlobID = GenerateUUID()
				depNew.BlobResourceID = GenerateUUID()

				depOld := makeTestDeployment()

				if rand.Intn(2) == 0 {
					// blob id changed
					depOld.BlobID = GenerateUUID()
					blobIdChangedNew[depNew.BlobID]++
					blobIdChangedOld[depOld.BlobID]++
				} else {
					// blob id unchanged
					depOld.BlobID = depNew.BlobID
				}

				if rand.Intn(2) == 0 {
					// blob id changed
					depOld.BlobResourceID = GenerateUUID()
					blobIdChangedNew[depNew.BlobResourceID]++
					blobIdChangedOld[depOld.BlobResourceID]++
				} else {
					// blob id unchanged
					depOld.BlobResourceID = depNew.BlobResourceID
				}

				change := common.Change{
					Operation: common.Update,
					Table:     CONFIG_METADATA_TABLE,
					NewRow:    rowFromDeployment(depNew),
					OldRow:    rowFromDeployment(depOld),
				}
				changes = append(changes, change)
			}

			// emit change event
			changeList := &common.ChangeList{
				Changes: changes,
			}

			apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < len(blobIdChangedNew); i++ {
				req := <-dummyBundleMan.requestChan
				blobIdChangedNew[req.blobId]++
				Expect(blobIdChangedNew[req.blobId]).Should(Equal(2))
			}
			for i := 0; i < len(blobIdChangedOld); i++ {
				blobId := <-dummyBundleMan.delBlobChan
				blobIdChangedOld[blobId]++
				Expect(blobIdChangedOld[blobId]).Should(Equal(2))
			}
		})
	})
})

func rowFromDeployment(dep *DataDeployment) common.Row {
	row := common.Row{}
	row["id"] = &common.ColumnVal{Value: dep.ID}
	row["organization_id"] = &common.ColumnVal{Value: dep.OrgID}
	row["environment_id"] = &common.ColumnVal{Value: dep.EnvID}
	row["bean_blob_id"] = &common.ColumnVal{Value: dep.BlobID}
	row["resource_blob_id"] = &common.ColumnVal{Value: dep.BlobResourceID}
	row["type"] = &common.ColumnVal{Value: dep.Type}
	row["name"] = &common.ColumnVal{Value: dep.Name}
	row["revision"] = &common.ColumnVal{Value: dep.Revision}
	row["path"] = &common.ColumnVal{Value: dep.Path}
	row["created_at"] = &common.ColumnVal{Value: dep.Created}
	row["created_by"] = &common.ColumnVal{Value: dep.CreatedBy}
	row["updated_at"] = &common.ColumnVal{Value: dep.Updated}
	row["updated_by"] = &common.ColumnVal{Value: dep.UpdatedBy}

	return row
}
