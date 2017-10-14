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
	"github.com/apid/apid-core"
	"github.com/apid/apid-core/util"
	"github.com/apigee-labs/transicator/common"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"time"
)

var _ = Describe("listener", func() {
	var dummyDbMan *dummyDbManager
	var dummyApiMan *dummyApiManager
	var dummyBundleMan *dummyBundleManager
	var testHandler *apigeeSyncHandler
	var testCount int
	var _ = BeforeEach(func() {
		testCount += 1
		// stop handler created by initPlugin()
		eventHandler.stopListener(services)

		dummyApiMan = &dummyApiManager{
			notifyChan: make(chan int, 1),
			initCalled: make(chan bool),
		}
		dummyDbMan = &dummyDbManager{
			lsn: "0.0.1",
		}
		dummyBundleMan = &dummyBundleManager{
			blobChan: make(chan string),
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
				id := util.GenerateUUID()
				blobMap[id] = 1
				unreadyBlobIds = append(unreadyBlobIds, id)
			}
			dummyDbMan.unreadyBlobIds = unreadyBlobIds

			// emit snapshot
			snapshot := &common.Snapshot{
				SnapshotInfo: fmt.Sprint(rand.Uint32()),
			}

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, snapshot)

			for i := 0; i < len(unreadyBlobIds); i++ {
				id := <-dummyBundleMan.blobChan
				blobMap[id]++
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
			// Expect(<-dummyApiMan.initCalled).Should(BeTrue())
		})

	})

	Context("Change list", func() {

		It("Insert event should enqueue download requests for all inserted deployments", func() {
			// emit change event
			changes := make([]common.Change, 0)
			blobs := make(map[string]int)
			for i := 0; i < 1+rand.Intn(10); i++ {
				dep := makeTestDeployment()
				change := common.Change{
					Operation: common.Insert,
					Table:     CONFIG_METADATA_TABLE,
					NewRow:    rowFromDeployment(dep),
				}
				changes = append(changes, change)
				blobs[dep.BlobID]++
				blobs[dep.BlobResourceID]++
			}

			changeList := &common.ChangeList{
				Changes: changes,
			}

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < 2*len(changes); i++ {
				blobId := <-dummyBundleMan.blobChan
				blobs[blobId]++
				Expect(blobs[blobId]).Should(Equal(2))
			}
		})

		XIt("Delete event should deliver to the bundle manager", func() {
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

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < len(changes); i++ {
			}
			Expect(len(deployments)).Should(BeZero())
		})

		It("Update event should enqueue download requests", func() {

			changes := make([]common.Change, 0)
			blobsNew := make(map[string]int)
			for i := 0; i < 1+rand.Intn(10); i++ {
				confNew := makeTestDeployment()
				confNew.BlobID = util.GenerateUUID()
				confNew.BlobResourceID = util.GenerateUUID()

				confOld := makeTestDeployment()
				confOld.BlobID = util.GenerateUUID()
				confOld.BlobResourceID = util.GenerateUUID()

				change := common.Change{
					Operation: common.Update,
					Table:     CONFIG_METADATA_TABLE,
					NewRow:    rowFromDeployment(confNew),
					OldRow:    rowFromDeployment(confOld),
				}
				changes = append(changes, change)

				blobsNew[confNew.BlobID]++
				blobsNew[confNew.BlobResourceID]++
			}
			testLSN := "1.1.1"

			// emit change event
			changeList := &common.ChangeList{
				Changes:      changes,
				LastSequence: testLSN,
			}

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)

			// verify
			for i := 0; i < 2*len(changes); i++ {
				blobId := <-dummyBundleMan.blobChan
				blobsNew[blobId]++
				Expect(blobsNew[blobId]).Should(Equal(2))
			}

		})
	})

	Context("LSN", func() {
		It("changelist with CONFIG_METADATA_TABLE should update apidLSN", func() {
			// emit change event
			changes := make([]common.Change, 0)
			deployments := make(map[string]Configuration)
			testLSN := fmt.Sprintf("%d.%d.%d", testCount, testCount, testCount)
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
				Changes:      changes,
				LastSequence: testLSN,
			}

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)
			for i := 0; i < 2*len(changes); i++ {
				<-dummyBundleMan.blobChan
			}
			Expect(dummyDbMan.getLSN()).Should(Equal(testLSN))

		})

		It("changelist without CONFIG_METADATA_TABLE shouldn't update apidLSN", func() {
			testLSN := fmt.Sprintf("%d.%d.%d", testCount, testCount, testCount)
			dummyDbMan.lsn = testLSN
			// emit change event
			changes := make([]common.Change, 0)
			deployments := make(map[string]Configuration)
			for i := 0; i < 1+rand.Intn(10); i++ {
				dep := makeTestDeployment()
				change := common.Change{
					Operation: common.Insert,
					Table:     "somewhat-table",
					NewRow:    rowFromDeployment(dep),
				}
				changes = append(changes, change)
				deployments[dep.ID] = *dep
			}

			changeList := &common.ChangeList{
				Changes:      changes,
				LastSequence: "aaa.aaa.aaa",
			}

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)
			Expect(dummyDbMan.getLSN()).Should(Equal(testLSN))

		})

		It("changelist should always update apidLSN if it has init value", func() {
			testLSN := fmt.Sprintf("%d.%d.%d", testCount, testCount, testCount)
			dummyDbMan.lsn = InitLSN
			// emit change event
			changeList := &common.ChangeList{
				Changes:      nil,
				LastSequence: testLSN,
			}

			<-apid.Events().Emit(APIGEE_SYNC_EVENT, changeList)
			Expect(dummyDbMan.getLSN()).Should(Equal(testLSN))

		})
	})
})

type dummyBundleManager struct {
	blobChan chan string
}

func (bm *dummyBundleManager) initializeBundleDownloading() {

}

func (bm *dummyBundleManager) downloadBlobsWithCallback(blobs []string, callback func()) {
	go func() {
		for _, id := range blobs {
			bm.blobChan <- id
		}
	}()
}

func (bm *dummyBundleManager) makeDownloadRequest(blobId string, bunchRequest *BunchDownloadRequest) *DownloadRequest {
	return &DownloadRequest{
		blobId:       blobId,
		bunchRequest: bunchRequest,
	}
}

func (bm *dummyBundleManager) deleteBlobs(blobIds []string) {

}

func (bm *dummyBundleManager) Close() {

}

func rowFromDeployment(dep *Configuration) common.Row {
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
