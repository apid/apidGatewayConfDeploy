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

		It("Snapshot event shoud enqueue download requests for all unready blobs", func() {
			// init unready blob ids
			unreadyBlobIds := make([]string, 0)
			blobMap := make(map[string]int)
			for i := 0; i < rand.Intn(10); i++ {
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

		It("Snapshot events shoud set db version, and should only init API endpoint once", func() {

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

})

type dummyBundleManager struct {
	requestChan chan *DownloadRequest
}

func (bm *dummyBundleManager) initializeBundleDownloading() {

}

func (bm *dummyBundleManager) queueDownloadRequest(dep *DataDeployment) {

}

func (bm *dummyBundleManager) enqueueRequest(req *DownloadRequest) {
	bm.requestChan <- req
}

func (bm *dummyBundleManager) makeDownloadRequest(blobId string) *DownloadRequest {
	return &DownloadRequest{
		blobId: blobId,
	}
}

func (bm *dummyBundleManager) Close() {

}
