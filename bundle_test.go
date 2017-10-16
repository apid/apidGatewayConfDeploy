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
	"net/http"

	"github.com/apid/apid-core/util"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mathrand "math/rand"
	"sync/atomic"
	"time"
)

const (
	bundleTestUrl       = "http://127.0.0.1:9000"
	dummySignedEndpoint = "/dummyblob/{blobId}"
)

var _ = Describe("api", func() {
	var testCount int
	var testBundleMan *bundleManager
	var dummyDbMan *dummyDbManager
	var dummyApiMan *dummyApiManager
	var blobServer *dummyBlobServer

	var _ = BeforeEach(func() {
		testCount += 1
		concurrentDownloads := 5
		downloadQueueSize := 5

		// init test blob server
		if blobServer == nil {
			blobServer = &dummyBlobServer{
				serverEndpoint: blobStoreUri,
				signedEndpoint: dummySignedEndpoint,
				signedTimeout:  new(int32),
				blobTimeout:    new(int32),
				resetTimeout:   true,
			}
			blobServer.start()
		}

		// init dummy db manager
		dummyDbMan = &dummyDbManager{
			fileResponse: make(chan string),
		}

		// init dummy api manager
		dummyApiMan = &dummyApiManager{
			notifyChan: make(chan bool, 1),
			initCalled: make(chan bool),
		}

		// init bundle manager
		testBundleMan = &bundleManager{
			blobServerUrl:         bundleTestUrl,
			dbMan:                 dummyDbMan,
			apiMan:                dummyApiMan,
			concurrentDownloads:   concurrentDownloads,
			markConfigFailedAfter: 5 * time.Second,
			bundleRetryDelay:      time.Second,
			bundleCleanupDelay:    5 * time.Second,
			downloadQueue:         make(chan *DownloadRequest, downloadQueueSize),
			isClosed:              new(int32),
			client: &http.Client{
				Timeout: time.Second,
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 10,
				},
			},
		}
		testBundleMan.initializeBundleDownloading()
		time.Sleep(100 * time.Millisecond)
	})

	var _ = AfterEach(func() {
		testBundleMan.Close()
		testBundleMan = nil
		dummyDbMan = nil
		dummyApiMan = nil
	})

	Context("download blobs", func() {

		It("should download blob according to id", func() {
			// download blob
			id := util.GenerateUUID()
			testBundleMan.enqueueRequest(testBundleMan.makeDownloadRequest(id, nil))
			received := <-dummyDbMan.fileResponse
			Expect(received).Should(Equal(id))
		})

		It("should timeout connection and retry", func() {
			// setup timeout
			atomic.StoreInt32(blobServer.signedTimeout, 1)
			atomic.StoreInt32(blobServer.blobTimeout, 1)
			testBundleMan.client.Timeout = 500 * time.Millisecond
			testBundleMan.bundleRetryDelay = 50 * time.Millisecond

			// download blobs
			id := util.GenerateUUID()
			testBundleMan.enqueueRequest(testBundleMan.makeDownloadRequest(id, nil))
			received := <-dummyDbMan.fileResponse
			Expect(received).Should(Equal(id))

		}, 4)

		It("should mark as failure according to markConfigFailedAfter", func() {
			// setup timeout
			atomic.StoreInt32(blobServer.signedTimeout, 1)
			atomic.StoreInt32(blobServer.blobTimeout, 1)
			testBundleMan.client.Timeout = 100 * time.Millisecond
			testBundleMan.bundleRetryDelay = 100 * time.Millisecond
			testBundleMan.markConfigFailedAfter = 200 * time.Millisecond

			// download blobs
			id := util.GenerateUUID()
			req := testBundleMan.makeDownloadRequest(id, nil)
			Expect(req.markFailedAt.After(time.Now())).Should(BeTrue())
			testBundleMan.enqueueRequest(req)

			// should fail
			time.Sleep(time.Second)
			Expect(req.markFailedAt.IsZero()).Should(BeTrue())
		}, 4)

		It("should call callback func after a round of download attempts", func() {
			// download blobs
			var ids []string
			num := 1 + mathrand.Intn(5)
			for i := 0; i < num; i++ {
				ids = append(ids, util.GenerateUUID())
			}
			finishChan := make(chan int)
			testBundleMan.downloadBlobsWithCallback(ids, func() {
				finishChan <- 1
			})
			for i := 0; i < num; i++ {
				<-dummyDbMan.fileResponse
			}
			<-finishChan
			// if there's no blob
			testBundleMan.downloadBlobsWithCallback(nil, func() {
				finishChan <- 1
			})
			<-finishChan
		}, 1)
	})

	Context("download blobs for changelist", func() {
		It("should download blobs for changelist", func() {
			//setup test data
			count := mathrand.Intn(10) + 1
			configs := make([]*Configuration, count)
			for i := 0; i < count; i++ {
				conf := makeTestDeployment()
				conf.BlobID = util.GenerateUUID()
				conf.BlobResourceID = util.GenerateUUID()
				configs[i] = conf
			}

			// should download blobs for changelist
			testBundleMan.downloadBlobsWithCallback(extractBlobsToDownload(configs), dummyApiMan.notifyNewChange)
			for i := 0; i < 2*count; i++ {
				<-dummyDbMan.fileResponse
			}

			// should notify after 1st download attempt
			<-dummyApiMan.notifyChan
		})

		It("should notify after 1st download attempt unless failure", func() {
			//setup test data
			count := mathrand.Intn(10) + 1
			configs := make([]*Configuration, count)
			for i := 0; i < count; i++ {
				conf := makeTestDeployment()
				conf.BlobID = util.GenerateUUID()
				conf.BlobResourceID = util.GenerateUUID()
				configs[i] = conf
			}

			// setup timeout
			atomic.StoreInt32(blobServer.signedTimeout, 1)
			atomic.StoreInt32(blobServer.blobTimeout, 1)
			testBundleMan.client.Timeout = 500 * time.Millisecond
			testBundleMan.bundleRetryDelay = 50 * time.Millisecond

			// should download blobs for changelist
			testBundleMan.downloadBlobsWithCallback(extractBlobsToDownload(configs), dummyApiMan.notifyNewChange)

			// should notify after 1st download attempt
			<-dummyApiMan.notifyChan

			//should retry download
			for i := 0; i < 2*count; i++ {
				<-dummyDbMan.fileResponse
			}
		})

	})

})
