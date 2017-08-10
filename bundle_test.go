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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"net/http"
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
		dummyApiMan = &dummyApiManager{}

		// init bundle manager
		testBundleMan = &bundleManager{
			blobServerUrl:             bundleTestUrl,
			dbMan:                     dummyDbMan,
			apiMan:                    dummyApiMan,
			concurrentDownloads:       concurrentDownloads,
			markDeploymentFailedAfter: 5 * time.Second,
			bundleRetryDelay:          time.Second,
			bundleCleanupDelay:        5 * time.Second,
			downloadQueue:             make(chan *DownloadRequest, downloadQueueSize),
			isClosed:                  new(int32),
			client: &http.Client{
				Timeout: time.Second,
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

	It("should download blob according to id", func() {
		// download blob
		id := GenerateUUID()
		testBundleMan.enqueueRequest(testBundleMan.makeDownloadRequest(id))
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
		id := GenerateUUID()
		testBundleMan.enqueueRequest(testBundleMan.makeDownloadRequest(id))
		received := <-dummyDbMan.fileResponse
		Expect(received).Should(Equal(id))

	}, 4)

	It("should mark as failure according to markDeploymentFailedAfter", func() {
		// setup timeout
		atomic.StoreInt32(blobServer.signedTimeout, 1)
		atomic.StoreInt32(blobServer.blobTimeout, 1)
		testBundleMan.client.Timeout = 100 * time.Millisecond
		testBundleMan.bundleRetryDelay = 100 * time.Millisecond
		testBundleMan.markDeploymentFailedAfter = 200 * time.Millisecond

		// download blobs
		id := GenerateUUID()
		req := testBundleMan.makeDownloadRequest(id)
		Expect(req.markFailedAt.After(time.Now())).Should(BeTrue())
		testBundleMan.enqueueRequest(req)

		// should fail
		time.Sleep(time.Second)
		Expect(req.markFailedAt.IsZero()).Should(BeTrue())
	}, 4)
})
