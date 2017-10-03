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

	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io"
	"strings"
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
			concurrentDownloads:       concurrentDownloads,
			markDeploymentFailedAfter: 5 * time.Second,
			bundleRetryDelay:          time.Second,
			bundleCleanupDelay:        5 * time.Second,
			downloadQueue:             make(chan *DownloadRequest, downloadQueueSize),
			isClosed:                  new(int32),
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

type dummyApiManager struct {
	initCalled bool
}

func (a *dummyApiManager) InitAPI() {
	a.initCalled = true
}

type dummyBlobServer struct {
	serverEndpoint string
	signedEndpoint string
	signedTimeout  *int32
	blobTimeout    *int32
	resetTimeout   bool
}

func (b *dummyBlobServer) start() {
	services.API().HandleFunc(b.serverEndpoint, b.returnSigned).Methods("GET")
	services.API().HandleFunc(b.signedEndpoint, b.returnBlob).Methods("GET")
}

// send a dummy uri as response
func (b *dummyBlobServer) returnSigned(w http.ResponseWriter, r *http.Request) {
	defer GinkgoRecover()
	if atomic.LoadInt32(b.signedTimeout) == int32(1) {
		if b.resetTimeout {
			atomic.StoreInt32(b.signedTimeout, 0)
		}
		time.Sleep(time.Second)
	}
	vars := mux.Vars(r)
	blobId := vars["blobId"]

	uriString := strings.Replace(bundleTestUrl+b.signedEndpoint, "{blobId}", blobId, 1)
	log.Debug("dummyBlobServer returnSigned: " + uriString)

	res := blobServerResponse{
		Id:                       blobId,
		Kind:                     "Blob",
		Self:                     r.RequestURI,
		SignedUrl:                uriString,
		SignedUrlExpiryTimestamp: time.Now().Add(3 * time.Hour).Format(time.RFC3339),
	}

	resBytes, err := json.Marshal(res)
	Expect(err).Should(Succeed())
	_, err = io.Copy(w, bytes.NewReader(resBytes))
	Expect(err).Should(Succeed())
	w.Header().Set("Content-Type", headerSteam)
}

// send blobId back as response
func (b *dummyBlobServer) returnBlob(w http.ResponseWriter, r *http.Request) {
	defer GinkgoRecover()
	if atomic.LoadInt32(b.blobTimeout) == int32(1) {
		if b.resetTimeout {
			atomic.StoreInt32(b.blobTimeout, 0)
		}
		time.Sleep(time.Second)
	}
	vars := mux.Vars(r)
	blobId := vars["blobId"]
	log.Debug("dummyBlobServer returnBlob id=" + blobId)
	_, err := io.Copy(w, bytes.NewReader([]byte(blobId)))
	Expect(err).Should(Succeed())
	w.Header().Set("Content-Type", headerSteam)
}
