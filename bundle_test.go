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
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io"
	"strings"
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
			bundleDownloadConnTimeout: 5 * time.Second,
			bundleRetryDelay:          time.Second,
			bundleCleanupDelay:        5 * time.Second,
			downloadQueue:             make(chan *DownloadRequest, downloadQueueSize),
			isClosed:                  new(int32),
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
		id := GenerateUUID()
		testBundleMan.enqueueRequest(testBundleMan.makeDownloadRequest(id))
		received := <-dummyDbMan.fileResponse
		Expect(received).Should(Equal(id))
	})
})

type dummyApiManager struct {
}

func (a *dummyApiManager) InitAPI() {
}

type dummyBlobServer struct {
	serverEndpoint string
	signedEndpoint string
}

func (b *dummyBlobServer) start() {
	services.API().HandleFunc(b.serverEndpoint, b.returnSigned).Methods("GET")
	services.API().HandleFunc(b.signedEndpoint, b.returnBlob).Methods("GET")
}

// send a dummy uri as response
func (b *dummyBlobServer) returnSigned(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blobId := vars["blobId"]

	uriString := strings.Replace(bundleTestUrl+b.signedEndpoint, "{blobId}", blobId, 1)
	log.Debug("dummyBlobServer returnSigned: " + uriString)
	_, err := io.Copy(w, bytes.NewReader([]byte(uriString)))
	Expect(err).Should(Succeed())
	w.Header().Set("Content-Type", headerSteam)
}

// send blobId back as response
func (b *dummyBlobServer) returnBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blobId := vars["blobId"]
	log.Debug("dummyBlobServer returnBlob id=" + blobId)
	_, err := io.Copy(w, bytes.NewReader([]byte(blobId)))
	Expect(err).Should(Succeed())
	w.Header().Set("Content-Type", headerSteam)
}
