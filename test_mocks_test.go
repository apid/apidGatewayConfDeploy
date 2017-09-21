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
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

type dummyTrackerClient struct {
	code int
	args []string
}

func (d *dummyTrackerClient) putConfigStatus(reqBody *configStatusBody) *trackerResponse {

	return &trackerResponse{
		code:        d.code,
		contentType: "application/octet-stream",
		body:        []byte(concatenateFields(reqBody)),
	}
}

func (d *dummyTrackerClient) putRegister(uuid string, reqBody *registerBody) *trackerResponse {

	return &trackerResponse{
		code:        d.code,
		contentType: "application/octet-stream",
		body:        []byte(concatenateFields(reqBody)),
	}
}

func (d *dummyTrackerClient) putHeartbeat(uuid, reported string) *trackerResponse {
	return &trackerResponse{
		code:        d.code,
		contentType: "application/octet-stream",
		body:        []byte(uuid + " " + reported),
	}
}
func concatenateFields(s interface{}) string {
	v := reflect.ValueOf(s).Elem()
	return recursiveSerialize(v)
}

func recursiveSerialize(v reflect.Value) string {
	fields := []string{}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Type().Kind() == reflect.Slice {
			for j := 0; j < field.Len(); j++ {
				fields = append(fields, recursiveSerialize(field.Index(j)))
			}
		} else {
			fields = append(fields, field.String())
		}
	}
	return strings.Join(fields, " ")
}

type dummyDbManager struct {
	unreadyBlobIds   []string
	readyDeployments []DataDeployment
	localFSLocation  string
	fileResponse     chan string
	version          string
}

func (d *dummyDbManager) setDbVersion(version string) {
	d.version = version
}

func (d *dummyDbManager) initDb() error {
	return nil
}

func (d *dummyDbManager) getUnreadyBlobs() ([]string, error) {
	return d.unreadyBlobIds, nil
}

func (d *dummyDbManager) getReadyDeployments() ([]DataDeployment, error) {
	return d.readyDeployments, nil
}

func (d *dummyDbManager) updateLocalFsLocation(blobId, localFsLocation string) error {
	file, err := os.Open(localFsLocation)
	if err != nil {
		return err
	}
	buff := make([]byte, 36)
	_, err = file.Read(buff)
	if err != nil {
		return err
	}
	d.fileResponse <- string(buff)
	return nil
}

func (d *dummyDbManager) getLocalFSLocation(string) (string, error) {
	return d.localFSLocation, nil
}

type dummyBundleManager struct {
	requestChan chan *DownloadRequest
	depChan     chan *DataDeployment
	delChan     chan *DataDeployment
	delBlobChan chan string
}

func (bm *dummyBundleManager) initializeBundleDownloading() {

}

func (bm *dummyBundleManager) queueDownloadRequest(dep *DataDeployment) {
	bm.depChan <- dep
}

func (bm *dummyBundleManager) enqueueRequest(req *DownloadRequest) {
	bm.requestChan <- req
}

func (bm *dummyBundleManager) makeDownloadRequest(blobId string) *DownloadRequest {
	return &DownloadRequest{
		blobId: blobId,
	}
}

func (bm *dummyBundleManager) deleteBundlesFromDeployments(deployments []DataDeployment) {
	for i := range deployments {
		bm.delChan <- &deployments[i]
	}
}

func (bm *dummyBundleManager) deleteBundleById(blobId string) {
	bm.delBlobChan <- blobId
}

func (bm *dummyBundleManager) Close() {

}

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
