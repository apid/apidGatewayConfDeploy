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
	"strings"
	"sync/atomic"
	"time"
)

type dummyDbManager struct {
	unreadyBlobIds   []string
	readyDeployments []Configuration
	localFSLocation  string
	fileResponse     chan string
	version          string
	configurations   map[string]*Configuration
	lsn              string
	dbLSN            string
	err              error
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

func (d *dummyDbManager) getAllConfigurations(typeFilter string) ([]Configuration, error) {
	if typeFilter == "" {
		return d.readyDeployments, nil
	}
	return []Configuration{*(d.configurations[typeFilter])}, nil
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
	go func(buff []byte) {
		d.fileResponse <- string(buff)
	}(buff)

	return nil
}

func (d *dummyDbManager) getLocalFSLocation(string) (string, error) {
	return d.localFSLocation, d.err
}

func (d *dummyDbManager) getConfigById(id string) (*Configuration, error) {
	return d.configurations[id], d.err
}
func (d *dummyDbManager) getLSN() string {
	return d.lsn
}

func (d *dummyDbManager) updateLSN(LSN string) error {
	d.lsn = LSN
	d.dbLSN = LSN
	return nil
}

func (d *dummyDbManager) loadLsnFromDb() error {
	d.lsn = d.dbLSN
	return nil
}

type dummyApiManager struct {
	initCalled chan bool
	notifyChan chan bool
}

func (a *dummyApiManager) InitAPI() {
	go func() {
		a.initCalled <- true
	}()
}

func (a *dummyApiManager) notifyNewChange() {
	a.notifyChan <- true
}

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
	go callback()
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
