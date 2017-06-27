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
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync/atomic"
	"time"
)

const (
	BLOBSTORE_URI = "/v1/blobstore/signeduri"
)

type bundleManagerInterface interface {
	initializeBundleDownloading()
	queueDownloadRequest(*DataDeployment)
	enqueueRequest(*DownloadRequest)
	deleteBundles([]DataDeployment)
	Close()
}

type bundleManager struct {
	dbMan                     dbManagerInterface
	apiMan                    apiManagerInterface
	concurrentDownloads       int
	markDeploymentFailedAfter time.Duration
	bundleDownloadConnTimeout time.Duration
	bundleRetryDelay          time.Duration
	bundleCleanupDelay        time.Duration
	downloadQueue             chan *DownloadRequest
	isClosed                  *int32
	workers                   []*BundleDownloader
}

func (bm *bundleManager) initializeBundleDownloading() {
	atomic.StoreInt32(bm.isClosed, 0)
	bm.workers = make([]*BundleDownloader, bm.concurrentDownloads)

	// create workers
	for i := 0; i < bm.concurrentDownloads; i++ {
		worker := BundleDownloader{
			id:       i + 1,
			workChan: make(chan *DownloadRequest),
			bm:       bm,
		}
		bm.workers[i] = &worker
		worker.Start()
	}
}

func (bm *bundleManager) queueDownloadRequest(dep *DataDeployment) {

	retryIn := bm.bundleRetryDelay
	maxBackOff := 5 * time.Minute
	markFailedAt := time.Now().Add(bm.markDeploymentFailedAfter)
	req := &DownloadRequest{
		bm:           bm,
		dep:          dep,
		bundleFile:   getBundleFile(dep),
		backoffFunc:  createBackoff(retryIn, maxBackOff),
		markFailedAt: markFailedAt,
		connTimeout:  bm.bundleDownloadConnTimeout,
	}
	go bm.enqueueRequest(req)
}

// a blocking method to enqueue download requests
func (bm *bundleManager) enqueueRequest(r *DownloadRequest) {
	if atomic.LoadInt32(bm.isClosed) == 1 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Warn("trying to enque requests to closed bundleManager")
		}
	}()
	bm.downloadQueue <- r
}

func (bm *bundleManager) Close() {
	atomic.StoreInt32(bm.isClosed, 1)
	close(bm.downloadQueue)
}

func (bm *bundleManager) deleteBundles(deletedDeployments []DataDeployment) {
	log.Debugf("will delete %d old bundles", len(deletedDeployments))
	go func() {
		// give clients a minute to avoid conflicts
		time.Sleep(bm.bundleCleanupDelay)
		for _, dep := range deletedDeployments {
			bundleFile := getBundleFile(&dep)
			log.Debugf("removing old bundle: %v", bundleFile)
			// TODO Remove from the Database table edgex_blob_available
			safeDelete(bundleFile)
		}
	}()
}

type DownloadRequest struct {
	bm           *bundleManager
	dep          *DataDeployment
	bundleFile   string
	backoffFunc  func()
	markFailedAt time.Time
	connTimeout  time.Duration
}

func (r *DownloadRequest) downloadBundle() error {

	dep := r.dep
	log.Debugf("starting bundle download attempt for depId=%s: blobId=%s", dep.ID, dep.BlobID)

	r.checkTimeout()

	tempFile, err := downloadFromURI(dep.BlobID, r.connTimeout)

	if err != nil {
		log.Errorf("Unable to download blob file blobId=%s: %s", dep.BlobID, err)
		return err
	}

	defer func() {
		if tempFile != "" {
			go safeDelete(tempFile)
		}
	}()

	err = os.Rename(tempFile, r.bundleFile)
	if err != nil {
		log.Errorf("Unable to rename temp blob file %s to %s: %s", tempFile, r.bundleFile, err)
		return err
	}

	blobId := atomic.AddInt64(&gwBlobId, 1)
	blobIds := strconv.FormatInt(blobId, 10)
	err = r.bm.dbMan.updateLocalFsLocation(dep.ID, blobIds, r.bundleFile)
	if err != nil {
		return err
	}
	dep.GWBlobID = blobIds

	log.Debugf("bundle for depId=%s downloaded: blobId=%s", dep.ID, dep.BlobID)

	// send deployments to client
	r.bm.apiMan.addChangedDeployment(dep.ID)

	return nil
}

func (r *DownloadRequest) checkTimeout() {

	if !r.markFailedAt.IsZero() {
		if time.Now().After(r.markFailedAt) {
			r.markFailedAt = time.Time{}
			log.Debugf("bundle download timeout. marking deployment %s failed. will keep retrying: %s",
				r.dep.ID, r.dep.BlobID)
		}
	}

}

func getBundleFile(dep *DataDeployment) string {

	return path.Join(bundlePath, base64.StdEncoding.EncodeToString([]byte(dep.ID)))

}

func getSignedURL(blobId string, bundleDownloadConnTimeout time.Duration) (string, error) {

	blobUri, err := url.Parse(blobServerURL)
	if err != nil {
		log.Panicf("bad url value for config %s: %s", blobUri, err)
	}

	blobUri.Path += BLOBSTORE_URI
	parameters := url.Values{}
	parameters.Add("action", "GET")
	parameters.Add("key", blobId)
	blobUri.RawQuery = parameters.Encode()

	uri := blobUri.String()

	surl, err := getURIReader(uri, bundleDownloadConnTimeout)
	if err != nil {
		log.Errorf("Unable to get signed URL from BlobServer %s: %v", uri, err)
		return "", err
	}

	signedURL, err := ioutil.ReadAll(surl)
	if err != nil {
		log.Errorf("Invalid response from BlobServer for {%s} error: {%v}", uri, err)
		return "", err
	}
	return string(signedURL), nil
}

// downloadFromURI involves retrieving the signed URL for the blob, and storing the resource locally
// after downloading the resource from GCS (via the signed URL)
func downloadFromURI(blobId string, bundleDownloadConnTimeout time.Duration) (tempFileName string, err error) {

	var tempFile *os.File
	log.Debugf("Downloading bundle: %s", blobId)

	uri, err := getSignedURL(blobId, bundleDownloadConnTimeout)
	if err != nil {
		log.Errorf("Unable to get signed URL for blobId {%s}, error : {%v}", blobId, err)
		return
	}

	tempFile, err = ioutil.TempFile(bundlePath, "download")
	if err != nil {
		log.Errorf("Unable to create temp file: %v", err)
		return
	}
	defer tempFile.Close()
	tempFileName = tempFile.Name()

	var confReader io.ReadCloser
	confReader, err = getURIReader(uri, bundleDownloadConnTimeout)
	if err != nil {
		log.Errorf("Unable to retrieve bundle %s: %v", uri, err)
		return
	}
	defer confReader.Close()

	_, err = io.Copy(tempFile, confReader)
	if err != nil {
		log.Errorf("Unable to write bundle %s: %v", tempFileName, err)
		return
	}

	log.Debugf("Bundle %s downloaded to: %s", uri, tempFileName)
	return
}

// retrieveBundle retrieves bundle data from a URI
func getURIReader(uriString string, bundleDownloadConnTimeout time.Duration) (io.ReadCloser, error) {

	client := http.Client{
		Timeout: bundleDownloadConnTimeout,
	}
	res, err := client.Get(uriString)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("GET uri %s failed with status %d", uriString, res.StatusCode)
	}
	return res.Body, nil
}

type BundleDownloader struct {
	id       int
	workChan chan *DownloadRequest
	bm       *bundleManager
}

func (w *BundleDownloader) Start() {
	go func() {
		log.Debugf("started bundle downloader %d", w.id)

		for req := range w.bm.downloadQueue {
			log.Debugf("starting download %s", req.bundleFile)
			err := req.downloadBundle()
			if err != nil {
				go func() {
					req.backoffFunc()
					w.bm.enqueueRequest(req)
				}()
			}
		}
		log.Debugf("bundle downloader %d stopped", w.id)
	}()
}

// simple doubling back-off
func createBackoff(retryIn, maxBackOff time.Duration) func() {
	return func() {
		log.Debugf("backoff called. will retry in %s.", retryIn)
		time.Sleep(retryIn)
		retryIn = retryIn * time.Duration(2)
		if retryIn > maxBackOff {
			retryIn = maxBackOff
		}
	}
}
