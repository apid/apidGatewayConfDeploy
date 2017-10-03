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
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"
	"time"
)

const (
	blobStoreUri = "/blobs/{blobId}"
)

type bundleManagerInterface interface {
	initializeBundleDownloading()
	queueDownloadRequest(*pendingConfiguration)
	enqueueRequest(*DownloadRequest)
	makeDownloadRequest(id string, counter *int32) *DownloadRequest
	deleteBlobs([]string)
	Close()
}

type bundleManager struct {
	blobServerUrl             string
	dbMan                     dbManagerInterface
	concurrentDownloads       int
	markDeploymentFailedAfter time.Duration
	bundleRetryDelay          time.Duration
	bundleCleanupDelay        time.Duration
	downloadQueue             chan *DownloadRequest
	isClosed                  *int32
	workers                   []*BundleDownloader
	client                    *http.Client
	configEtag *ConfigurationsEtagCache
}

type blobServerResponse struct {
	Id                       string `json:"id"`
	Kind                     string `json:"kind"`
	Self                     string `json:"self"`
	SignedUrl                string `json:"signedurl"`
	SignedUrlExpiryTimestamp string `json:"signedurlexpirytimestamp"`
}

type pendingConfiguration struct {
	dataDeployment *Configuration
	counter        int32
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

// download bundle blob and resource blob
// TODO do not download duplicate blobs
func (bm *bundleManager) queueDownloadRequest(conf *pendingConfiguration) {
	log.Debugf("enque pendingConfiguration: %s", conf.dataDeployment.ID)
	conf.counter = 0
	if conf.dataDeployment.BlobID != "" {
		conf.counter++
	}
	if conf.dataDeployment.BlobResourceID != "" {
		conf.counter++
	}
	if conf.dataDeployment.BlobID != "" {
		blobReq := bm.makeDownloadRequest(conf.dataDeployment.BlobID, &conf.counter)
		go bm.enqueueRequest(blobReq)
	}
	if conf.dataDeployment.BlobResourceID != "" {
		resourceReq := bm.makeDownloadRequest(conf.dataDeployment.BlobResourceID, &conf.counter)
		go bm.enqueueRequest(resourceReq)
	}
}


func (bm *bundleManager) makeDownloadRequest(id string, counter *int32) *DownloadRequest {
	if id=="" {
		return nil
	}
	markFailedAt := time.Now().Add(bm.markDeploymentFailedAfter)
	retryIn := bm.bundleRetryDelay
	maxBackOff := 5 * time.Minute

	return &DownloadRequest{
		blobServerURL: bm.blobServerUrl,
		bm:            bm,
		blobId:        id,
		backoffFunc:   createBackoff(retryIn, maxBackOff),
		markFailedAt:  markFailedAt,
		client:        bm.client,
		counter:       counter,
	}
}

// a blocking method to enqueue download requests
func (bm *bundleManager) enqueueRequest(r *DownloadRequest) {
	if atomic.LoadInt32(bm.isClosed) == 1 {
		return
	}
	bm.downloadQueue <- r
}

func (bm *bundleManager) Close() {
	atomic.StoreInt32(bm.isClosed, 1)
	close(bm.downloadQueue)
}

func (bm *bundleManager) deleteBlobs (ids []string) {
	// TODO Delete from the Database table apid_blob_available, reference counting

	// TODO Delete from local file system
}



type DownloadRequest struct {
	bm            *bundleManager
	blobId        string
	backoffFunc   func()
	markFailedAt  time.Time
	blobServerURL string
	client        *http.Client
	counter       *int32
	dep           *Configuration
}

func (r *DownloadRequest) downloadBundle() error {

	log.Debugf("starting bundle download attempt for blobId=%s", r.blobId)

	if r.checkTimeout() {
		return &timeoutError{
			markFailedAt: r.markFailedAt,
		}
	}

	cleanTempFile := func(file string) {
		if os.Remove(file) != nil {
			log.Warnf("Unable to remove temp file %s", file)
		}
	}

	downloadedFile, err := downloadFromURI(r.client, r.blobServerURL, r.blobId)

	if err != nil {
		log.Errorf("Unable to download blob file blobId=%s err:%v", r.blobId, err)
		if downloadedFile != "" {
			go cleanTempFile(downloadedFile)
		}
		return err
	}

	// succeeded

	log.Debugf("blob downloaded. blobid=%s  filepath=%s", r.blobId, downloadedFile)

	err = r.bm.dbMan.updateLocalFsLocation(r.blobId, downloadedFile)
	if err != nil {
		log.Errorf("updateLocalFsLocation failed: blobId=%s", r.blobId)
		if downloadedFile != "" {
			go cleanTempFile(downloadedFile)
		}
		return err
	}

	log.Debugf("blob inserted: blobId=%s filename=%s", r.blobId, downloadedFile)

	// if all required blobs have been downloaded
	if atomic.AddInt32(r.counter, -1) == 0 && r.bm.configEtag!=nil {
		go r.bm.configEtag.Insert(r.dep)
	}

	return nil
}

func (r *DownloadRequest) checkTimeout() bool {

	if !r.markFailedAt.IsZero() && time.Now().After(r.markFailedAt) {
		r.markFailedAt = time.Time{}
		log.Debugf("bundle download timeout. blobId=", r.blobId)
		// TODO notify gateway of this failure
		return true
	}
	return false
}

func getBlobFilePath(blobId string) string {
	return path.Join(bundlePath, base64.StdEncoding.EncodeToString([]byte(blobId)))
}

func getSignedURL(client *http.Client, blobServerURL string, blobId string) (string, error) {

	blobUri, err := url.Parse(blobServerURL)
	if err != nil {
		log.Panicf("bad url value for config %s: %s", blobUri, err)
	}

	blobUri.Path += strings.Replace(blobStoreUri, "{blobId}", blobId, 1)
	parameters := url.Values{}
	parameters.Add("action", "GET")
	blobUri.RawQuery = parameters.Encode()

	uri := blobUri.String()

	surl, err := getUriReaderWithAuth(client, uri)
	if err != nil {
		log.Errorf("Unable to get signed URL from BlobServer %s: %v", uri, err)
		return "", err
	}
	defer surl.Close()

	body, err := ioutil.ReadAll(surl)
	if err != nil {
		log.Errorf("Invalid response from BlobServer for {%s} error: {%v}", uri, err)
		return "", err
	}
	res := blobServerResponse{}
	err = json.Unmarshal(body, &res)
	if err != nil {
		log.Errorf("Invalid response from BlobServer for {%s} error: {%v}", uri, err)
		return "", err
	}

	return res.SignedUrl, nil
}

// downloadFromURI involves retrieving the signed URL for the blob, and storing the resource locally
// after downloading the resource from GCS (via the signed URL)
func downloadFromURI(client *http.Client, blobServerURL string, blobId string) (tempFileName string, err error) {

	var tempFile *os.File
	log.Debugf("Downloading bundle: %s", blobId)

	uri, err := getSignedURL(client, blobServerURL, blobId)
	if err != nil {
		log.Errorf("Unable to get signed URL for blobId {%s}, error : {%v}", blobId, err)
		return
	}

	tempFile, err = ioutil.TempFile(bundlePath, "blob")
	if err != nil {
		log.Errorf("Unable to create temp file: %v", err)
		return
	}
	defer tempFile.Close()
	tempFileName = tempFile.Name()

	var confReader io.ReadCloser
	confReader, err = getUriReaderWithAuth(client, uri)
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
func getUriReaderWithAuth(client *http.Client, uriString string) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", uriString, nil)
	if err != nil {
		return nil, err
	}
	// add Auth
	req.Header.Add("Authorization", getBearerToken())
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
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
			log.Debugf("starting download blobId=%s", req.blobId)
			err := req.downloadBundle()
			if err != nil {
				// timeout
				if _, ok := err.(*timeoutError); ok {
					continue
				}
				go func(r *DownloadRequest, bm *bundleManager) {
					r.backoffFunc()
					bm.enqueueRequest(r)
				}(req, w.bm)
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

type timeoutError struct {
	markFailedAt time.Time
}

func (e *timeoutError) Error() string {
	return fmt.Sprintf("Timeout. markFailedAt=%v", e.markFailedAt)
}
