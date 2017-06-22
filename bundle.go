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

var (
	markDeploymentFailedAfter time.Duration
	bundleDownloadConnTimeout time.Duration
	bundleRetryDelay          = time.Second
	downloadQueue             = make(chan *DownloadRequest, downloadQueueSize)
	workerQueue               = make(chan chan *DownloadRequest, concurrentDownloads)
)

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

func queueDownloadRequest(dep DataDeployment) {

	retryIn := bundleRetryDelay
	maxBackOff := 5 * time.Minute
	markFailedAt := time.Now().Add(markDeploymentFailedAfter)
	req := &DownloadRequest{
		dep:          dep,
		bundleFile:   getBundleFile(dep),
		backoffFunc:  createBackoff(retryIn, maxBackOff),
		markFailedAt: markFailedAt,
	}
	downloadQueue <- req
}

type DownloadRequest struct {
	dep          DataDeployment
	bundleFile   string
	backoffFunc  func()
	markFailedAt time.Time
}

func (r *DownloadRequest) downloadBundle() {

	dep := r.dep
	log.Debugf("starting bundle download attempt for %s: %s", dep.ID, dep.BlobID)

	r.checkTimeout()

	tempFile, err := downloadFromURI(dep.BlobID)

	if err == nil {
		err = os.Rename(tempFile, r.bundleFile)
		if err != nil {
			log.Errorf("Unable to rename temp bundle file %s to %s: %s", tempFile, r.bundleFile, err)
		}
	}

	if tempFile != "" {
		go safeDelete(tempFile)
	}

	if err == nil {
		blobId := atomic.AddInt64(&gwBlobId, 1)
		blobIds := strconv.FormatInt(blobId, 10)
		err = updatelocal_fs_location(dep.ID, blobIds, r.bundleFile)
		if err != nil {
			dep.GWBlobID = blobIds
		}
	}

	if err != nil {
		// add myself back into the queue after back off
		go func() {
			r.backoffFunc()
			downloadQueue <- r
		}()
		return
	}

	log.Debugf("bundle for %s downloaded: %s", dep.ID, dep.BlobID)

	// send deployments to client
	deploymentsChanged <- dep.ID
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

func getBundleFile(dep DataDeployment) string {

	return path.Join(bundlePath, base64.StdEncoding.EncodeToString([]byte(dep.ID)))

}

func getSignedURL(blobId string) (string, error) {

	blobUri, err := url.Parse(config.GetString(configBlobServerBaseURI))
	if err != nil {
		log.Panicf("bad url value for config %s: %s", blobUri, err)
	}

	blobUri.Path += BLOBSTORE_URI
	parameters := url.Values{}
	parameters.Add("action", "GET")
	parameters.Add("key", blobId)
	blobUri.RawQuery = parameters.Encode()

	uri := blobUri.String()

	surl, err := getURIReader(uri)
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
func downloadFromURI(blobId string) (tempFileName string, err error) {

	var tempFile *os.File
	log.Debugf("Downloading bundle: %s", blobId)

	uri, err := getSignedURL(blobId)
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
	confReader, err = getURIReader(uri)
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
func getURIReader(uriString string) (io.ReadCloser, error) {

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

func initializeBundleDownloading() {

	// create workers
	for i := 0; i < concurrentDownloads; i++ {
		worker := BundleDownloader{
			id:       i + 1,
			workChan: make(chan *DownloadRequest),
			quitChan: make(chan bool),
		}
		worker.Start()
	}

	// run dispatcher
	go func() {
		for {
			select {
			case req := <-downloadQueue:
				log.Debugf("dispatching downloader for: %s", req.bundleFile)
				go func() {
					worker := <-workerQueue
					log.Debugf("got a worker for: %s", req.bundleFile)
					worker <- req
				}()
			}
		}
	}()
}

type BundleDownloader struct {
	id       int
	workChan chan *DownloadRequest
	quitChan chan bool
}

func (w *BundleDownloader) Start() {
	go func() {
		log.Debugf("started bundle downloader %d", w.id)
		for {
			// wait for work
			workerQueue <- w.workChan

			select {
			case req := <-w.workChan:
				log.Debugf("starting download %s", req.bundleFile)
				req.downloadBundle()

			case <-w.quitChan:
				log.Debugf("bundle downloader %d stopped", w.id)
				return
			}
		}
	}()
}

func (w *BundleDownloader) Stop() {
	go func() {
		w.quitChan <- true
	}()
}
