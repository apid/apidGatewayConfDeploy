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
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// todo: the full set of states should probably be RECEIVED, READY, FAIL, SUCCESS
const (
	RESPONSE_STATUS_SUCCESS = "SUCCESS"
	RESPONSE_STATUS_FAIL    = "FAIL"
)

const (
	TRACKER_ERR_BUNDLE_DOWNLOAD_TIMEOUT = iota + 1
)

const (
	deploymentsEndpoint = "/configurations"
	blobEndpointPath    = "/blobs"
	blobEndpoint        = blobEndpointPath + "/{blobId}"
)

const (
	API_ERR_BAD_BLOCK = iota + 1
	API_ERR_INTERNAL
)

const (
	sqlTimeFormat    = "2006-01-02 15:04:05.999 -0700 MST"
	iso8601          = "2006-01-02T15:04:05.999Z07:00"
	sqliteTimeFormat = "2006-01-02 15:04:05.999-07:00"
	changeTimeFormat = "2006-01-02 15:04:05.999"
)

const (
	kindCollection = "Collection"
)

const (
	headerSteam = "application/octet-stream"
)

type deploymentsResult struct {
	deployments []Configuration
	err         error
	eTag        string
}

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

type ApiDeploymentDetails struct {
	Self            string `json:"self"`
	Name            string `json:"name"`
	Type            string `json:"type"`
	Revision        string `json:"revision"`
	BeanBlobUrl     string `json:"beanBlob"`
	Org             string `json:"orgId"`
	Env             string `json:"envId"`
	ResourceBlobUrl string `json:"resourceBlob"`
	Path            string `json:"path"`
	Created         string `json:"created"`
	Updated         string `json:"updated"`
}

type ApiDeploymentResponse struct {
	Kind                   string                 `json:"kind"`
	Self                   string                 `json:"self"`
	ApiDeploymentsResponse []ApiDeploymentDetails `json:"contents"`
}

//TODO add support for block and subscriber
type apiManagerInterface interface {
	InitDistributeEvents()
	InitAPI()
}

type apiManager struct {
	dbMan               dbManagerInterface
	deploymentsEndpoint string
	blobEndpoint        string
	addSubscriber       chan chan interface{}
	apiInitialized      bool
	configEtag *ConfigurationsEtagCache
}

func (a *apiManager) InitDistributeEvents(){
	go distributeEvents(a.configEtag.getChangeChannel(), a.addSubscriber)
}

func (a *apiManager) InitAPI() {
	if a.apiInitialized {
		return
	}
	services.API().HandleFunc(a.deploymentsEndpoint, a.apiGetConfigurations).Methods("GET")
	services.API().HandleFunc(a.blobEndpoint, a.apiReturnBlobData).Methods("GET")
	a.apiInitialized = true
	log.Debug("API endpoints initialized")
}

func (a *apiManager) writeError(w http.ResponseWriter, status int, code int, reason string) {
	w.WriteHeader(status)
	e := errorResponse{
		ErrorCode: code,
		Reason:    reason,
	}
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Errorf("unable to marshal errorResponse: %v", err)
	} else {
		w.Write(bytes)
	}
	log.Debugf("sending %d error to client: %s", status, reason)
}

func (a *apiManager) writeInternalError(w http.ResponseWriter, err string) {
	a.writeError(w, http.StatusInternalServerError, API_ERR_INTERNAL, err)
}






func (a *apiManager) apiReturnBlobData(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	blobId := vars["blobId"]
	fs, err := a.dbMan.getLocalFSLocation(blobId)
	if err != nil {
		a.writeInternalError(w, "BlobId "+blobId+" has no mapping blob file")
		return
	}
	byte, err := ioutil.ReadFile(fs)
	if err != nil {
		a.writeInternalError(w, err.Error())
		return
	}
	_, err = io.Copy(w, bytes.NewReader(byte))
	if err != nil {
		a.writeInternalError(w, err.Error())
	}
	w.Header().Set("Content-Type", headerSteam)

}

func (a *apiManager) apiGetConfigurations(w http.ResponseWriter, r *http.Request) {

	// If returning without a bundle (immediately or after timeout), status = 404
	// If returning If-None-Match value is equal to current deployment, status = 304
	// If returning a new value, status = 200

	// If timeout > 0 AND there is no deployment (or new deployment) available (per If-None-Match), then
	// block for up to the specified number of seconds until a new deployment becomes available.
	b := r.URL.Query().Get("block")
	var timeout int
	if b != "" {
		var err error
		timeout, err = strconv.Atoi(b)
		if err != nil || timeout<0{
			a.writeError(w, http.StatusBadRequest, API_ERR_BAD_BLOCK, "bad block value, must be number of seconds")
			return
		}
	}
	log.Debugf("api timeout: %d", timeout)

	// If If-None-Match header matches the ETag of current bundle list AND if the request does NOT have a 'block'
	// query param > 0, the server returns a 304 Not Modified response indicating that the client already has the
	// most recent bundle list.
	requestETag := r.Header.Get("Etag")
	log.Debugf("Etag: %s", requestETag)

	// send unmodified if matches prior eTag and no timeout
	eTag := a.getETag()
	if requestETag=="" || eTag != requestETag { // send results if different eTag
		a.sendReadyDeployments(w)
		return
	}

	if timeout == 0 { // non-blocking
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// long poll

	// subscribe to any new deployment changes
	ConfigChangeChan := make(chan interface{}, 1)
	a.addSubscriber <- ConfigChangeChan

	log.Debug("Blocking request... Waiting for new Deployments.")

	select {
	case <-ConfigChangeChan:
		// send configs and etag
		a.sendReadyDeployments(w)
	case <-time.After(time.Duration(timeout) * time.Second):
		log.Debug("Blocking configuration request timed out.")
		w.WriteHeader(http.StatusNotModified)
	}
}

func (a *apiManager) sendReadyDeployments(w http.ResponseWriter) {
	eTagConfig := a.configEtag.GetConfigsAndETag()
	a.sendDeployments(w, eTagConfig.Configs, eTagConfig.ETag)
}

func (a *apiManager) sendDeployments(w http.ResponseWriter, dataDeps []Configuration, eTag string) {

	apiDeps := ApiDeploymentResponse{}
	apiDepDetails := make([]ApiDeploymentDetails, 0)

	apiDeps.Kind = kindCollection
	apiDeps.Self = getHttpHost() + a.deploymentsEndpoint

	for _, d := range dataDeps {
		apiDepDetails = append(apiDepDetails, ApiDeploymentDetails{
			Self:            apiDeps.Self + "/" + d.ID,
			Name:            d.Name,
			Type:            d.Type,
			Revision:        d.Revision,
			BeanBlobUrl:     getBlobUrl(d.BlobID),
			Org:             d.OrgID,
			Env:             d.EnvID,
			ResourceBlobUrl: getBlobUrl(d.BlobResourceID),
			Path:            d.Path,
			Created:         convertTime(d.Created),
			Updated:         convertTime(d.Updated),
		})
	}
	apiDeps.ApiDeploymentsResponse = apiDepDetails

	b, err := json.Marshal(apiDeps)
	if err != nil {
		log.Errorf("unable to marshal deployments: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Debugf("sending deployments %s: %s", eTag, b)
	w.Header().Set("ETag", eTag)
	w.Write(b)
}


func (a *apiManager) getETag() string {
	if a.configEtag==nil {
		return ""
	}
	return a.configEtag.GetETag()
}

// escape the blobId into url
func getBlobUrl(blobId string) string {
	if blobId == "" {
		return ""
	}
	return getHttpHost() + "/blobs/" + url.PathEscape(blobId)
}

func convertTime(t string) string {
	if t == "" {
		return ""
	}
	formats := []string{sqliteTimeFormat, sqlTimeFormat, iso8601, time.RFC3339, changeTimeFormat}
	for _, f := range formats {
		timestamp, err := time.Parse(f, t)
		if err == nil {
			return timestamp.Format(iso8601)
		}
	}
	log.Error("convertTime: Unsupported time format: " + t)
	return t
}

func getHttpHost() string {

	configuredEndpoint := config.GetString(configBundleBlobDownloadEndpoint)
	if configuredEndpoint != "" {
		return configuredEndpoint
	}
	// apid-core has to set this according to the protocol apid is to be run: http/https
	proto := config.GetString(configProtocol)
	if proto == "" {
		proto = "http"
	}
	proto = proto + "://" + config.GetString(configAPIListen)
	return proto
}
