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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apigee-labs/transicator/common"
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
	deploymentsEndpoint  = "/configurations"
	blobEndpointPath     = "/blobs"
	blobEndpoint         = blobEndpointPath + "/{blobId}"
	deploymentIdEndpoint = deploymentsEndpoint + "/{configId}"
)

const (
	API_ERR_BAD_BLOCK = iota + 1
	API_ERR_INTERNAL
	API_ERR_BAD_CONFIG_ID
	API_ERR_NOT_FOUND
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
	headerSteam           = "application/octet-stream"
	headerJson            = "application/json"
	apidConfigIndexPar    = "apid-config-index"
	apidConfigIndexHeader = "x-apid-config-index"
)

var (
	ErrNoLSN      = errors.New("No last sequence in DB")
	ErrInvalidLSN = errors.New(apidConfigIndexPar + " is invalid")
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
	InitAPI()
	notifyNewChangeList(newLSN string)
}

type apiManager struct {
	dbMan                dbManagerInterface
	deploymentsEndpoint  string
	blobEndpoint         string
	deploymentIdEndpoint string
	addSubscriber        chan chan interface{}
	newChangeListChan    chan interface{}
	apiInitialized       bool
}

func (a *apiManager) InitAPI() {
	if a.apiInitialized {
		return
	}
	services.API().HandleFunc(a.deploymentsEndpoint, a.apiGetCurrentConfigs).Methods("GET")
	services.API().HandleFunc(a.blobEndpoint, a.apiReturnBlobData).Methods("GET")
	services.API().HandleFunc(a.deploymentIdEndpoint, a.apiHandleConfigId).Methods("GET")
	a.initDistributeEvents()
	a.apiInitialized = true
	log.Debug("API endpoints initialized")
}

func (a *apiManager) initDistributeEvents() {
	go distributeEvents(a.newChangeListChan, a.addSubscriber)
}

func (a *apiManager) notifyNewChangeList(newLSN string) {
	a.newChangeListChan <- newLSN
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

func (a *apiManager) apiHandleConfigId(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	configId := vars["configId"]
	config, err := a.dbMan.getConfigById(configId)
	if err != nil {
		if err == sql.ErrNoRows {
			a.writeError(w, http.StatusNotFound, API_ERR_NOT_FOUND, "cannot find the configuration")
		} else {
			log.Errorf("apiHandleConfigId: %v", err)
			a.writeInternalError(w, err.Error())
		}
		return
	}
	configDetail := ApiDeploymentDetails{
		Self:            getHttpHost() + a.deploymentsEndpoint + "/" + config.ID,
		Name:            config.Name,
		Type:            config.Type,
		Revision:        config.Revision,
		BeanBlobUrl:     getBlobUrl(config.BlobID),
		Org:             config.OrgID,
		Env:             config.EnvID,
		ResourceBlobUrl: getBlobUrl(config.BlobResourceID),
		Path:            config.Path,
		Created:         convertTime(config.Created),
		Updated:         convertTime(config.Updated),
	}

	b, err := json.Marshal(configDetail)
	if err != nil {
		log.Errorf("unable to marshal config: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Debugf("sending configuration %s", b)
	w.Write(b)
}

func (a *apiManager) apiGetCurrentConfigs(w http.ResponseWriter, r *http.Request) {

	// If returning without a bundle (immediately or after timeout), status = 404
	// If returning If-None-Match value is equal to current deployment, status = 304
	// If returning a new value, status = 200

	// If timeout > 0 AND there is no deployment (or new deployment) available (per If-None-Match), then
	// block for up to the specified number of seconds until a new deployment becomes available.
	blockSec := r.URL.Query().Get("block")
	typeFilter := r.URL.Query().Get("type")
	headerLSN := r.URL.Query().Get(apidConfigIndexPar)
	var timeout int
	var err error
	if blockSec != "" {
		timeout, err = strconv.Atoi(blockSec)
		if err != nil || timeout < 0 {
			a.writeError(w, http.StatusBadRequest, API_ERR_BAD_BLOCK, "bad block value, must be number of seconds")
			return
		}
	}
	log.Debugf("/configurations long-poll timeout: %d", timeout)

	log.Debugf("Long-Poll-Index: %s", headerLSN)

	// if filter by "type"
	if typeFilter != "" {
		a.sendReadyDeployments(typeFilter, w, "")
		return
	}

	// if no filter, check for long polling
	cmpRes, apidLSN, err := a.compareLSN(headerLSN)
	switch {
	case err != nil:
		if err == ErrInvalidLSN {
			a.writeError(w, http.StatusBadRequest, http.StatusBadRequest, err.Error())
			return
		}
		log.Errorf("Error in compareLSN: %v", err)
		a.writeInternalError(w, err.Error())
		return
	case cmpRes <= 0: //APID_LSN <= Header_LSN
		if timeout == 0 { // no long polling
			w.WriteHeader(http.StatusNotModified)
		} else { // long polling
			a.waitForNewCL(w, time.Duration(timeout))
		}
		return
	case cmpRes > 0: //APID_LSN > Header_LSN
		a.sendReadyDeployments("", w, apidLSN)
		return
	}
}

func (a *apiManager) waitForNewCL(w http.ResponseWriter, timeout time.Duration) {
	ConfigChangeChan := make(chan interface{}, 1)
	a.addSubscriber <- ConfigChangeChan

	log.Debug("Long-polling... Waiting for new Deployments.")

	select {
	case LSN := <-ConfigChangeChan:
		// send configs and LSN
		lsn, ok := LSN.(string)
		if !ok {
			log.Errorf("Wrong LSN type: %v", LSN)
			a.writeInternalError(w, "Wrong LSN type")
			return
		}
		//TODO: read db only once for all subscribers
		a.sendReadyDeployments("", w, lsn)
	case <-time.After(timeout * time.Second):
		log.Debug("long-polling configuration request timed out.")
		w.WriteHeader(http.StatusNotModified)
	}
}

func (a *apiManager) sendReadyDeployments(typeFilter string, w http.ResponseWriter, apidLSN string) {
	deployments, err := a.dbMan.getReadyDeployments(typeFilter)
	if err != nil {
		log.Errorf("Database error: %v", err)
		a.writeInternalError(w, fmt.Sprintf("Database error: %s", err.Error()))
		return
	}
	a.sendDeployments(w, deployments, apidLSN, typeFilter)
}

func (a *apiManager) sendDeployments(w http.ResponseWriter, dataDeps []Configuration, apidLSN string, typeFilter string) {

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

	if typeFilter != "" {
		apiDeps.Self += "?type=" + typeFilter
	}

	b, err := json.Marshal(apiDeps)
	if err != nil {
		log.Errorf("unable to marshal deployments: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if apidLSN != "" {
		w.Header().Set(apidConfigIndexHeader, apidLSN)
	}
	w.Header().Set("Content-Type", headerJson)
	log.Debugf("sending deployments %s: %s", apidLSN, b)
	w.Write(b)
}

func (a *apiManager) compareLSN(headerLSN string) (res int, apidLSN string, err error) {
	apidLSN = a.dbMan.getLSN()
	log.Debugf("apidLSN: %v", apidLSN)

	// if no Long Poll Index
	if headerLSN == "" {
		return 1, apidLSN, nil
	}

	headerSeq, err := common.ParseSequence(headerLSN)
	if err != nil {
		log.Debugf("Error when Parse headerLSN Sequence: %v", err)
		return 0, "", ErrInvalidLSN
	}

	apidSeq, err := common.ParseSequence(apidLSN)
	if err != nil {
		log.Errorf("Error when Parse apidLSN Sequence: %v", err)
		return 0, "", err
	}

	return apidSeq.Compare(headerSeq), apidLSN, nil
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
