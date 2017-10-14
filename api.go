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
	"github.com/apid/apid-core/util"
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
	configEndpoint   = "/configurations"
	blobEndpointPath = "/blobs"
	blobEndpoint     = blobEndpointPath + "/{blobId}"
	configIdEndpoint = configEndpoint + "/{configId}"
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

type ApiConfigurationDetails struct {
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

type ApiConfigurationResponse struct {
	Kind                      string                    `json:"kind"`
	Self                      string                    `json:"self"`
	ApiConfigurationsResponse []ApiConfigurationDetails `json:"contents"`
}

type confChangeNotification struct {
	LSN   string
	confs []Configuration
	err   error
}

type apiManagerInterface interface {
	// an idempotent method to initialize api endpoints
	InitAPI()
	notifyNewChange()
}

type apiManager struct {
	dbMan                   dbManagerInterface
	configurationEndpoint   string
	blobEndpoint            string
	configurationIdEndpoint string
	addSubscriber           chan chan interface{}
	newChangeListChan       chan interface{}
	apiInitialized          bool
}

func (a *apiManager) InitAPI() {
	if a.apiInitialized {
		return
	}
	services.API().HandleFunc(a.configurationEndpoint, a.apiGetCurrentConfigs).Methods("GET")
	services.API().HandleFunc(a.blobEndpoint, a.apiReturnBlobData).Methods("GET")
	services.API().HandleFunc(a.configurationIdEndpoint, a.apiHandleConfigId).Methods("GET")
	a.initDistributeEvents()
	a.apiInitialized = true
	log.Debug("API endpoints initialized")
}

func (a *apiManager) initDistributeEvents() {
	go util.DistributeEvents(a.newChangeListChan, a.addSubscriber)
}

func (a *apiManager) notifyNewChange() {
	confs, err := a.dbMan.getAllConfigurations("")
	if err != nil {
		log.Errorf("Database error in getReadyConfigurations: %v", err)
	}
	a.newChangeListChan <- &confChangeNotification{
		LSN:   a.dbMan.getLSN(),
		confs: confs,
		err:   err,
	}
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
	configDetail := ApiConfigurationDetails{
		Self:            getHttpHost() + a.configurationEndpoint + "/" + config.ID,
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
	w.Header().Set("Content-Type", headerJson)
	w.Write(b)
}

// If not long-polling, return configurations, status = 200
// If "apid-config-index" is given in request parameters, return immediately with status = 200/304
// If both "block" and "apid-config-index" are given:
// if apid's LSN > apid-config-index in header, return immediately with status = 200
// if apid's LSN <= apid-config-index, long polling for timeout=block secs
func (a *apiManager) apiGetCurrentConfigs(w http.ResponseWriter, r *http.Request) {
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
		a.sendReadyConfigurations(typeFilter, w, "")
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
			util.LongPolling(w, time.Duration(timeout)*time.Second, a.addSubscriber, a.LongPollSuccessHandler, a.LongPollTimeoutHandler)
		}
		return
	case cmpRes > 0: //APID_LSN > Header_LSN
		a.sendReadyConfigurations("", w, apidLSN)
		return
	}
}

func (a *apiManager) LongPollSuccessHandler(c interface{}, w http.ResponseWriter) {
	// send configs and LSN
	confChange, ok := c.(*confChangeNotification)
	if !ok || confChange.err != nil {
		log.Errorf("Wrong confChangeNotification: %v, %v", ok, confChange)
		a.writeInternalError(w, "Error getting configurations with long-polling")
		return
	}
	a.sendDeployments(w, confChange.confs, confChange.LSN, "")
}

func (a *apiManager) LongPollTimeoutHandler(w http.ResponseWriter) {
	log.Debug("long-polling configuration request timed out.")
	w.WriteHeader(http.StatusNotModified)
}

func (a *apiManager) sendReadyConfigurations(typeFilter string, w http.ResponseWriter, apidLSN string) {
	configurations, err := a.dbMan.getAllConfigurations(typeFilter)
	if err != nil {
		log.Errorf("Database error: %v", err)
		a.writeInternalError(w, fmt.Sprintf("Database error: %s", err.Error()))
		return
	}
	a.sendDeployments(w, configurations, apidLSN, typeFilter)
}

func (a *apiManager) sendDeployments(w http.ResponseWriter, dataConfs []Configuration, apidLSN string, typeFilter string) {

	apiConfs := ApiConfigurationResponse{}
	apiConfDetails := make([]ApiConfigurationDetails, 0)

	apiConfs.Kind = kindCollection
	apiConfs.Self = getHttpHost() + a.configurationEndpoint

	for _, d := range dataConfs {
		apiConfDetails = append(apiConfDetails, ApiConfigurationDetails{
			Self:            apiConfs.Self + "/" + d.ID,
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
	apiConfs.ApiConfigurationsResponse = apiConfDetails

	if typeFilter != "" {
		apiConfs.Self += "?type=" + typeFilter
	}

	b, err := json.Marshal(apiConfs)
	if err != nil {
		log.Errorf("unable to marshal deployments: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if apidLSN != "" {
		w.Header().Set(apidConfigIndexHeader, apidLSN)
	}
	w.Header().Set("Content-Type", headerJson)
	log.Debugf("sending deployments %s", apidLSN)
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
