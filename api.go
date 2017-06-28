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
	"strconv"
	"sync/atomic"
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

type deploymentsResult struct {
	deployments []DataDeployment
	err         error
	eTag        string
}

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

type ApiDeploymentDetails struct {
	Self           string `json:"self"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	Org            string `json:"organization"`
	Env            string `json:"environment"`
	Scope          string `json:"scope"`
	Revision       string `json:"revision"`
	BlobId         string `json:"blobId"`
	BlobURL        string `json:"bloburl"`
	ResourceBlobId string `json:"resourceBlobId"`
	Created        string `json:"created"`
	Updated        string `json:"updated"`
}

type ApiDeploymentResponse struct {
	Kind                   string                 `json:"kind"`
	Self                   string                 `json:"self"`
	ApiDeploymentsResponse []ApiDeploymentDetails `json:"contents"`
}

const deploymentsEndpoint = "/configurations"
const blobEndpointPath = "/blob"
const blobEndpoint = blobEndpointPath + "/{blobId}"

type apiManagerInterface interface {
	InitAPI()
	addChangedDeployment(string)
	distributeEvents()
}

type apiManager struct {
	dbMan               dbManagerInterface
	deploymentsEndpoint string
	blobEndpoint        string
	eTag                int64
	deploymentsChanged  chan interface{}
	addSubscriber       chan chan deploymentsResult
	removeSubscriber    chan chan deploymentsResult
}

func (a *apiManager) InitAPI() {
	log.Debug("API endpoints initialized")
	services.API().HandleFunc(a.deploymentsEndpoint, a.apiGetCurrentDeployments).Methods("GET")
	services.API().HandleFunc(a.blobEndpoint, a.apiReturnBlobData).Methods("GET")
}

func (a *apiManager) addChangedDeployment(id string) {
	a.deploymentsChanged <- id
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

func (a *apiManager) debounce(in chan interface{}, out chan []interface{}, window time.Duration) {
	send := func(toSend []interface{}) {
		if toSend != nil {
			log.Debugf("debouncer sending: %v", toSend)
			out <- toSend
		}
	}
	var toSend []interface{}
	for {
		select {
		case incoming, ok := <-in:
			if ok {
				log.Debugf("debouncing %v", incoming)
				toSend = append(toSend, incoming)
			} else {
				send(toSend)
				log.Debugf("closing debouncer")
				close(out)
				return
			}
		case <-time.After(window):
			send(toSend)
			toSend = nil
		}
	}
}

func (a *apiManager) distributeEvents() {
	subscribers := make(map[chan deploymentsResult]bool)
	deliverDeployments := make(chan []interface{}, 1)

	go a.debounce(a.deploymentsChanged, deliverDeployments, debounceDuration)

	for {
		select {
		case _, ok := <-deliverDeployments:
			if !ok {
				return // todo: using this?
			}
			subs := subscribers
			subscribers = make(map[chan deploymentsResult]bool)
			go func() {
				eTag := a.incrementETag()
				deployments, err := a.dbMan.getUnreadyDeployments()
				log.Debugf("delivering deployments to %d subscribers", len(subs))
				for subscriber := range subs {
					log.Debugf("delivering to: %v", subscriber)
					subscriber <- deploymentsResult{deployments, err, eTag}
				}
			}()
		case subscriber := <-a.addSubscriber:
			log.Debugf("Add subscriber: %v", subscriber)
			subscribers[subscriber] = true
		case subscriber := <-a.removeSubscriber:
			log.Debugf("Remove subscriber: %v", subscriber)
			delete(subscribers, subscriber)
		}
	}
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

}

func (a *apiManager) apiGetCurrentDeployments(w http.ResponseWriter, r *http.Request) {

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
		if err != nil {
			a.writeError(w, http.StatusBadRequest, API_ERR_BAD_BLOCK, "bad block value, must be number of seconds")
			return
		}
	}
	log.Debugf("api timeout: %d", timeout)

	// If If-None-Match header matches the ETag of current bundle list AND if the request does NOT have a 'block'
	// query param > 0, the server returns a 304 Not Modified response indicating that the client already has the
	// most recent bundle list.
	ifNoneMatch := r.Header.Get("If-None-Match")
	log.Debugf("if-none-match: %s", ifNoneMatch)

	// send unmodified if matches prior eTag and no timeout
	eTag := a.getETag()
	if eTag == ifNoneMatch && timeout == 0 {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// send results if different eTag
	if eTag != ifNoneMatch {
		a.sendReadyDeployments(w)
		return
	}

	// otherwise, subscribe to any new deployment changes
	var newDeploymentsChannel chan deploymentsResult
	if timeout > 0 && ifNoneMatch != "" {
		newDeploymentsChannel = make(chan deploymentsResult, 1)
		a.addSubscriber <- newDeploymentsChannel
	}

	log.Debug("Blocking request... Waiting for new Deployments.")

	select {
	case result := <-newDeploymentsChannel:
		if result.err != nil {
			a.writeInternalError(w, "Database error")
		} else {
			a.sendDeployments(w, result.deployments, result.eTag)
		}

	case <-time.After(time.Duration(timeout) * time.Second):
		a.removeSubscriber <- newDeploymentsChannel
		log.Debug("Blocking deployment request timed out.")
		if ifNoneMatch != "" {
			w.WriteHeader(http.StatusNotModified)
		} else {
			a.sendReadyDeployments(w)
		}
	}
}

func (a *apiManager) sendReadyDeployments(w http.ResponseWriter) {
	eTag := a.getETag()
	deployments, err := a.dbMan.getReadyDeployments()
	if err != nil {
		a.writeInternalError(w, "Database error")
		return
	}
	a.sendDeployments(w, deployments, eTag)
}

func (a *apiManager) sendDeployments(w http.ResponseWriter, dataDeps []DataDeployment, eTag string) {

	apiDeps := ApiDeploymentResponse{}
	apiDepDetails := make([]ApiDeploymentDetails, 0)

	apiDeps.Kind = kindCollection
	apiDeps.Self = getHttpHost() + a.deploymentsEndpoint

	for _, d := range dataDeps {
		apiDepDetails = append(apiDepDetails, ApiDeploymentDetails{
			Self:           apiDeps.Self + "/" + d.ID,
			Name:           d.Name,
			Type:           d.Type,
			Org:            d.OrgID,
			Env:            d.EnvID,
			Scope:          a.getDeploymentScope(),
			Revision:       d.Revision,
			BlobId:         d.BlobID,
			BlobURL:        d.BlobURL,
			ResourceBlobId: d.BlobResourceID,
			Created:        convertTime(d.Created),
			Updated:        convertTime(d.Updated),
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

// call whenever the list of deployments changes
func (a *apiManager) incrementETag() string {
	e := atomic.AddInt64(&a.eTag, 1)
	return strconv.FormatInt(e, 10)
}

func (a *apiManager) getETag() string {
	e := atomic.LoadInt64(&a.eTag)
	return strconv.FormatInt(e, 10)
}

// TODO
func (a *apiManager) getDeploymentScope() string {
	return ""
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
	// apid-core has to set this according to the protocol apid is to be run: http/https
	proto := config.GetString("protocol_type")
	if proto == "" {
		proto = "http"
	}
	proto = proto + "://" + config.GetString("api_listen")
	return proto
}
