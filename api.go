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
)

type deploymentsResult struct {
	deployments []DataDeployment
	err         error
	eTag        string
}

var (
	deploymentsChanged = make(chan interface{}, 5)
	addSubscriber      = make(chan chan deploymentsResult)
	removeSubscriber   = make(chan chan deploymentsResult)
	eTag               int64
)

type errorResponse struct {
	ErrorCode int    `json:"errorCode"`
	Reason    string `json:"reason"`
}

type ApiDeploymentDetails struct {
	Self           string `json:"self"`
	Name           string `json:"name"`
	Org            string `json:"org"`
	Env            string `json:"env"`
	Type           string `json:"type"`
	BlobURL        string `json:"bloburl"`
	Revision       string `json:"revision"`
	BlobId         string `json:"blobId"`
	ResourceBlobId string `json:"resourceBlobId"`
	Created        string `json:"created"`
	Updated        string `json:"updated"`
}

type ApiDeploymentResponse struct {
	Kind                  string                 `json:"kind"`
	Self                  string                 `json:"self"`
	ApiDeploymentResponse []ApiDeploymentDetails `json:"contents"`
}

const deploymentsEndpoint = "/configurations"
const BlobEndpoint = "/blob/{blobId}"

func InitAPI() {
	services.API().HandleFunc(deploymentsEndpoint, apiGetCurrentDeployments).Methods("GET")
	services.API().HandleFunc(BlobEndpoint, apiReturnBlobData).Methods("GET")
}

func writeError(w http.ResponseWriter, status int, code int, reason string) {
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

func writeInternalError(w http.ResponseWriter, err string) {
	writeError(w, http.StatusInternalServerError, API_ERR_INTERNAL, err)
}

func debounce(in chan interface{}, out chan []interface{}, window time.Duration) {
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

func distributeEvents() {
	subscribers := make(map[chan deploymentsResult]struct{})
	deliverDeployments := make(chan []interface{}, 1)

	go debounce(deploymentsChanged, deliverDeployments, debounceDuration)

	for {
		select {
		case _, ok := <-deliverDeployments:
			if !ok {
				return // todo: using this?
			}
			subs := subscribers
			subscribers = make(map[chan deploymentsResult]struct{})
			go func() {
				eTag := incrementETag()
				deployments, err := getUnreadyDeployments()
				log.Debugf("delivering deployments to %d subscribers", len(subs))
				for subscriber := range subs {
					log.Debugf("delivering to: %v", subscriber)
					subscriber <- deploymentsResult{deployments, err, eTag}
				}
			}()
		case subscriber := <-addSubscriber:
			log.Debugf("Add subscriber: %v", subscriber)
			subscribers[subscriber] = struct{}{}
		case subscriber := <-removeSubscriber:
			log.Debugf("Remove subscriber: %v", subscriber)
			delete(subscribers, subscriber)
		}
	}
}

func apiReturnBlobData(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	blobId := vars["blobId"]
	fs, err := getLocalFSLocation(blobId)
	if err != nil {
		writeInternalError(w, "BlobId "+blobId+" has no mapping blob file")
		return
	}
	byte, err := ioutil.ReadFile(fs)
	if err != nil {
		writeInternalError(w, err.Error())
		return
	}
	_, err = io.Copy(w, bytes.NewReader(byte))
	if err != nil {
		writeInternalError(w, err.Error())
	}

}

func apiGetCurrentDeployments(w http.ResponseWriter, r *http.Request) {

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
			writeError(w, http.StatusBadRequest, API_ERR_BAD_BLOCK, "bad block value, must be number of seconds")
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
	eTag := getETag()
	if eTag == ifNoneMatch && timeout == 0 {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// send results if different eTag
	if eTag != ifNoneMatch {
		sendReadyDeployments(w)
		return
	}

	// otherwise, subscribe to any new deployment changes
	var newDeploymentsChannel chan deploymentsResult
	if timeout > 0 && ifNoneMatch != "" {
		newDeploymentsChannel = make(chan deploymentsResult, 1)
		addSubscriber <- newDeploymentsChannel
	}

	log.Debug("Blocking request... Waiting for new Deployments.")

	select {
	case result := <-newDeploymentsChannel:
		if result.err != nil {
			writeInternalError(w, "Database error")
		} else {
			sendDeployments(w, result.deployments, result.eTag)
		}

	case <-time.After(time.Duration(timeout) * time.Second):
		removeSubscriber <- newDeploymentsChannel
		log.Debug("Blocking deployment request timed out.")
		if ifNoneMatch != "" {
			w.WriteHeader(http.StatusNotModified)
		} else {
			sendReadyDeployments(w)
		}
	}
}

func sendReadyDeployments(w http.ResponseWriter) {
	eTag := getETag()
	deployments, err := getReadyDeployments()
	if err != nil {
		writeInternalError(w, "Database error")
		return
	}
	sendDeployments(w, deployments, eTag)
}

func get_http_host() string {
	// apid-core has to set this according to the protocol apid is to be run: http/https
	proto := config.GetString("protocol_type")
	if proto == "" {
		proto = "http"
	}
	proto = proto + "://" + config.GetString("api_listen")
	return proto
}

func sendDeployments(w http.ResponseWriter, dataDeps []DataDeployment, eTag string) {

	apiDeps := ApiDeploymentResponse{}
	apiDepDetails := []ApiDeploymentDetails{}

	apiDeps.Kind = "Collections"
	apiDeps.Self = get_http_host() + "/configurations"

	for _, d := range dataDeps {
		apiDepDetails = append(apiDepDetails, ApiDeploymentDetails{
			Org:            d.OrgID,
			Env:            d.EnvID,
			Revision:       d.Revision,
			BlobId:         d.GWBlobID,
			ResourceBlobId: d.BlobResourceID,
			Created:        d.Created,
			Updated:        d.Updated,
			Type:           d.Type,
			BlobURL:        d.BlobURL,
		})
	}
	apiDeps.ApiDeploymentResponse = apiDepDetails

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
func incrementETag() string {
	e := atomic.AddInt64(&eTag, 1)
	return strconv.FormatInt(e, 10)
}

func getETag() string {
	e := atomic.LoadInt64(&eTag)
	return strconv.FormatInt(e, 10)
}
