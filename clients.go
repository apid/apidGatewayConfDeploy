package apiGatewayConfDeploy

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

const (
	configBearerToken           = "apigeesync_bearer_token"
	trackerConfigStatusEndpoint = "/configurations/status"
	trackerHeartbeatEndpoint    = "/serviceheartbeat/{uuid}"
	trackerRegisterEndpoint     = "/serviceregister/{uuid}"
)

type trackerClientInterface interface {
	putConfigStatus(body *configStatusBody) *trackerResponse
	putRegister(uuid string, body *registerBody) *trackerResponse
	putHeartbeat(uuid, reportedTime string) *trackerResponse
}

type trackerResponse struct {
	code        int
	contentType string
	body        []byte
}

type trackerClient struct {
	trackerBaseUrl string
	clusterId      string
	httpclient     *http.Client
}

func (t *trackerClient) putConfigStatus(reqBody *configStatusBody) *trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + trackerConfigStatusEndpoint)
	if err != nil {
		log.Errorf("putConfigStatus failed to parse tracker uri: %v", err)
		return (internalError(err))
	}

	bodyBytes, err := json.Marshal(*reqBody)

	req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(bodyBytes))
	req.Header.Add("Authorization", getBearerToken())

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return (internalError(err))
	}
	defer r.Body.Close()

	return parseTrackerResponse(r)
}

func (t *trackerClient) putRegister(uuid string, reqBody *registerBody) *trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + strings.Replace(trackerRegisterEndpoint, "{uuid}", uuid, 1))
	if err != nil {
		log.Errorf("postRegister failed to parse tracker uri: %v", err)
		return (internalError(err))
	}

	body := serviceRegisterBody{
		ClusterId:    t.clusterId,
		Pod:          reqBody.Pod,
		PodType:      reqBody.Type,
		ReportedTime: reqBody.ReportedTime,
		Name:         reqBody.Name,
		Type:         reqBody.Type,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return (internalError(err))
	}

	req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(bodyBytes))
	req.Header.Add("Authorization", getBearerToken())

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return (internalError(err))
	}
	defer r.Body.Close()
	return parseTrackerResponse(r)
}

func (t *trackerClient) putHeartbeat(uuid, reported string) *trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + strings.Replace(trackerHeartbeatEndpoint, "{uuid}", uuid, 1))
	if err != nil {
		log.Errorf("putHeartbeat failed to parse tracker uri: %v", err)
		return internalError(err)
	}
	req, err := http.NewRequest("PUT", uri.String(), nil)
	req.Header.Add("Authorization", getBearerToken())
	req.Header.Add("reportedTime", reported)

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return internalError(err)
	}
	defer r.Body.Close()
	return parseTrackerResponse(r)
}

func internalError(err error) *trackerResponse {
	res := &trackerResponse{}
	res.code = http.StatusInternalServerError
	res.body = []byte(err.Error())
	return res
}

func getBearerToken() string {
	return "Bearer " + config.GetString(configBearerToken)
}

func parseTrackerResponse(r *http.Response) *trackerResponse {
	trackerBody, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("apid failed to read response body: %v", err)
		return (internalError(err))
	}
	res := &trackerResponse{}
	res.contentType = r.Header.Get("Content-type")
	switch r.StatusCode {
	case http.StatusOK:
		res.code = r.StatusCode
		res.body = trackerBody
	case http.StatusUnauthorized, http.StatusForbidden:
		res.code = http.StatusInternalServerError
		res.body = []byte("apid token rejected by tracker")
		log.Errorf("%v: %v, %v", res.body, r.StatusCode, trackerBody)
	case http.StatusNotFound:
		res.code = http.StatusInternalServerError
		res.body = []byte("apid cannot connect to tracker")
		log.Errorf("%v: %v, %v", res.body, r.StatusCode, trackerBody)
	default:
		log.Infof("Abnormal Response from Tracker: %v, %v", r.StatusCode, trackerBody)
		res.code = r.StatusCode
		res.body = trackerBody
	}
	return res
}

type serviceRegisterBody struct {
	ClusterId    string `json:"clusterId"`
	Pod          string `json:"pod"`
	PodType      string `json:"podType"`
	ReportedTime string `json:"reportedTime"`
	Name         string `json:"name"`
	Type         string `json:"type"`
}
