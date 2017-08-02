package apiGatewayConfDeploy

import (
	"io/ioutil"
	"net/http"
	"net/url"
)

const (
	trackerConfigStatusEndpoint = "/serviceconfigstatus"
	trackerHeartbeatEndpoint    = "/serviceheartbeat/{uuid}"
	trackerRegisterEndpoint     = "/serviceregister/{uuid}"
)

type trackerClientInterface interface {
	putConfigStatus(configId, status, uuid, created string) trackerResponse
	postRegister(uuid, pod, created, name, podType, serviceType string) trackerResponse
	putHeartbeat(uuid, updated string) trackerResponse
}

type trackerResponse struct {
	code        int
	contentType string
	errString   string
}

type trackerClient struct {
	trackerBaseUrl string
	clusterId      string
	httpclient     *http.Client
}

func (t *trackerClient) putConfigStatus(configId, status, uuid, created string) trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + trackerConfigStatusEndpoint)
	if err != nil {
		log.Errorf("putConfigStatus failed to parse tracker uri: %v", err)
		return (internalError(err))
	}
	req, err := http.NewRequest("PUT", uri, nil)
	req.Header.Add("configid", configId)
	req.Header.Add("Authorization", getAuthToken())
	req.Header.Add("status", status)
	req.Header.Add("uuid", uuid)
	req.Header.Add("created", created)
	req.Header.Add("clusterid", t.clusterId)

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return (internalError(err))
	}
	defer r.Body.Close()
	res := trackerResponse{}
	switch r.StatusCode {
	case http.StatusOK:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
	default:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return (internalError(err))
		}
		res.errString = string(body)
	}
	return res
}

func (t *trackerClient) postRegister(uuid, pod, created, name, podType, serviceType string) trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + trackerRegisterEndpoint)
	if err != nil {
		log.Errorf("postRegister failed to parse tracker uri: %v", err)
		return (internalError(err))
	}
	req, err := http.NewRequest("PUT", uri, nil)
	req.Header.Add("uuid", uuid)
	req.Header.Add("Authorization", getAuthToken())
	req.Header.Add("pod", pod)
	req.Header.Add("podtype", podType)
	req.Header.Add("created", created)
	req.Header.Add("name", name)
	req.Header.Add("type", serviceType)
	req.Header.Add("clusterid", t.clusterId)

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return (internalError(err))
	}
	defer r.Body.Close()
	res := trackerResponse{}
	switch r.StatusCode {
	case http.StatusOK:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
	default:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return (internalError(err))
		}
		res.errString = string(body)
	}
	return res
}

func (t *trackerClient) putHeartbeat(uuid, updated string) trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + trackerHeartbeatEndpoint)
	if err != nil {
		log.Errorf("putHeartbeat failed to parse tracker uri: %v", err)
		return (internalError(err))
	}
	req, err := http.NewRequest("PUT", uri, nil)
	req.Header.Add("uuid", uuid)
	req.Header.Add("Authorization", getAuthToken())
	req.Header.Add("updated", updated)
	req.Header.Add("clusterid", t.clusterId)

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return (internalError(err))
	}
	defer r.Body.Close()
	res := trackerResponse{}
	switch r.StatusCode {
	case http.StatusOK:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
	default:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return (internalError(err))
		}
		res.errString = string(body)
	}
	return res
}

func internalError(err error) (res trackerResponse) {
	res.code = http.StatusInternalServerError
	res.errString = err.Error()
	return res
}

func getAuthToken() string {
	//TODO
	return "Bearer "
}
