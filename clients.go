package apiGatewayConfDeploy

import (
	"github.com/30x/apid-core"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

const (
	trackerConfigStatusEndpoint = "/serviceconfigstatus"
	trackerHeartbeatEndpoint    = "/serviceheartbeat/{uuid}"
	trackerRegisterEndpoint     = "/serviceregister/{uuid}"
	ApigeeSyncTokenSelector     = "ApigeeSyncToken"
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
	handler        *tokenEventHandler
}

func (t *trackerClient) putConfigStatus(configId, status, uuid, created string) trackerResponse {
	uri, err := url.Parse(t.trackerBaseUrl + strings.Replace(trackerConfigStatusEndpoint, "{uuid}", uuid, 1))
	if err != nil {
		log.Errorf("putConfigStatus failed to parse tracker uri: %v", err)
		return (internalError(err))
	}
	req, err := http.NewRequest("PUT", uri.String(), nil)
	req.Header.Add("configid", configId)
	req.Header.Add("Authorization", t.handler.getBearerToken())
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
	case http.StatusUnauthorized, http.StatusForbidden:
		res.code = http.StatusInternalServerError
		res.errString = "apid token rejected by tracker"
		return res
	default:
		if 400 <= res.code && res.code < 500 {
			res.code = http.StatusInternalServerError
		} else {
			res.code = r.StatusCode
		}
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
	uri, err := url.Parse(t.trackerBaseUrl + strings.Replace(trackerRegisterEndpoint, "{uuid}", uuid, 1))
	if err != nil {
		log.Errorf("postRegister failed to parse tracker uri: %v", err)
		return (internalError(err))
	}
	req, err := http.NewRequest("PUT", uri.String(), nil)
	req.Header.Add("Authorization", t.handler.getBearerToken())
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
	case http.StatusUnauthorized, http.StatusForbidden:
		res.code = http.StatusInternalServerError
		res.errString = "apid token rejected by tracker"
		return res
	default:
		if 400 <= res.code && res.code < 500 {
			res.code = http.StatusInternalServerError
		} else {
			res.code = r.StatusCode
		}
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
	uri, err := url.Parse(t.trackerBaseUrl + strings.Replace(trackerHeartbeatEndpoint, "{uuid}", uuid, 1))
	if err != nil {
		log.Errorf("putHeartbeat failed to parse tracker uri: %v", err)
		return internalError(err)
	}
	req, err := http.NewRequest("PUT", uri.String(), nil)
	req.Header.Add("Authorization", t.handler.getBearerToken())
	req.Header.Add("updated", updated)
	req.Header.Add("clusterid", t.clusterId)

	r, err := t.httpclient.Do(req)
	if err != nil {
		log.Errorf("trackerClient communication error: %v", err)
		return internalError(err)
	}
	defer r.Body.Close()
	res := trackerResponse{}
	switch r.StatusCode {
	case http.StatusOK:
		res.code = r.StatusCode
		res.contentType = r.Header.Get("Content-type")
	case http.StatusUnauthorized, http.StatusForbidden:
		res.code = http.StatusInternalServerError
		res.errString = "apid token rejected by tracker"
		return res
	default:
		if 400 <= res.code && res.code < 500 {
			res.code = http.StatusInternalServerError
		} else {
			res.code = r.StatusCode
		}
		res.contentType = r.Header.Get("Content-type")
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return internalError(err)
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

type tokenEventHandler struct {
	token string
}

func (h *tokenEventHandler) Handle(e apid.Event) {
	if token, ok := e.(string); ok {
		h.token = token
	}
}

func (h *tokenEventHandler) getBearerToken() string {
	return "Bearer " + h.token
}

func (h *tokenEventHandler) initListener(services apid.Services) {

	services.Events().Listen(ApigeeSyncTokenSelector, h)
}
