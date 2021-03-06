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
	"fmt"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/apid/apid-core"
	"github.com/apid/apid-core/util"
	"net/http"
	"sync"
)

const (
	configProtocol              = "protocol_type"
	configAPIListen             = "api_listen"
	configBlobDownloadEndpoint  = "gatewaydeploy_bundle_download_endpoint"
	configBlobDirKey            = "gatewaydeploy_bundle_dir"
	configDebounceDuration      = "gatewaydeploy_debounce_duration"
	configBlobCleanupDelay      = "gatewaydeploy_bundle_cleanup_delay"
	configMarkDeployFailedAfter = "gatewaydeploy_deployment_timeout"
	configDownloadConnTimeout   = "gatewaydeploy_download_connection_timeout"
	configApiServerBaseURI      = "apigeesync_proxy_server_base"
	configApidInstanceID        = "apigeesync_apid_instance_id"
	configApidClusterID         = "apigeesync_cluster_id"
	configConcurrentDownloads   = "apigeesync_concurrent_downloads"
	configDownloadQueueSize     = "apigeesync_download_queue_size"
	configBlobServerBaseURI     = "apigeesync_blob_server_base"
	configStoragePath           = "local_storage_path"
	maxIdleConnsPerHost         = 50
	httpTimeout                 = time.Minute
	configBearerToken           = "apigeesync_bearer_token"
)

var (
	services         apid.Services
	log              apid.LogService
	config           apid.ConfigService
	bundlePath       string
	debounceDuration time.Duration
	apiServerBaseURI *url.URL
	eventHandler     *apigeeSyncHandler
)

func init() {
	apid.RegisterPlugin(initPlugin, pluginData)
}

func initPlugin(s apid.Services) (apid.PluginData, error) {
	var tr *http.Transport

	services = s
	log = services.Log().ForModule("apiGatewayConfDeploy")
	log.Debug("start init")

	config = services.Config()

	if !config.IsSet(configApiServerBaseURI) {
		return pluginData, fmt.Errorf("Missing required config value: %s", configApiServerBaseURI)
	}

	if !config.IsSet(configBlobServerBaseURI) {
		return pluginData, fmt.Errorf("Missing required config value: %s", configBlobServerBaseURI)
	}

	var err error
	apiServerBaseURI, err = url.Parse(config.GetString(configApiServerBaseURI))
	if err != nil {
		return pluginData, fmt.Errorf("%s value %s parse err: %v", configApiServerBaseURI, apiServerBaseURI, err)
	}

	config.SetDefault(configBlobDirKey, "bundles")
	config.SetDefault(configDebounceDuration, time.Second)
	config.SetDefault(configBlobCleanupDelay, time.Minute)
	config.SetDefault(configMarkDeployFailedAfter, 5*time.Minute)
	config.SetDefault(configDownloadConnTimeout, 5*time.Minute)
	config.SetDefault(configConcurrentDownloads, 15)
	config.SetDefault(configDownloadQueueSize, 2000)

	debounceDuration = config.GetDuration(configDebounceDuration)
	if debounceDuration < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configDebounceDuration)
	}

	bundleCleanupDelay := config.GetDuration(configBlobCleanupDelay)
	if bundleCleanupDelay < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configBlobCleanupDelay)
	}

	markDeploymentFailedAfter := config.GetDuration(configMarkDeployFailedAfter)
	if markDeploymentFailedAfter < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configMarkDeployFailedAfter)
	}

	bundleDownloadConnTimeout := config.GetDuration(configDownloadConnTimeout)
	if bundleDownloadConnTimeout < time.Millisecond {
		return pluginData, fmt.Errorf("%s must be a positive duration", configDownloadConnTimeout)
	}

	log.Debug("apiServerBaseURI = " + apiServerBaseURI.String())

	tr = util.Transport(config.GetString(util.ConfigfwdProxyPortURL))
	tr.MaxIdleConnsPerHost = maxIdleConnsPerHost
	// initialize tracker client
	httpClient := &http.Client{
		Transport: tr,
		Timeout:   httpTimeout,
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			req.Header.Set("Authorization", getBearerToken())
			return nil
		},
	}

	// initialize db manager

	dbMan := &dbManager{
		data:  services.Data(),
		dbMux: sync.RWMutex{},
	}

	// initialize api manager

	apiMan := &apiManager{
		dbMan: dbMan,
		configurationEndpoint:   configEndpoint,
		blobEndpoint:            blobEndpoint,
		configurationIdEndpoint: configIdEndpoint,
		newChangeListChan:       make(chan interface{}, 5),
		addSubscriber:           make(chan chan interface{}, 100),
		apiInitialized:          false,
	}

	// initialize bundle manager

	blobServerURL := config.GetString(configBlobServerBaseURI)
	relativeBundlePath := config.GetString(configBlobDirKey)
	storagePath := config.GetString(configStoragePath)
	bundlePath = path.Join(storagePath, relativeBundlePath)
	if err := os.MkdirAll(bundlePath, 0700); err != nil {
		return pluginData, fmt.Errorf("Failed bundle directory creation: %v", err)
	}
	log.Infof("Bundle directory path is %s", bundlePath)
	concurrentDownloads := config.GetInt(configConcurrentDownloads)
	downloadQueueSize := config.GetInt(configDownloadQueueSize)
	bundleMan := &bundleManager{
		blobServerUrl:         blobServerURL,
		dbMan:                 dbMan,
		apiMan:                apiMan,
		concurrentDownloads:   concurrentDownloads,
		markConfigFailedAfter: markDeploymentFailedAfter,
		bundleRetryDelay:      time.Second,
		bundleCleanupDelay:    bundleCleanupDelay,
		downloadQueue:         make(chan *DownloadRequest, downloadQueueSize),
		isClosed:              new(int32),
		client:                httpClient,
	}

	bundleMan.initializeBundleDownloading()

	// initialize event handler
	eventHandler = &apigeeSyncHandler{
		dbMan:     dbMan,
		apiMan:    apiMan,
		bundleMan: bundleMan,
		closed:    false,
	}

	eventHandler.initListener(services)

	log.Debug("end init")

	return pluginData, nil
}

func getBearerToken() string {
	return "Bearer " + config.GetString(configBearerToken)
}
