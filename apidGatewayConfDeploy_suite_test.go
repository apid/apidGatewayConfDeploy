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
	"github.com/30x/apid-core"
	"github.com/30x/apid-core/factory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var (
	tmpDir string
)

const (
	configLevel          = "log_level"
	localStoragePathKey  = "local_storage_path"
	configBlobServerPort = "5555"
)

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())
	config := apid.Config()
	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())

	config.Set(configLevel, "debug")
	config.Set(configBlobServerBaseURI, "http://localhost:"+configBlobServerPort)
	config.Set(localStoragePathKey, tmpDir)
	config.Set(configApidInstanceID, "INSTANCE_ID")
	config.Set(configApidClusterID, "CLUSTER_ID")
	config.Set(configApiServerBaseURI, "http://localhost")
	config.Set(configDebounceDuration, "1ms")
	config.Set(configDownloadQueueSize, 1)
	config.Set(configBundleCleanupDelay, time.Millisecond)
	apid.InitializePlugins("0.0.0")
	go apid.API().Listen()
	time.Sleep(1 * time.Second)
	log.Debug("initialized")
}, 2)

var _ = AfterSuite(func() {
	apid.Events().Close()
	os.RemoveAll(tmpDir)
})

func TestApidGatewayDeploy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidGatewayConfDeploy Suite")
}
