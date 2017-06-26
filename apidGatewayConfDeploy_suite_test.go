package apiGatewayConfDeploy

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/30x/apid-core"
	"github.com/30x/apid-core/factory"
	"io/ioutil"
	"time"
	"os"
)


var (
	tmpDir              string
)

var _ = BeforeSuite(func() {
	apid.Initialize(factory.DefaultServicesFactory())
	config := apid.Config()
	var err error
	tmpDir, err = ioutil.TempDir("", "api_test")
	Expect(err).NotTo(HaveOccurred())

	config.Set("local_storage_path", tmpDir)
	config.Set(configApidInstanceID, "INSTANCE_ID")
	config.Set(configApidClusterID, "CLUSTER_ID")
	config.Set(configApiServerBaseURI, "http://localhost")
	config.Set(configDebounceDuration, "1ms")
	apid.InitializePlugins("")


	bundleCleanupDelay = time.Millisecond
	bundleRetryDelay = 10 * time.Millisecond
	markDeploymentFailedAfter = 50 * time.Millisecond
	concurrentDownloads = 1
	downloadQueueSize = 1

})

var _ = AfterSuite(func() {
	apid.Events().Close()
	os.RemoveAll(tmpDir)
})