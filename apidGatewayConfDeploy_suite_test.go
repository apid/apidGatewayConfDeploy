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
	config.Set(configDownloadQueueSize, 1)
	config.Set(configBundleCleanupDelay, time.Millisecond)
})

var _ = AfterSuite(func() {
	apid.Events().Close()
	os.RemoveAll(tmpDir)
})

func TestApidGatewayDeploy(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ApidGatewayConfDeploy Suite")
}
