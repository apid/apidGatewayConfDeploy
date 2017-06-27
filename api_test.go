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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"time"
)

const (
	testUrl = "http://127.0.0.1:9000"
)

var _ = Describe("api", func() {
	Context("GET /deployments", func() {
		var testCount int
		var testApiMan *apiManager

		var _ = BeforeEach(func() {
			testCount += 1
			dbMan := &dummyDbMan{}
			testApiMan = &apiManager{
				dbMan:               dbMan,
				deploymentsEndpoint: deploymentsEndpoint + strconv.Itoa(testCount),
				blobEndpoint:        blobEndpointPath + strconv.Itoa(testCount) + "/{blobId}",
				eTag:                int64(testCount * 10),
				deploymentsChanged:  make(chan interface{}, 5),
				addSubscriber:       make(chan chan deploymentsResult),
				removeSubscriber:    make(chan chan deploymentsResult),
			}
			testApiMan.InitAPI()
			time.Sleep(100 * time.Millisecond)
		})

		var _ = AfterEach(func() {
			testApiMan = nil
		})

		It("should get empty set if no deployments", func() {
			uri, err := url.Parse(testUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount)
			log.Debug("uri string: " + uri.String())
			log.Debug("port: " + config.GetString("api_port"))

			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()

			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(len(depRes.ApiDeploymentsResponse)).To(Equal(0))
			Expect(depRes.Kind).Should(Equal(kindCollection))
			Expect(depRes.Self).Should(Equal(testUrl + deploymentsEndpoint + strconv.Itoa(testCount)))

		})
	})
})

type dummyDbMan struct {
	unreadyDeployments []DataDeployment
	readyDeployments   []DataDeployment
}

func (d *dummyDbMan) setDbVersion(version string) {

}

func (d *dummyDbMan) initDb() error {
	return nil
}

func (d *dummyDbMan) getUnreadyDeployments() ([]DataDeployment, error) {
	return d.unreadyDeployments, nil
}

func (d *dummyDbMan) getReadyDeployments() ([]DataDeployment, error) {
	return nil, nil
}

func (d *dummyDbMan) updateLocalFsLocation(string, string, string) error {
	return nil
}

func (d *dummyDbMan) getLocalFSLocation(string) (string, error) {
	return "", nil
}
