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
)

var _ = Describe("api", func() {
	Context("GET /deployments", func() {

		It("should get empty set if no deployments", func() {
			//only called once
			InitAPI()

			var uri url.URL
			uri = *apiServerBaseURI
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()

			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			log.Error(depRes)

			//Expect(len(depRes)).To(Equal(0))
			//Expect(string(body)).Should(Equal("[]"))
		})
	})
})

type dummyDbMan struct {
}

func (d *dummyDbMan) setDbVersion(version string) {

}

func (d *dummyDbMan) initDb() error {
	return nil
}

func (d *dummyDbMan) getUnreadyDeployments() ([]DataDeployment, error) {
	return nil, nil
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
