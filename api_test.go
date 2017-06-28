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

	"crypto/rand"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mathrand "math/rand"
	"strconv"
	"time"
)

const (
	testUrl = "http://127.0.0.1:9000"
)

var _ = Describe("api", func() {
	Context("GET /deployments", func() {
		var testCount int
		var dummyDbMan *dummyDbManager
		var testApiMan *apiManager

		var _ = BeforeEach(func() {
			testCount += 1
			dummyDbMan = &dummyDbManager{}
			testApiMan = &apiManager{
				dbMan:               dummyDbMan,
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

			// http get
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusOK))
			// parse response
			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).Should(Succeed())
			err = json.Unmarshal(body, &depRes)
			Expect(err).Should(Succeed())
			// verify response
			Expect(len(depRes.ApiDeploymentsResponse)).To(Equal(0))
			Expect(depRes.Kind).Should(Equal(kindCollection))
			Expect(depRes.Self).Should(Equal(testUrl + deploymentsEndpoint + strconv.Itoa(testCount)))

		})

		It("should get correct config format", func() {
			uri, err := url.Parse(testUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount)

			// set test data
			details := setTestDeployments(dummyDbMan, uri.String())

			// http get
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusOK))
			// parse response
			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).Should(Succeed())
			err = json.Unmarshal(body, &depRes)
			Expect(err).Should(Succeed())
			// verify response
			Expect(depRes.Kind).Should(Equal(kindCollection))
			Expect(depRes.Self).Should(Equal(uri.String()))
			Expect(depRes.ApiDeploymentsResponse).Should(Equal(details))

		})

		It("should debounce requests", func(done Done) {
			var in = make(chan interface{})
			var out = make(chan []interface{})

			go testApiMan.debounce(in, out, 3*time.Millisecond)

			go func() {
				defer GinkgoRecover()

				received, ok := <-out
				Expect(ok).To(BeTrue())
				Expect(len(received)).To(Equal(2))

				close(in)
				received, ok = <-out
				Expect(ok).To(BeFalse())

				close(done)
			}()

			in <- "x"
			in <- "y"
		})

	})
})

func setTestDeployments(dummyDbMan *dummyDbManager, self string) []ApiDeploymentDetails {
	deployments := make([]DataDeployment, 0)
	details := make([]ApiDeploymentDetails, 0)
	mathrand.Seed(time.Now().UnixNano())
	count := mathrand.Intn(5) + 1

	for i := 0; i < count; i++ {
		dep := DataDeployment{
			ID:             GenerateUUID(),
			OrgID:          GenerateUUID(),
			EnvID:          GenerateUUID(),
			Type:           "virtual-host",
			Name:           "vh-secure",
			Revision:       "1",
			BlobID:         GenerateUUID(),
			GWBlobID:       GenerateUUID(),
			BlobResourceID: GenerateUUID(),
			Updated:        time.Now().Format(time.RFC3339),
			UpdatedBy:      "haoming@google.com",
			Created:        time.Now().Format(time.RFC3339),
			CreatedBy:      "haoming@google.com",
			BlobFSLocation: "BlobFSLocation",
			BlobURL:        "http://localhost:6666/testBlobURL",
		}

		detail := ApiDeploymentDetails{
			Self:           self + "/" + dep.ID,
			Name:           dep.Name,
			Type:           dep.Type,
			Org:            dep.OrgID,
			Env:            dep.EnvID,
			Scope:          "",
			Revision:       dep.Revision,
			BlobId:         dep.BlobID,
			BlobURL:        dep.BlobURL,
			ResourceBlobId: dep.BlobResourceID,
			Created:        dep.Created,
			Updated:        dep.Updated,
		}

		deployments = append(deployments, dep)
		details = append(details, detail)
	}

	dummyDbMan.readyDeployments = deployments
	dummyDbMan.unreadyDeployments = deployments

	return details
}

type dummyDbManager struct {
	unreadyDeployments []DataDeployment
	readyDeployments   []DataDeployment
}

func (d *dummyDbManager) setDbVersion(version string) {

}

func (d *dummyDbManager) initDb() error {
	return nil
}

func (d *dummyDbManager) getUnreadyDeployments() ([]DataDeployment, error) {
	return d.unreadyDeployments, nil
}

func (d *dummyDbManager) getReadyDeployments() ([]DataDeployment, error) {
	return d.readyDeployments, nil
}

func (d *dummyDbManager) updateLocalFsLocation(string, string, string) error {
	return nil
}

func (d *dummyDbManager) getLocalFSLocation(string) (string, error) {
	return "", nil
}

func GenerateUUID() string {

	buff := make([]byte, 16)
	numRead, err := rand.Read(buff)
	if numRead != len(buff) || err != nil {
		panic(err)
	}
	/* uuid v4 spec */
	buff[6] = (buff[6] | 0x40) & 0x4F
	buff[8] = (buff[8] | 0x80) & 0xBF
	return fmt.Sprintf("%x-%x-%x-%x-%x", buff[0:4], buff[4:6], buff[6:8], buff[8:10], buff[10:])
}
