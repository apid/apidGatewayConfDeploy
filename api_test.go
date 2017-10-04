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
	"database/sql"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mathrand "math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	apiTestUrl = "http://127.0.0.1:9000"
	testBlobId = "gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a"
)

var _ = Describe("api", func() {
	var testCount int
	var dummyDbMan *dummyDbManager
	var testApiMan *apiManager

	var _ = BeforeEach(func() {
		testCount += 1
		dummyDbMan = &dummyDbManager{}
		testApiMan = &apiManager{
			dbMan:                dummyDbMan,
			deploymentsEndpoint:  deploymentsEndpoint + strconv.Itoa(testCount),
			blobEndpoint:         blobEndpointPath + strconv.Itoa(testCount) + "/{blobId}",
			deploymentIdEndpoint: deploymentsEndpoint + strconv.Itoa(testCount) + "/{configId}",
			eTag:                 int64(testCount * 10),
			deploymentsChanged:   make(chan interface{}, 5),
			addSubscriber:        make(chan chan deploymentsResult),
			removeSubscriber:     make(chan chan deploymentsResult),
		}
		testApiMan.InitAPI()
		time.Sleep(100 * time.Millisecond)
	})

	var _ = AfterEach(func() {
		testApiMan = nil
	})
	Context("GET /configurations", func() {
		It("should get empty set if no deployments", func() {
			// setup http client
			uri, err := url.Parse(apiTestUrl)
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
			Expect(depRes.Self).Should(Equal(apiTestUrl + deploymentsEndpoint + strconv.Itoa(testCount)))

		})

		It("should get correct config format", func() {
			// setup http client
			uri, err := url.Parse(apiTestUrl)
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

		It("should get configs by filter", func() {
			typeFilter := "ORGANIZATION"
			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount)
			uri.RawQuery = "type=" + typeFilter
			// set test data
			dep := makeTestDeployment()

			dummyDbMan.configurations = make(map[string]*DataDeployment)
			dummyDbMan.configurations[typeFilter] = dep
			detail := makeExpectedDetail(dep, strings.Split(uri.String(), "?")[0])

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
			Expect(depRes.ApiDeploymentsResponse).Should(Equal([]ApiDeploymentDetails{*detail}))

		})

		It("should get 304 for no change", func() {

			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount)

			// set test data
			setTestDeployments(dummyDbMan, uri.String())

			// http get
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusOK))
			etag := res.Header.Get("etag")
			Expect(etag).ShouldNot(BeEmpty())

			// send second request
			req, err := http.NewRequest("GET", uri.String(), nil)
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("If-None-Match", etag)

			// get response
			res, err = http.DefaultClient.Do(req)
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusNotModified))
		})

		// block is not enabled now
		XIt("should get empty set after blocking if no deployments", func() {

			start := time.Now()

			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount)
			query := uri.Query()
			query.Add("block", "1")
			uri.RawQuery = query.Encode()

			// http get
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			//verify blocking time
			blockingTime := time.Since(start)
			log.Warnf("time used: %v", blockingTime.Seconds())
			Expect(blockingTime.Seconds() > 0.9).Should(BeTrue())

			// parse response
			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).Should(Succeed())
			err = json.Unmarshal(body, &depRes)
			Expect(err).Should(Succeed())

			// verify response
			Expect(len(depRes.ApiDeploymentsResponse)).To(Equal(0))
			Expect(depRes.Kind).Should(Equal(kindCollection))
			Expect(depRes.Self).Should(Equal(apiTestUrl + deploymentsEndpoint + strconv.Itoa(testCount)))

		}, 2)

		It("should get iso8601 time", func() {
			testTimes := []string{"", "2017-04-05 04:47:36.462 +0000 UTC", "2017-04-05 04:47:36.462-07:00", "2017-04-05T04:47:36.462Z", "2017-04-05 23:23:38.162+00:00", "2017-06-22 16:41:02.334"}
			isoTime := []string{"", "2017-04-05T04:47:36.462Z", "2017-04-05T04:47:36.462-07:00", "2017-04-05T04:47:36.462Z", "2017-04-05T23:23:38.162Z", "2017-06-22T16:41:02.334Z"}

			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount)

			for i, t := range testTimes {
				log.Debug("insert deployment with timestamp: " + t)
				// set test data
				dep := makeTestDeployment()
				dep.Created = t
				dep.Updated = t
				dummyDbMan.readyDeployments = []DataDeployment{*dep}
				detail := makeExpectedDetail(dep, uri.String())
				detail.Created = isoTime[i]
				detail.Updated = isoTime[i]
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
				Expect(depRes.ApiDeploymentsResponse).Should(Equal([]ApiDeploymentDetails{*detail}))

			}
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

	Context("GET /blobs", func() {
		It("should get file bytesfrom endpoint", func() {
			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())
			uri.Path = blobEndpointPath + strconv.Itoa(testCount) + "/test"

			// set test data
			testFile, err := ioutil.TempFile(bundlePath, "test")
			randString := GenerateUUID()
			testFile.Write([]byte(randString))
			err = testFile.Close()
			Expect(err).Should(Succeed())
			dummyDbMan.localFSLocation = testFile.Name()

			log.Debug("randString: " + randString)

			// http get
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			// parse response
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).Should(Succeed())
			Expect(string(body)).Should(Equal(randString))
		})
	})

	Context("GET /configurations/{configId}", func() {
		It("should get configuration according to {configId}", func() {
			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())
			uri.Path = deploymentsEndpoint + strconv.Itoa(testCount) + "/3ecd351c-1173-40bf-b830-c194e5ef9038"

			//setup test data
			dummyDbMan.err = nil
			dummyDbMan.configurations = make(map[string]*DataDeployment)
			expectedConfig := &DataDeployment{
				ID:             "3ecd351c-1173-40bf-b830-c194e5ef9038",
				OrgID:          "73fcac6c-5d9f-44c1-8db0-333efda3e6e8",
				EnvID:          "ada76573-68e3-4f1a-a0f9-cbc201a97e80",
				BlobID:         "gcs:SHA-512:8fcc902465ccb32ceff25fa9f6fb28e3b314dbc2874c0f8add02f4e29c9e2798d344c51807aa1af56035cf09d39c800cf605d627ba65723f26d8b9c83c82d2f2",
				BlobResourceID: "gcs:SHA-512:0c648779da035bfe0ac21f6268049aa0ae74d9d6411dadefaec33991e55c2d66c807e06f7ef84e0947f7c7d63b8c9e97cf0684cbef9e0a86b947d73c74ae7455",
				Type:           "ENVIRONMENT",
				Name:           "test",
				Revision:       "",
				Path:           "/organizations/Org1//environments/test/",
				Created:        "2017-06-27 03:14:46.018+00:00",
				CreatedBy:      "defaultUser",
				Updated:        "2017-06-27 03:14:46.018+00:00",
				UpdatedBy:      "defaultUser",
			}
			dummyDbMan.configurations[expectedConfig.ID] = expectedConfig
			// http get
			res, err := http.Get(uri.String())
			Expect(err).Should(Succeed())
			defer res.Body.Close()
			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			// parse response
			var depRes ApiDeploymentDetails
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).Should(Succeed())
			err = json.Unmarshal(body, &depRes)
			Expect(err).Should(Succeed())

			// verify response
			Expect(depRes.Self).Should(ContainSubstring(expectedConfig.ID))
			Expect(depRes.Org).Should(Equal(expectedConfig.OrgID))
			Expect(depRes.Name).Should(Equal(expectedConfig.Name))
			Expect(depRes.Type).Should(Equal(expectedConfig.Type))
			Expect(depRes.Revision).Should(Equal(expectedConfig.Revision))
			Expect(depRes.BeanBlobUrl).Should(ContainSubstring(expectedConfig.BlobID))
			Expect(depRes.ResourceBlobUrl).Should(ContainSubstring(expectedConfig.BlobResourceID))
			Expect(depRes.Path).Should(Equal(expectedConfig.Path))
			Expect(depRes.Created).Should(Equal(convertTime(expectedConfig.Created)))
			Expect(depRes.Updated).Should(Equal(convertTime(expectedConfig.Updated)))
		})

		It("should get error responses", func() {
			// setup http client
			uri, err := url.Parse(apiTestUrl)
			Expect(err).Should(Succeed())

			//setup test data
			testData := [][]interface{}{
				{"invalid-uuid", nil},
				{GenerateUUID(), sql.ErrNoRows},
				{GenerateUUID(), fmt.Errorf("test error")},
			}
			expectedCode := []int{
				http.StatusBadRequest,
				http.StatusNotFound,
				http.StatusInternalServerError,
			}

			for i, data := range testData {
				if data[1] != nil {
					dummyDbMan.err = data[1].(error)
				}
				dummyDbMan.configurations = make(map[string]*DataDeployment)
				dummyDbMan.configurations[data[0].(string)] = &DataDeployment{}
				// http get
				uri.Path = deploymentsEndpoint + strconv.Itoa(testCount) + "/" + data[0].(string)
				res, err := http.Get(uri.String())
				Expect(err).Should(Succeed())
				Expect(res.StatusCode).Should(Equal(expectedCode[i]))
				res.Body.Close()
			}
		})
	})

})

func setTestDeployments(dummyDbMan *dummyDbManager, self string) []ApiDeploymentDetails {

	mathrand.Seed(time.Now().UnixNano())
	count := mathrand.Intn(5) + 1
	deployments := make([]DataDeployment, count)
	details := make([]ApiDeploymentDetails, count)

	for i := 0; i < count; i++ {
		dep := makeTestDeployment()
		detail := makeExpectedDetail(dep, self)

		deployments[i] = *dep
		details[i] = *detail
	}

	dummyDbMan.readyDeployments = deployments

	return details
}

func makeTestDeployment() *DataDeployment {
	dep := &DataDeployment{
		ID:             GenerateUUID(),
		OrgID:          GenerateUUID(),
		EnvID:          GenerateUUID(),
		BlobID:         testBlobId,
		BlobResourceID: "",
		Type:           "virtual-host",
		Name:           "vh-secure",
		Revision:       "1",
		Path:           "/organizations/Org1/",
		Created:        time.Now().Format(time.RFC3339),
		CreatedBy:      "haoming@google.com",
		Updated:        time.Now().Format(time.RFC3339),
		UpdatedBy:      "haoming@google.com",
	}
	return dep
}

func makeExpectedDetail(dep *DataDeployment, self string) *ApiDeploymentDetails {
	detail := &ApiDeploymentDetails{
		Self:            self + "/" + dep.ID,
		Name:            dep.Name,
		Type:            dep.Type,
		Revision:        dep.Revision,
		BeanBlobUrl:     getBlobUrl(dep.BlobID),
		Org:             dep.OrgID,
		Env:             dep.EnvID,
		ResourceBlobUrl: "",
		Path:            dep.Path,
		Created:         dep.Created,
		Updated:         dep.Updated,
	}
	return detail
}

type dummyDbManager struct {
	unreadyBlobIds   []string
	readyDeployments []DataDeployment
	localFSLocation  string
	fileResponse     chan string
	version          string
	configurations   map[string]*DataDeployment
	err              error
}

func (d *dummyDbManager) setDbVersion(version string) {
	d.version = version
}

func (d *dummyDbManager) initDb() error {
	return nil
}

func (d *dummyDbManager) getUnreadyBlobs() ([]string, error) {
	return d.unreadyBlobIds, nil
}

func (d *dummyDbManager) getReadyDeployments(typeFilter string) ([]DataDeployment, error) {
	if typeFilter == "" {
		return d.readyDeployments, nil
	}
	return []DataDeployment{*(d.configurations[typeFilter])}, nil
}

func (d *dummyDbManager) updateLocalFsLocation(blobId, localFsLocation string) error {
	file, err := os.Open(localFsLocation)
	if err != nil {
		return err
	}
	buff := make([]byte, 36)
	_, err = file.Read(buff)
	if err != nil {
		return err
	}
	d.fileResponse <- string(buff)
	return nil
}

func (d *dummyDbManager) getLocalFSLocation(string) (string, error) {
	return d.localFSLocation, nil
}

func (d *dummyDbManager) getConfigById(id string) (*DataDeployment, error) {
	return d.configurations[id], d.err
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
