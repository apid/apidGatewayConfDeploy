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

	"bytes"
	"crypto/rand"
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	mathrand "math/rand"
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
			configStatusEndpoint: "/test" + strconv.Itoa(testCount) + configStatusEndpoint,
			heartbeatEndpoint:    "/test" + strconv.Itoa(testCount) + heartbeatEndpoint,
			registerEndpoint:     "/test" + strconv.Itoa(testCount) + registerEndpoint,
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

	Context("Tracking endpoints", func() {
		var dummyClient *dummyTrackerClient
		var testClient *http.Client

		var _ = BeforeEach(func() {
			dummyClient = &dummyTrackerClient{}
			testApiMan.trackerCl = dummyClient
			testClient = &http.Client{}
		})

		var _ = AfterEach(func() {

		})

		Context("PUT /heartbeat/{uuid}", func() {
			It("/heartbeat should validate request", func() {
				// setup test data
				dummyClient.code = http.StatusOK
				testData := [][]string{
					{GenerateUUID(), time.Now().Format(time.RFC3339)},
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), "invalid-time"},
					{GenerateUUID(), time.Now().Format(time.RubyDate)},
					{"invalid-uuid", time.Now().Format(iso8601)},
					{"invalid-uuid", "invalid-time"},
					{"", time.Now().Format(time.RFC3339)},
					{GenerateUUID(), ""},
				}

				expectedCode := []int{
					http.StatusOK,
					http.StatusOK,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusNotFound,
					http.StatusBadRequest,
				}

				expectedBody := []string{
					strings.Join(testData[0], " "),
					strings.Join(testData[1], " "),
					"reportedTime",
					"reportedTime",
					"UUID",
					"UUID",
					"",
					"reportedTime",
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					uri.Path = strings.Replace(testApiMan.heartbeatEndpoint, "{uuid}", data[0], 1)
					log.Debug(uri.String())
					req, err := http.NewRequest("PUT", uri.String(), nil)
					Expect(err).Should(Succeed())
					req.Header.Set("reportedTime", data[1])
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})

			It("/heartbeat should populate errors from tracker", func() {
				// setup test data
				testData := [][]string{
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), time.Now().Format(iso8601)},
				}

				expectedCode := []int{
					http.StatusBadRequest,
					http.StatusInternalServerError,
					http.StatusBadGateway,
				}

				expectedBody := []string{
					strings.Join(testData[0], " "),
					strings.Join(testData[1], " "),
					strings.Join(testData[2], " "),
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					dummyClient.code = expectedCode[i]
					uri.Path = strings.Replace(testApiMan.heartbeatEndpoint, "{uuid}", data[0], 1)
					req, err := http.NewRequest("PUT", uri.String(), nil)
					Expect(err).Should(Succeed())
					req.Header.Set("reportedTime", data[1])
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})
		})

		Context("PUT /configurations/status", func() {
			It("/configurations/status should validate request body", func() {
				// setup test data
				dummyClient.code = http.StatusOK
				testData := [][]string{
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), time.Now().Format(time.RFC3339)},
					{GenerateUUID(), "invalid-time"},
					{GenerateUUID(), time.Now().Format(time.RubyDate)},
					{"invalid-uuid", time.Now().Format(iso8601)},
					{"invalid-uuid", "invalid-time"},
					{"", time.Now().Format(time.RFC3339)},
					{GenerateUUID(), ""},
				}

				expectedCode := []int{
					http.StatusOK,
					http.StatusOK,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
				}

				expectedBody := []string{
					"",
					"",
					"reportedTime",
					"reportedTime",
					"ServiceId",
					"ServiceId",
					"ServiceId",
					"reportedTime",
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					uri.Path = testApiMan.configStatusEndpoint
					log.Debug(uri.String())
					configNum := mathrand.Intn(5)
					statusDetails := make([]statusDetailsJson, 0)
					expectedSlice := []string{}
					for j := 0; j < configNum; j++ {
						configStatus, expected := generateStatusDetails(0)
						statusDetails = append(statusDetails, *configStatus)
						expectedSlice = append(expectedSlice, expected)
					}

					expectedSlice = append(expectedSlice, data...)

					if expectedCode[i] == http.StatusOK {
						expectedBody[i] = strings.Join(expectedSlice, " ")
					}

					reqBody, err := json.Marshal(configStatusBody{
						ServiceId:     data[0],
						ReportedTime:  data[1],
						StatusDetails: statusDetails,
					})
					Expect(err).Should(Succeed())
					req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(reqBody))
					Expect(err).Should(Succeed())
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})

			It("/configurations/status should validate status detail", func() {
				// setup test data
				dummyClient.code = http.StatusOK
				testData := [][]string{
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), time.Now().Format(iso8601)},
				}

				expectedCode := []int{
					http.StatusBadRequest,
					http.StatusBadRequest,
				}

				expectedBody := []string{
					"Status",
					"ConfigurationId",
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					uri.Path = testApiMan.configStatusEndpoint
					log.Debug(uri.String())
					configNum := mathrand.Intn(5) + 1
					statusDetails := make([]statusDetailsJson, 0)
					expectedSlice := []string{}
					for j := 0; j < configNum; j++ {
						configStatus, expected := generateStatusDetails(i + 1)
						statusDetails = append(statusDetails, *configStatus)
						expectedSlice = append(expectedSlice, expected)
					}

					expectedSlice = append(expectedSlice, data...)

					reqBody, err := json.Marshal(configStatusBody{
						ServiceId:     data[0],
						ReportedTime:  data[1],
						StatusDetails: statusDetails,
					})
					Expect(err).Should(Succeed())
					req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(reqBody))
					Expect(err).Should(Succeed())
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})

			It("/configurations/status should reject invalid json, and marshal valid fields", func() {
				// setup test data
				dummyClient.code = http.StatusOK
				testData := []string{
					"invalid-json",
					`{"serviceId":"8eebdb60-be68-4380-a902-8cd0a2a0744c",
					"reportedTime":"2017-08-09T13:30:03.987-07:00"}`,
					`{"invalid-field1":"8eebdb60-be68-4380-a902-8cd0a2a0744c",
					"invalid-field2":"2017-08-09T13:30:03.987-07:00"}`,
				}

				expectedCode := []int{
					http.StatusBadRequest,
					http.StatusOK,
					http.StatusBadRequest,
				}

				expectedBody := []string{
					"json",
					"",
					"",
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					uri.Path = testApiMan.configStatusEndpoint
					log.Debug(uri.String(), data)
					req, err := http.NewRequest("PUT", uri.String(), strings.NewReader(data))
					Expect(err).Should(Succeed())
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})

			It("/configurations/status should populate errors from tracker", func() {
				// setup test data
				testData := [][]string{
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), time.Now().Format(iso8601)},
					{GenerateUUID(), time.Now().Format(iso8601)},
				}

				expectedCode := []int{
					http.StatusBadRequest,
					http.StatusInternalServerError,
					http.StatusBadGateway,
				}

				expectedBody := []string{
					"",
					"",
					"",
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					uri.Path = testApiMan.configStatusEndpoint
					log.Debug(uri.String())
					configNum := mathrand.Intn(5)
					statusDetails := make([]statusDetailsJson, 0)
					expectedSlice := []string{}
					for j := 0; j < configNum; j++ {
						configStatus, expected := generateStatusDetails(0)
						statusDetails = append(statusDetails, *configStatus)
						expectedSlice = append(expectedSlice, expected)
					}

					expectedSlice = append(expectedSlice, data...)

					if expectedCode[i] == http.StatusOK {
						expectedBody[i] = strings.Join(expectedSlice, " ")
					}

					reqBody, err := json.Marshal(configStatusBody{
						ServiceId:     data[0],
						ReportedTime:  data[1],
						StatusDetails: statusDetails,
					})
					Expect(err).Should(Succeed())
					req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(reqBody))
					Expect(err).Should(Succeed())

					dummyClient.code = expectedCode[i]
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})
		})

		Context("PUT /register/{uuid}", func() {
			It("/register should validate request", func() {
				// setup test data
				dummyClient.code = http.StatusOK

				testData := [][]string{
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "name", "type"},
					{"", "pod", "podType", time.Now().Format(iso8601), "name", "type"},
					{GenerateUUID(), "", "podType", time.Now().Format(iso8601), "name", "type"},
					{GenerateUUID(), "pod", "", time.Now().Format(iso8601), "name", "type"},
					{GenerateUUID(), "pod", "podType", "", "name", "type"},
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "", "type"},
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "name", ""},
					{"invalid-uuid", "pod", "podType", time.Now().Format(iso8601), "name", "type"},
					{GenerateUUID(), "pod", "podType", "invalid-time", "name", "type"},
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "name", "type"},
				}

				pathUuid := []string{
					"",
					GenerateUUID(),
					"",
					"",
					"",
					"",
					"",
					"",
					"",
					GenerateUUID(),
				}

				expectedCode := []int{
					http.StatusOK,
					http.StatusBadRequest,
					http.StatusOK,
					http.StatusOK,
					http.StatusBadRequest,
					http.StatusOK,
					http.StatusOK,
					http.StatusBadRequest,
					http.StatusBadRequest,
					http.StatusBadRequest,
				}

				expectedBody := []string{
					strings.Join(testData[0], " "),
					"mismatch UUID",
					strings.Join(testData[2], " "),
					strings.Join(testData[3], " "),
					"reportedTime",
					strings.Join(testData[5], " "),
					strings.Join(testData[6], " "),
					"UUID",
					"reportedTime",
					"mismatch UUID",
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					uuid := pathUuid[i]
					if uuid == "" {
						uuid = data[0]
					}
					uri.Path = strings.Replace(testApiMan.registerEndpoint, "{uuid}", uuid, 1)
					reqBody, err := json.Marshal(registerBody{
						Uuid:         data[0],
						Pod:          data[1],
						PodType:      data[2],
						ReportedTime: data[3],
						Name:         data[4],
						Type:         data[5],
					})
					Expect(err).Should(Succeed())
					log.Debug(uri.String())
					req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(reqBody))
					Expect(err).Should(Succeed())
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})

			It("/register should populate errors from tracker", func() {
				// setup test data
				testData := [][]string{
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "name", "type"},
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "name", "type"},
					{GenerateUUID(), "pod", "podType", time.Now().Format(iso8601), "name", "type"},
				}

				expectedCode := []int{
					http.StatusBadRequest,
					http.StatusInternalServerError,
					http.StatusBadGateway,
				}

				expectedBody := []string{
					strings.Join(testData[0], " "),
					strings.Join(testData[1], " "),
					strings.Join(testData[2], " "),
				}

				// setup http client
				uri, err := url.Parse(apiTestUrl)
				Expect(err).Should(Succeed())
				for i, data := range testData {
					dummyClient.code = expectedCode[i]
					uuid := data[0]
					uri.Path = strings.Replace(testApiMan.registerEndpoint, "{uuid}", uuid, 1)
					reqBody, err := json.Marshal(registerBody{
						Uuid:         data[0],
						Pod:          data[1],
						PodType:      data[2],
						ReportedTime: data[3],
						Name:         data[4],
						Type:         data[5],
					})
					Expect(err).Should(Succeed())
					log.Debug(uri.String())
					req, err := http.NewRequest("PUT", uri.String(), bytes.NewReader(reqBody))
					Expect(err).Should(Succeed())
					// http put
					res, err := testClient.Do(req)
					Expect(err).Should(Succeed())
					// parse response
					defer res.Body.Close()
					Expect(res.StatusCode).Should(Equal(expectedCode[i]))
					body, err := ioutil.ReadAll(res.Body)
					Expect(err).Should(Succeed())
					Expect(strings.Contains(strings.ToLower(string(body)), strings.ToLower(expectedBody[i]))).To(BeTrue())
				}
			})
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

func generateStatusDetails(flag int) (*statusDetailsJson, string) {
	id := GenerateUUID()
	var ret *statusDetailsJson
	var expected string
	switch flag {
	case 1: // invalid Status
		ret = &statusDetailsJson{
			Status:          "",
			ConfigurationId: id,
			ErrorCode:       "errorcode" + id,
			Message:         "message" + id,
		}
		expected = "Status"
	case 2: // invalid ConfigurationId
		ret = &statusDetailsJson{
			Status:          "status" + id,
			ConfigurationId: "",
			ErrorCode:       "errorcode" + id,
			Message:         "message" + id,
		}
		expected = "ConfigurationId"
	default:
		ret = &statusDetailsJson{
			Status:          "status" + id,
			ConfigurationId: id,
			ErrorCode:       "errorcode" + id,
			Message:         "message" + id,
		}
		expected = strings.Join([]string{ret.Status, ret.ConfigurationId, ret.ErrorCode, ret.Message}, " ")
	}

	return ret, expected
}
