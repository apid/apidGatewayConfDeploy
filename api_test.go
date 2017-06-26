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
		/*
		It("should debounce requests", func(done Done) {
			var in = make(chan interface{})
			var out = make(chan []interface{})

			go debounce(in, out, 3*time.Millisecond)

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

		It("should get current deployments", func() {

			deploymentID := "api_get_current"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			Expect(res.StatusCode).Should(Equal(http.StatusOK))

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(len(depRes)).To(Equal(1))

			dep := depRes[0]

			Expect(dep.ID).To(Equal(deploymentID))
			Expect(dep.ScopeId).To(Equal(deploymentID))
			Expect(dep.DisplayName).To(Equal(deploymentID))

			var config bundleConfigJson

			err = json.Unmarshal(dep.ConfigJson, &config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(config.Name).To(Equal("/bundles/1"))

			err = json.Unmarshal(dep.BundleConfigJson, &config)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(config.Name).To(Equal("/bundles/1"))
		})

		It("should get 304 for no change", func() {

			deploymentID := "api_no_change"
			insertTestDeployment(testServer, deploymentID)

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.Header.Get("etag")).ShouldNot(BeEmpty())

			req, err := http.NewRequest("GET", uri.String(), nil)
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("If-None-Match", res.Header.Get("etag"))

			res, err = http.DefaultClient.Do(req)
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusNotModified))
		})

		It("should get empty set after blocking if no deployments", func() {

			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint

			query := uri.Query()
			query.Add("block", "1")
			uri.RawQuery = query.Encode()

			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()

			var depRes ApiDeploymentResponse
			body, err := ioutil.ReadAll(res.Body)
			Expect(err).ShouldNot(HaveOccurred())
			json.Unmarshal(body, &depRes)

			Expect(res.StatusCode).Should(Equal(http.StatusOK))
			Expect(string(body)).Should(Equal("[]"))
		})

		It("should get new deployment set after blocking", func(done Done) {

			deploymentID := "api_get_current_blocking"
			insertTestDeployment(testServer, deploymentID)
			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			eTag := res.Header.Get("etag")
			Expect(eTag).ShouldNot(BeEmpty())

			deploymentID = "api_get_current_blocking2"
			go func() {
				defer GinkgoRecover()

				query := uri.Query()
				query.Add("block", "1")
				uri.RawQuery = query.Encode()
				req, err := http.NewRequest("GET", uri.String(), nil)
				req.Header.Add("Content-Type", "application/json")
				req.Header.Add("If-None-Match", eTag)

				res, err := http.DefaultClient.Do(req)
				Expect(err).ShouldNot(HaveOccurred())
				defer res.Body.Close()
				Expect(res.StatusCode).To(Equal(http.StatusOK))

				Expect(res.Header.Get("etag")).ShouldNot(BeEmpty())
				Expect(res.Header.Get("etag")).ShouldNot(Equal(eTag))

				var depRes ApiDeploymentResponse
				body, err := ioutil.ReadAll(res.Body)
				Expect(err).ShouldNot(HaveOccurred())
				json.Unmarshal(body, &depRes)

				Expect(len(depRes)).To(Equal(2))

				dep := depRes[1]

				Expect(dep.ID).To(Equal(deploymentID))
				Expect(dep.ScopeId).To(Equal(deploymentID))
				Expect(dep.DisplayName).To(Equal(deploymentID))

				close(done)
			}()

			time.Sleep(250 * time.Millisecond) // give api call above time to block
			insertTestDeployment(testServer, deploymentID)
			deploymentsChanged <- deploymentID
		})

		It("should get 304 after blocking if no new deployment", func() {

			deploymentID := "api_no_change_blocking"
			insertTestDeployment(testServer, deploymentID)
			uri, err := url.Parse(testServer.URL)
			uri.Path = deploymentsEndpoint
			res, err := http.Get(uri.String())
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.Header.Get("etag")).ShouldNot(BeEmpty())

			query := uri.Query()
			query.Add("block", "1")
			uri.RawQuery = query.Encode()
			req, err := http.NewRequest("GET", uri.String(), nil)
			req.Header.Add("Content-Type", "application/json")
			req.Header.Add("If-None-Match", res.Header.Get("etag"))

			res, err = http.DefaultClient.Do(req)
			Expect(err).ShouldNot(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusNotModified))
		})
		*/
	})
})