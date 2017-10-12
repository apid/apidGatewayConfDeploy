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
	"database/sql"
	"fmt"
	"github.com/apid/apid-core"
	"github.com/apid/apid-core/data"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	testBlobLocalFsPrefix = "/tmp/tmpapid/"
	readyBlobId           = "gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a"
	readyResourceId       = "gcs:SHA-512:ddd64d03c365dde4bb175cabb7d84beeb81dae11f1e326b30c9035b74be3ecb537187bdf35568647aa1b2adb341499516ca2faf2d73b78b1b98cba038f2a9e3c"
)

var (
	unreadyBlobIds = map[string]bool{
		"gcs:SHA-512:8fcc902465ccb32ceff25fa9f6fb28e3b314dbc2874c0f8add02f4e29c9e2798d344c51807aa1af56035cf09d39c800cf605d627ba65723f26d8b9c83c82d2f2": true,
		"gcs:SHA-512:0c648779da035bfe0ac21f6268049aa0ae74d9d6411dadefaec33991e55c2d66c807e06f7ef84e0947f7c7d63b8c9e97cf0684cbef9e0a86b947d73c74ae7455": true,
	}
)

var _ = Describe("data", func() {
	var testCount int
	var testDbMan *dbManager
	var _ = BeforeEach(func() {
		testCount += 1
		testDbMan = &dbManager{
			data:     services.Data(),
			dbMux:    sync.RWMutex{},
			lsnMutex: sync.RWMutex{},
		}
		testDbMan.setDbVersion("test" + strconv.Itoa(testCount))
		initTestDb(testDbMan.getDb())
		err := testDbMan.initDb()
		Expect(err).Should(Succeed())
		time.Sleep(100 * time.Millisecond)
	})

	var _ = AfterEach(func() {
		testDbMan = nil
		data.Delete(data.VersionedDBID("common", "test"+strconv.Itoa(testCount)))
	})

	Context("basic db tests", func() {
		It("initDb() should be idempotent", func() {
			err := testDbMan.initDb()
			Expect(err).Should(Succeed())
		})

		It("should succefully initialized tables", func() {
			// apid_blob_available
			rows, err := testDbMan.getDb().Query(`
				SELECT count(*) from apid_blob_available;
			`)
			Expect(err).Should(Succeed())
			defer rows.Close()
			var count int
			for rows.Next() {
				rows.Scan(&count)
			}
			Expect(count).Should(Equal(0))

			// metadata_runtime_entity_metadata
			rows, err = testDbMan.getDb().Query(`
				SELECT count(*) from metadata_runtime_entity_metadata;
			`)
			Expect(err).Should(Succeed())
			defer rows.Close()
			for rows.Next() {
				rows.Scan(&count)
			}
			Expect(count).Should(Equal(6))
		})

		It("should initialize support for long-polling", func() {
			// APID_CONFIGURATION_LSN
			rows, err := testDbMan.getDb().Query(`
				SELECT lsn from APID_CONFIGURATION_LSN;
			`)
			Expect(err).Should(Succeed())
			defer rows.Close()
			count := 0
			var lsn sql.NullString
			for rows.Next() {
				count++
				rows.Scan(&lsn)
			}
			Expect(count).Should(Equal(1))
			Expect(lsn.Valid).Should(BeTrue())
			Expect(lsn.String).Should(Equal(InitLSN))
		})

		It("should maintain LSN", func() {
			testLSN := fmt.Sprintf("%d.%d.%d", testCount, testCount, testCount)
			// write
			err := testDbMan.updateLSN(testLSN)
			Expect(err).Should(Succeed())
			rows, err := testDbMan.getDb().Query(`
				SELECT lsn from APID_CONFIGURATION_LSN;
			`)
			defer rows.Close()
			count := 0
			var lsn sql.NullString
			for rows.Next() {
				count++
				rows.Scan(&lsn)
			}
			Expect(count).Should(Equal(1))
			Expect(lsn.Valid).Should(BeTrue())
			Expect(lsn.String).Should(Equal(testLSN))

			// read
			Expect(testDbMan.getLSN()).Should(Equal(testLSN))

			//load
			Expect(testDbMan.loadLsnFromDb()).Should(Succeed())
			Expect(testDbMan.apidLSN).Should(Equal(testLSN))
		})
	})

	Context("configuration tests", func() {
		It("should get empty slice if no deployments are ready", func() {
			deps, err := testDbMan.getReadyDeployments("")
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(BeZero())
		})

		It("should succefully update local FS location", func() {

			err := testDbMan.updateLocalFsLocation(testBlobId, testBlobLocalFsPrefix+testBlobId)
			Expect(err).Should(Succeed())
			// apid_blob_available
			rows, err := testDbMan.getDb().Query(`
				SELECT count(*) from apid_blob_available;
			`)
			Expect(err).Should(Succeed())
			defer rows.Close()
			var count int
			for rows.Next() {
				rows.Scan(&count)
			}
			Expect(count).Should(Equal(1))
		})

		It("should succefully get local FS location", func() {

			err := testDbMan.updateLocalFsLocation(testBlobId, testBlobLocalFsPrefix+testBlobId)
			Expect(err).Should(Succeed())

			// apid_blob_available
			location, err := testDbMan.getLocalFSLocation(testBlobId)
			Expect(err).Should(Succeed())
			Expect(location).Should(Equal(testBlobLocalFsPrefix + testBlobId))
		})

		It("should get configuration by Id", func() {
			config, err := testDbMan.getConfigById("3ecd351c-1173-40bf-b830-c194e5ef9038")
			Expect(err).Should(Succeed())
			expectedResponse := &Configuration{
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
			Expect(config).ShouldNot(BeNil())
			Expect(reflect.DeepEqual(expectedResponse, config)).Should(BeTrue())
		})

		It("should get non-nil error for nonexistent Id", func() {
			_, err := testDbMan.getConfigById("3ecd351c-aaaa-40bf-b830-c194e5ef9038")
			Expect(err).ShouldNot(Succeed())
		})

		It("should successfully get all ready configurations", func() {

			err := testDbMan.updateLocalFsLocation(readyBlobId, testBlobLocalFsPrefix+readyBlobId)
			Expect(err).Should(Succeed())
			err = testDbMan.updateLocalFsLocation(readyResourceId, testBlobLocalFsPrefix+readyResourceId)
			Expect(err).Should(Succeed())

			deps, err := testDbMan.getReadyDeployments("")
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(Equal(2))
			for _, dep := range deps {
				Expect(dep.BlobID).Should(Equal(readyBlobId))
				if dep.BlobResourceID != "" {
					Expect(dep.BlobResourceID).Should(Equal(readyResourceId))
				}
			}
		})

		It("should get ready configurations by type filter", func() {

			err := testDbMan.updateLocalFsLocation(readyBlobId, testBlobLocalFsPrefix+readyBlobId)
			Expect(err).Should(Succeed())
			err = testDbMan.updateLocalFsLocation(readyResourceId, testBlobLocalFsPrefix+readyResourceId)
			Expect(err).Should(Succeed())

			deps, err := testDbMan.getReadyDeployments("ORGANIZATION")
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(Equal(1))
			Expect(deps[0].ID).Should(Equal("319963ff-217e-4ecc-8d6e-c3665e962d1e"))

			deps, err = testDbMan.getReadyDeployments("ENVIRONMENT")
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(Equal(1))
			Expect(deps[0].ID).Should(Equal("1dc4895e-6494-4b59-979f-5f4c89c073b4"))

			deps, err = testDbMan.getReadyDeployments("INVALID-TYPE")
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(Equal(0))
		})

		It("should succefully get all unready blob ids", func() {

			err := testDbMan.updateLocalFsLocation(readyBlobId, testBlobLocalFsPrefix+readyBlobId)
			Expect(err).Should(Succeed())
			err = testDbMan.updateLocalFsLocation(readyResourceId, testBlobLocalFsPrefix+readyResourceId)
			Expect(err).Should(Succeed())

			ids, err := testDbMan.getUnreadyBlobs()
			Expect(err).Should(Succeed())
			Expect(len(ids)).Should(Equal(2))
			for _, id := range ids {
				Expect(unreadyBlobIds[id]).Should(BeTrue())
			}
		})

	})

})

//initialize DB for tests
func initTestDb(db apid.DB) {
	tx, err := db.Begin()
	Expect(err).Should(Succeed())
	defer tx.Rollback()
	_, err = tx.Exec(`
		CREATE TABLE metadata_runtime_entity_metadata (
		id text,
		organization_id text,
		environment_id text,
		bean_blob_id text,
		resource_blob_id text,
		type text,
		name text,
		revision text,
		path text,
		created_at blob,
		created_by text,
		updated_at blob,
		updated_by text,
		_change_selector text,
		primary key (id));
	`)
	Expect(err).Should(Succeed())

	// ready blob, empty resource
	_, err = tx.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'1dc4895e-6494-4b59-979f-5f4c89c073b4',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'',
		'gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a',
		NULL,
		'ENVIRONMENT',
		'Org1',
		'',
		'/organizations/edgex01//environments/prod/',
		'2017-06-27 03:14:45.748+00:00',
		'defaultUser',
		'2017-06-27 03:15:03.557+00:00',
		'defaultUser',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8'
		);
	`)
	Expect(err).Should(Succeed())

	// ready blob, ready resource
	_, err = tx.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'319963ff-217e-4ecc-8d6e-c3665e962d1e',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'',
		'gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a',
		'gcs:SHA-512:ddd64d03c365dde4bb175cabb7d84beeb81dae11f1e326b30c9035b74be3ecb537187bdf35568647aa1b2adb341499516ca2faf2d73b78b1b98cba038f2a9e3c',
		'ORGANIZATION',
		'Org1',
		'',
		'/organizations/Org1/',
		'2017-06-27 03:14:45.748+00:00',
		'defaultUser',
		'2017-06-27 03:15:03.557+00:00',
		'defaultUser',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8'
		);
	`)
	Expect(err).Should(Succeed())

	// ready blob, unready resource
	_, err = tx.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'3af44bb7-0a74-4283-860c-3561e6c19132',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'',
		'gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a',
		'gcs:SHA-512:0c648779da035bfe0ac21f6268049aa0ae74d9d6411dadefaec33991e55c2d66c807e06f7ef84e0947f7c7d63b8c9e97cf0684cbef9e0a86b947d73c74ae7455',
		'ORGANIZATION',
		'Org1',
		'',
		'/organizations/Org1/',
		'2017-06-27 03:14:45.748+00:00',
		'defaultUser',
		'2017-06-27 03:15:03.557+00:00',
		'defaultUser',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8'
		);
	`)
	Expect(err).Should(Succeed())

	// unready blob, empty resource
	_, err = tx.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'd5ffd9db-4795-43eb-b645-d2a0b6c8ac6a',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80',
		'gcs:SHA-512:8fcc902465ccb32ceff25fa9f6fb28e3b314dbc2874c0f8add02f4e29c9e2798d344c51807aa1af56035cf09d39c800cf605d627ba65723f26d8b9c83c82d2f2',
		'',
		'ENVIRONMENT',
		'test',
		'',
		'/organizations/Org1//environments/test/',
		'2017-06-27 03:14:46.018+00:00',
		'defaultUser',
		'2017-06-27 03:14:46.018+00:00',
		'defaultUser',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80'
		);
	`)
	Expect(err).Should(Succeed())

	// unready blob, ready resource
	_, err = tx.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'84ac8d68-b3d1-4bcc-ad0d-c6a0ed67e16c',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80',
		'gcs:SHA-512:8fcc902465ccb32ceff25fa9f6fb28e3b314dbc2874c0f8add02f4e29c9e2798d344c51807aa1af56035cf09d39c800cf605d627ba65723f26d8b9c83c82d2f2',
		'gcs:SHA-512:ddd64d03c365dde4bb175cabb7d84beeb81dae11f1e326b30c9035b74be3ecb537187bdf35568647aa1b2adb341499516ca2faf2d73b78b1b98cba038f2a9e3c',
		'ENVIRONMENT',
		'test',
		'',
		'/organizations/Org1//environments/test/',
		'2017-06-27 03:14:46.018+00:00',
		'defaultUser',
		'2017-06-27 03:14:46.018+00:00',
		'defaultUser',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80'
		);
	`)
	Expect(err).Should(Succeed())

	// unready blob, unready resource
	_, err = tx.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'3ecd351c-1173-40bf-b830-c194e5ef9038',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80',
		'gcs:SHA-512:8fcc902465ccb32ceff25fa9f6fb28e3b314dbc2874c0f8add02f4e29c9e2798d344c51807aa1af56035cf09d39c800cf605d627ba65723f26d8b9c83c82d2f2',
		'gcs:SHA-512:0c648779da035bfe0ac21f6268049aa0ae74d9d6411dadefaec33991e55c2d66c807e06f7ef84e0947f7c7d63b8c9e97cf0684cbef9e0a86b947d73c74ae7455',
		'ENVIRONMENT',
		'test',
		'',
		'/organizations/Org1//environments/test/',
		'2017-06-27 03:14:46.018+00:00',
		'defaultUser',
		'2017-06-27 03:14:46.018+00:00',
		'defaultUser',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80'
		);
	`)
	Expect(err).Should(Succeed())
	Expect(tx.Commit()).Should(Succeed())
}
