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
	"github.com/30x/apid-core/data"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"sync"
	"time"
)

const (
	readyBlobId      = "gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a"
	readyblobLocalFs = "/tmp/tmpapid/gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a"
)

var (
	unreadyBlobIds = map[string]bool{
		"gcs:SHA-512:8fcc902465ccb32ceff25fa9f6fb28e3b314dbc2874c0f8add02f4e29c9e2798d344c51807aa1af56035cf09d39c800cf605d627ba65723f26d8b9c83c82d2f2": true,
		"gcs:SHA-512:9938d075b8c8925e118a61c047d330a6ba852c2b3ccb2fd2c4ecf6f444482ef3a24ef2e7cd3b8a01be771135d78db370518debce244a33289c1bb3d7f325d4a2": true,
	}
)

var _ = Describe("data", func() {
	var testCount int
	var testDbMan *dbManager
	var _ = BeforeEach(func() {
		testCount += 1
		testDbMan = &dbManager{
			data:  services.Data(),
			dbMux: sync.RWMutex{},
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

	Context("db tests", func() {
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
			Expect(count).Should(Equal(3))
		})

		It("should get empty slice if no deployments are ready", func() {
			deps, err := testDbMan.getReadyDeployments()
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(BeZero())
		})

		It("should succefully update local FS location", func() {

			err := testDbMan.updateLocalFsLocation(readyBlobId, readyblobLocalFs)
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

			err := testDbMan.updateLocalFsLocation(readyBlobId, readyblobLocalFs)
			Expect(err).Should(Succeed())

			// apid_blob_available
			location, err := testDbMan.getLocalFSLocation(readyBlobId)
			Expect(err).Should(Succeed())
			Expect(location).Should(Equal(readyblobLocalFs))
		})

		It("should succefully get ready deployments", func() {

			err := testDbMan.updateLocalFsLocation(readyBlobId, readyblobLocalFs)
			Expect(err).Should(Succeed())
			deps, err := testDbMan.getReadyDeployments()
			Expect(err).Should(Succeed())
			Expect(len(deps)).Should(Equal(1))
			Expect(deps[0].BlobID).Should(Equal(readyBlobId))
		})

		It("should succefully get unready blob ids", func() {

			err := testDbMan.updateLocalFsLocation(readyBlobId, readyblobLocalFs)
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
	_, err := db.Exec(`
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

	_, err = db.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'1dc4895e-6494-4b59-979f-5f4c89c073b4',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'',
		'gcs:SHA-512:39ca7ae89bb9468af34df8bc873748b4035210c91bcc01359c092c1d51364b5f3df06bc69a40621acfaa46791af9ea41bc0f3429a84738ba1a7c8d394859601a',
		'',
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

	_, err = db.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80',
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

	_, err = db.Exec(`
		INSERT INTO "metadata_runtime_entity_metadata" VALUES(
		'65e7c3d3-9ef7-4e41-84ae-6cc385178e9d',
		'73fcac6c-5d9f-44c1-8db0-333efda3e6e8',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80',
		'gcs:SHA-512:9938d075b8c8925e118a61c047d330a6ba852c2b3ccb2fd2c4ecf6f444482ef3a24ef2e7cd3b8a01be771135d78db370518debce244a33289c1bb3d7f325d4a2',
		'',
		'VIRTUAL_HOST',
		'default',
		'',
		'/organizations/Org1/environments/test/virtualhosts/default',
		'2017-06-27 03:48:18.284+00:00',
		'-NA-',
		'2017-06-27 03:48:18.284+00:00',
		'-NA-',
		'ada76573-68e3-4f1a-a0f9-cbc201a97e80'
		);
	`)
	Expect(err).Should(Succeed())
}
