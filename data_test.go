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
		testDbMan.initDb()
		time.Sleep(100 * time.Millisecond)
	})

	var _ = AfterEach(func() {
		testDbMan = nil
		data.Delete(data.VersionedDBID("common", "test"+strconv.Itoa(testCount)))
	})

	Context("db tests", func() {
		It("should succefully initialized tables", func() {
			// edgex_blob_available
			rows, err := testDbMan.getDb().Query(`
				SELECT count(*) from edgex_blob_available;
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
			Expect(count).Should(Equal(1))
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

}
