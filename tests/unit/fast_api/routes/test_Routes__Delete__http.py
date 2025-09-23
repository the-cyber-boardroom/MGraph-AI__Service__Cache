import concurrent.futures
import pytest
import requests
import time
from unittest                                                                   import TestCase
from osbot_fast_api.utils.Fast_API_Server                                       import Fast_API_Server
from osbot_fast_api_serverless.utils.testing.skip_tests                         import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                     import __, __SKIP__
from osbot_utils.utils.Misc                                                     import is_guid
from osbot_utils.utils.Objects                                                  import obj
from tests.unit.Service__Cache__Test_Objs                                       import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE

class test_Routes__Delete__http(TestCase):                                           # Local HTTP tests using temp FastAPI server

    @classmethod
    def setUpClass(cls):                                                             # ONE-TIME expensive setup
        #if in_github_action():
        #    pytest.skip("Skipping HTTP tests on GitHub Actions")

        cls.test_objs       = setup__service__cache__test_objs()                   # Use shared infrastructure
        cls.cache_fixtures  = cls.test_objs.cache_fixtures
        cls.service__app    = cls.test_objs.fast_api__app

        cls.fast_api_server = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url        = cls.fast_api_server.url().rstrip("/")                 # remove trailing slashes
        cls.headers         = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE}

        # Use fixtures namespace for reading, separate for deletion
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.test_namespace     = f"http-delete-{int(time.time())}"

        # Track created items for cleanup (simpler approach)
        cls.created_cache_ids  = []

    @classmethod
    def tearDownClass(cls):                                                         # Clean up and stop server
        for cache_id in cls.created_cache_ids:
            try:
                url = f"{cls.base_url}/{cls.test_namespace}/delete/{cache_id}"
                requests.delete(url, headers=cls.headers)
            except:
                pass

        cls.fast_api_server.stop()

    def _store_for_deletion(self, data      : str  = "test data"      ,             # Helper to create deletable items
                                  strategy  : str  = "direct"         ,
                                  namespace : str  = None
                            ) -> str:
        namespace = namespace or self.test_namespace
        url       = f"{self.base_url}/{namespace}/{strategy}/store/string"
        response  = requests.post(url, json=data, headers=self.headers)

        assert response.status_code == 200
        cache_id = response.json()['cache_id']

        if namespace == self.test_namespace:                                        # Only track our test namespace
            self.created_cache_ids.append(cache_id)

        return cache_id

    def test_01_health_check(self):                                                 # Verify API is accessible
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
        assert response.status_code == 200
        assert response.json()      == {'status': 'ok'}

    def test_02_delete_basic_workflow(self):                                        # Test basic delete workflow
        test_data = f"HTTP delete test at {time.time()}"
        cache_id  = self._store_for_deletion(test_data, strategy="direct")

        assert is_guid(cache_id)

        # Verify exists
        retrieve_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
        response     = requests.get(retrieve_url, headers=self.headers)
        assert response.status_code        == 200
        assert response.json()['data']     == test_data

        # Delete
        delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
        response   = requests.delete(delete_url, headers=self.headers)

        assert response.status_code         == 200
        result = response.json()
        assert result['status']             == 'success'
        assert result['cache_id']           == cache_id
        assert result['deleted_count']      > 0

        # Verify deleted
        response = requests.get(retrieve_url, headers=self.headers)
        assert obj(response.json()) == __(detail = __(resource_id    = None                                     ,
                                                      cache_hash     = None                                     ,
                                                      cache_id       = cache_id                                 ,
                                                      namespace      = self.test_namespace                      ,
                                                      error_type     = 'NOT_FOUND'                              ,
                                                      message        = 'The requested cache entry was not found',
                                                      timestamp      = __SKIP__                                 ,
                                                      request_id     = __SKIP__                                 ,
                                                      resource_type  = 'cache_entry'                           ))


        # Remove from tracking since deleted
        self.created_cache_ids.remove(cache_id)

    def test_03_delete_non_existent(self):                                          # Test deleting non-existent entry
        non_existent = "12345678-1234-1234-1234-123456789012"
        url          = f"{self.base_url}/{self.test_namespace}/delete/{non_existent}"
        response     = requests.delete(url, headers=self.headers)

        assert response.status_code == 200
        result = response.json()
        assert result['status']     == 'not_found'
        assert result['message']    == f'Cache ID {non_existent} not found'

    def test_04_namespace_isolation(self):                                          # Test namespace isolation
        skip__if_not__in_github_actions()
        ns1 = f"http-ns1-{int(time.time())}"
        ns2 = f"http-ns2-{int(time.time())}"

        # Store in different namespaces
        data      = "namespace isolation test"
        cache_id1 = self._store_for_deletion(data, namespace=ns1)
        cache_id2 = self._store_for_deletion(data, namespace=ns2)

        # Try to delete ns2 item from ns1 (should fail)
        wrong_url = f"{self.base_url}/{ns1}/delete/{cache_id2}"
        response  = requests.delete(wrong_url, headers=self.headers)
        assert response.json()['status'] == 'not_found'

        # Verify ns2 item still exists
        check_url = f"{self.base_url}/{ns2}/retrieve/{cache_id2}"
        response  = requests.get(check_url, headers=self.headers)
        assert response.json()['data'] == data

        # Clean up both
        requests.delete(f"{self.base_url}/{ns1}/delete/{cache_id1}", headers=self.headers)
        requests.delete(f"{self.base_url}/{ns2}/delete/{cache_id2}", headers=self.headers)

    def test_05_delete_all_strategies(self):                                        # Test delete works with all strategies
        skip__if_not__in_github_actions()
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                cache_id = self._store_for_deletion(f"delete {strategy}", strategy=strategy)

                delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
                response   = requests.delete(delete_url, headers=self.headers)

                assert response.status_code         == 200
                result = response.json()
                assert result['status']             == 'success'
                assert result['deleted_count']      > 8                             # All strategies create at least 9 files

                self.created_cache_ids.remove(cache_id)

    def test_06_concurrent_deletes(self):                                           # Test concurrent delete operations
        skip__if_not__in_github_actions()
        # Create items to delete
        cache_ids = [self._store_for_deletion(f"concurrent {i}") for i in range(10)]

        def delete_item(cache_id):                                                  # Delete a cache item
            url      = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
            response = requests.delete(url, headers=self.headers)
            return response.json()['status'] == 'success' if response.status_code == 200 else False

        # Run concurrent deletes
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(delete_item, cid) for cid in cache_ids]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(results)

        # Clear from tracking
        for cache_id in cache_ids:
            self.created_cache_ids.remove(cache_id)

    def test_07_delete_multiple_times(self):                                        # Test deleting same ID multiple times
        skip__if_not__in_github_actions()
        cache_id   = self._store_for_deletion("multiple delete test")
        delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"

        # First delete should succeed
        response = requests.delete(delete_url, headers=self.headers)
        assert response.status_code        == 200
        assert response.json()['status']   == 'success'

        # Second delete should not find it
        response = requests.delete(delete_url, headers=self.headers)
        assert response.status_code        == 200
        assert response.json()['status']   == 'not_found'

        # Third delete should also not find it
        response = requests.delete(delete_url, headers=self.headers)
        assert response.status_code        == 200
        assert response.json()['status']   == 'not_found'

        self.created_cache_ids.remove(cache_id)

    def test_08_performance_batch_deletes(self):                                    # Test performance with batch deletes
        skip__if_not__in_github_actions()
        num_items = 5
        cache_ids = [self._store_for_deletion(f"perf {i}", "direct") for i in range(num_items)]

        start_time = time.time()

        for cache_id in cache_ids:
            url      = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
            response = requests.delete(url, headers=self.headers)
            assert response.status_code        == 200
            assert response.json()['status']   == 'success'

        elapsed = time.time() - start_time
        assert elapsed < 1.0                                                        # Should be fast

        # Clear from tracking
        for cache_id in cache_ids:
            self.created_cache_ids.remove(cache_id)

    def test_10_delete_binary_data(self):                                           # Test deleting binary data
        namespace   = f"delete-binary-{int(time.time())}"
        binary_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100                         # Fake PNG header

        # Store binary
        store_url = f"{self.base_url}/{namespace}/direct/store/binary"
        response  = requests.post(store_url                                       ,
                                 data    = binary_data                            ,
                                 headers = {**self.headers, "Content-Type": "application/octet-stream"})

        assert response.status_code == 200
        cache_id = response.json()['cache_id']

        # Delete binary
        delete_url = f"{self.base_url}/{namespace}/delete/{cache_id}"
        response   = requests.delete(delete_url, headers=self.headers)

        assert response.status_code             == 200
        result = response.json()
        assert result['status']                 == 'success'
        assert result['deleted_count']          == 9

    def test_11_delete_json_data(self):                                             # Test deleting JSON data
        namespace = f"delete-json-{int(time.time())}"
        json_data = {"test": "delete", "nested": {"key": "value"}}

        # Store JSON
        store_url = f"{self.base_url}/{namespace}/temporal_versioned/store/json"
        response  = requests.post(store_url, json=json_data, headers=self.headers)

        assert response.status_code == 200
        cache_id = response.json()['cache_id']

        # Delete JSON
        delete_url = f"{self.base_url}/{namespace}/delete/{cache_id}"
        response   = requests.delete(delete_url, headers=self.headers)

        assert response.status_code    == 200
        assert response.json()['status'] == 'success'

    def test_12_integration_with_all_routes(self):                                  # Test integration with other routes
        skip__if_not__in_github_actions()
        namespace = f"integration-{int(time.time())}"
        test_data = "integration test data"

        # Store via Routes__Store
        store_url = f"{self.base_url}/{namespace}/temporal/store/string"
        response  = requests.post(store_url, json=test_data, headers=self.headers)

        assert response.status_code == 200
        cache_id   = response.json()['cache_id']
        cache_hash = response.json()['hash']

        # Check exists via Routes__Exists
        exists_url = f"{self.base_url}/{namespace}/exists/hash/{cache_hash}"
        response   = requests.get(exists_url, headers=self.headers)
        assert response.json()['exists'] is True

        # Retrieve via Routes__Retrieve
        retrieve_url = f"{self.base_url}/{namespace}/retrieve/{cache_id}"
        response     = requests.get(retrieve_url, headers=self.headers)
        assert response.json()['data'] == test_data

        # Delete via Routes__Delete
        delete_url = f"{self.base_url}/{namespace}/delete/{cache_id}"
        response   = requests.delete(delete_url, headers=self.headers)
        assert response.json()['status'] == 'success'

        # Verify gone via all routes
        response = requests.get(exists_url, headers=self.headers)
        assert response.json()['exists'] is False

        response = requests.get(retrieve_url, headers=self.headers)
        assert obj(response.json()).detail.error_type == 'NOT_FOUND'

    def test_99_cleanup_verification(self):                                         # Verify cleanup worked
        assert len(self.created_cache_ids) == 0                                     # All items should be cleaned up