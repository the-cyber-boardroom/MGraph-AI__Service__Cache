import concurrent.futures
import pytest
import requests
import time
from unittest                                 import TestCase
from osbot_fast_api.utils.Fast_API_Server     import Fast_API_Server
from osbot_utils.utils.Env                    import in_github_action
from osbot_utils.utils.Misc                   import is_guid
from tests.unit.Service__Fast_API__Test_Objs  import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Delete__http(TestCase):                                  # Local HTTP tests using temp FastAPI server

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME expensive setup
        if in_github_action():
            pytest.skip("Skipping this test on GitHub Actions (because we are getting 404 on the routes)")

        cls.key_name     = TEST_API_KEY__NAME
        cls.key_value    = TEST_API_KEY__VALUE
        cls.auth_headers = {cls.key_name: cls.key_value}

        if not cls.key_name or not cls.key_value:
            pytest.skip("No Auth key name or key value provided")

        cls.service__fast_api = setup__service_fast_api_test_objs().fast_api
        cls.service__app      = cls.service__fast_api.app()
        cls.fast_api_server   = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url = cls.fast_api_server.url()
        cls.headers  = cls.auth_headers

        # Track all created resources for cleanup
        cls.created_resources = []

        # Test namespace with timestamp to avoid conflicts
        cls.test_namespace = f"http-delete-test-{int(time.time())}"

    @classmethod
    def tearDownClass(cls):                                                 # Clean up and stop server
        """Ensure all created files are deleted and stop the server"""
        for resource in cls.created_resources:
            try:
                cache_id  = resource.get('cache_id')
                namespace = resource.get('namespace', cls.test_namespace)

                if cache_id:
                    delete_url = f"{cls.base_url}/{namespace}/delete/{cache_id}"
                    response   = requests.delete(delete_url, headers=cls.headers)

                    if response.status_code == 200:
                        result = response.json()
                        # Don't warn about "not_found" - it might have been cleaned up already
                        if result.get('status') == 'not_found':
                            continue                                         # Already deleted, that's fine
                        elif result.get('status') != 'success':
                            print(f"Warning: Failed to delete {cache_id}: {result}")
                    else:
                        print(f"Warning: Delete request failed for {cache_id}: {response.status_code}")

            except Exception as e:
                print(f"Cleanup error for resource {resource}: {e}")

        cls.fast_api_server.stop()

    def setUp(self):                                                        # PER-TEST lightweight setup
        assert self.created_resources == []

    def tearDown(self):                                                     # PER-TEST cleanup
        assert self.created_resources == []

    def _store_string(self, data        : str                         ,     # Helper to store string and track for cleanup
                            strategy    : str  = "temporal"          ,
                            namespace   : str  = None
                      ) -> dict:
        namespace = namespace or self.test_namespace
        url       = f"{self.base_url}/{namespace}/{strategy}/store/string"

        response = requests.post(url     ,
                               json    = data                                ,
                               headers = self.headers                       )
        self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")

        result   = response.json()
        cache_id = result.get('cache_id')

        # Track for cleanup
        self.created_resources.append({'cache_id' : cache_id ,
                                      'namespace': namespace ,
                                      'type'     : 'string'  ,
                                      'strategy' : strategy  })

        return result

    def test_01_health_check(self):                                         # Test API is accessible
        """Verify the API is accessible and responding"""
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    def test_02_delete_basic_workflow(self):                                # Test basic delete workflow
        """Test basic workflow: store -> retrieve -> delete -> verify deleted"""
        test_data = f"HTTP delete test data at {time.time()}"

        # 1. Store data
        store_result = self._store_string(test_data, strategy="direct")
        cache_id     = store_result['cache_id']
        cache_hash   = store_result['hash']

        self.assertTrue(is_guid(cache_id))
        self.assertEqual(len(cache_hash), 16)

        # 2. Verify exists
        retrieve_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)

        self.assertEqual(retrieve_response.status_code, 200)
        retrieve_result = retrieve_response.json()
        self.assertEqual(retrieve_result['data'], test_data)

        # 3. Delete via Routes__Delete endpoint
        delete_url      = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
        delete_response = requests.delete(delete_url, headers=self.headers)

        self.assertEqual(delete_response.status_code, 200)
        delete_result = delete_response.json()
        self.assertEqual(delete_result['status'], 'success')
        self.assertEqual(delete_result['cache_id'], cache_id)
        self.assertGreater(delete_result['deleted_count'], 0)
        self.assertEqual(delete_result['failed_count'], 0)

        # 4. Verify deleted
        verify_response = requests.get(retrieve_url, headers=self.headers)
        self.assertEqual(verify_response.status_code, 200)
        verify_result = verify_response.json()
        self.assertEqual(verify_result, {"status": "not_found", "message": "Cache entry not found"})

        # Remove from tracking since we manually deleted
        self.created_resources[:] = [r for r in self.created_resources      # Mutate in place to avoid shadowing
                                    if r['cache_id'] != cache_id]

    def test_03_delete_non_existent(self):                                  # Test deleting non-existent entry
        """Test deleting non-existent cache ID returns not_found"""
        non_existent = "12345678-1234-1234-1234-123456789012"
        url          = f"{self.base_url}/{self.test_namespace}/delete/{non_existent}"
        response     = requests.delete(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result['status'], 'not_found')
        self.assertEqual(result['message'], f'Cache ID {non_existent} not found')

    def test_04_namespace_isolation(self):                                  # Test namespace isolation
        """Test that deletes are isolated by namespace"""
        ns1 = f"http-delete-ns1-{int(time.time())}"
        ns2 = f"http-delete-ns2-{int(time.time())}"

        try:
            # Store same data in different namespaces
            data    = "namespace isolation test for delete"
            result1 = self._store_string(data, namespace=ns1)
            result2 = self._store_string(data, namespace=ns2)

            cache_id1 = result1['cache_id']
            cache_id2 = result2['cache_id']

            # Try to delete ns2 item from ns1 namespace (should fail)
            wrong_delete_url = f"{self.base_url}/{ns1}/delete/{cache_id2}"
            wrong_response   = requests.delete(wrong_delete_url, headers=self.headers)

            self.assertEqual(wrong_response.status_code, 200)
            wrong_result = wrong_response.json()
            self.assertEqual(wrong_result['status'], 'not_found')

            # Verify ns2 item still exists
            check_url      = f"{self.base_url}/{ns2}/retrieve/{cache_id2}"
            check_response = requests.get(check_url, headers=self.headers)

            self.assertEqual(check_response.status_code, 200)
            self.assertEqual(check_response.json()['data'], data)

            # Delete from correct namespace
            correct_delete_url = f"{self.base_url}/{ns2}/delete/{cache_id2}"
            correct_response   = requests.delete(correct_delete_url, headers=self.headers)

            self.assertEqual(correct_response.status_code, 200)
            self.assertEqual(correct_response.json()['status'], 'success')

        finally:
            # Clean up both namespaces
            for cache_id, ns in [(cache_id1, ns1), (cache_id2, ns2)]:
                delete_url = f"{self.base_url}/{ns}/delete/{cache_id}"
                requests.delete(delete_url, headers=self.headers)

            # Remove from tracking
            self.created_resources[:] = [r for r in self.created_resources
                                        if r['cache_id'] not in [cache_id1, cache_id2]]

    def test_05_delete_all_strategies(self):                                # Test delete works with all strategies
        """Test that delete works for all storage strategies"""
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]
        stored_ids = []


        for strategy in strategies:
            data   = f"delete test for {strategy}"
            result = self._store_string(data, strategy=strategy)

            stored_ids.append(result['cache_id'])
            cache_id = result['cache_id']

            # Delete
            delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
            response   = requests.delete(delete_url, headers=self.headers)

            self.assertEqual(response.status_code, 200)
            delete_result = response.json()
            self.assertEqual(delete_result['status'], 'success')
            assert delete_result['deleted_count'] >  8         # All strategies create at least 9 files

            # Verify deleted
            check_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
            check_response = requests.get(check_url, headers=self.headers)

            self.assertEqual(check_response.json()['status'], 'not_found')

        # Clear from tracking since already deleted
        self.created_resources[:] = [r for r in self.created_resources
                                    if r['cache_id'] not in stored_ids]

        # except Exception as e:
        #     # Clean up on failure
        #     for cache_id in stored_ids:
        #         if cache_id not in [r['cache_id'] for r in self.created_resources]:
        #             continue
        #         delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
        #         requests.delete(delete_url, headers=self.headers)
        #     raise e

    def test_06_concurrent_deletes(self):                                   # Test concurrent delete operations
        """Test multiple concurrent delete operations"""

        # Create items to delete
        items = []
        for i in range(10):
            result = self._store_string(f"concurrent delete {i}")
            items.append(result['cache_id'])

        def delete_item(cache_id):
            """Delete a cache item"""
            url      = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
            response = requests.delete(url, headers=self.headers)

            if response.status_code == 200:
                result = response.json()
                return result['status'] == 'success'
            return False

        # Run concurrent deletes
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(delete_item, cache_id) for cache_id in items]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Verify all succeeded
        self.assertTrue(all(results))

        # Remove from tracking
        self.created_resources[:] = [r for r in self.created_resources
                                    if r['cache_id'] not in items]

    def test_07_delete_multiple_times(self):                                # Test deleting same ID multiple times
        """Test that deleting the same ID multiple times handles gracefully"""
        test_data = "multiple delete test"
        result    = self._store_string(test_data)
        cache_id  = result['cache_id']

        delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"

        # First delete should succeed
        response1 = requests.delete(delete_url, headers=self.headers)
        self.assertEqual(response1.status_code, 200)
        result1 = response1.json()
        self.assertEqual(result1['status'], 'success')

        # Second delete should not find it
        response2 = requests.delete(delete_url, headers=self.headers)
        self.assertEqual(response2.status_code, 200)
        result2 = response2.json()
        self.assertEqual(result2['status'], 'not_found')

        # Third delete should also not find it
        response3 = requests.delete(delete_url, headers=self.headers)
        self.assertEqual(response3.status_code, 200)
        result3 = response3.json()
        self.assertEqual(result3['status'], 'not_found')

        # Remove from tracking
        self.created_resources[:] = [r for r in self.created_resources
                                    if r['cache_id'] != cache_id]

    def test_08_performance_batch_deletes(self):                            # Test performance with batch deletes
        """Test performance when deleting many items"""
        # Create multiple items
        num_items = 5
        items     = []

        try:
            for i in range(num_items):
                result = self._store_string(f"perf test {i}", strategy="direct")
                items.append(result['cache_id'])

            # Delete all rapidly
            start_time = time.time()

            for cache_id in items:
                url      = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
                response = requests.delete(url, headers=self.headers)

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['status'], 'success')

            elapsed = time.time() - start_time

            # Should be reasonably fast
            self.assertLess(elapsed, 1.0, f"Batch deletes too slow: {elapsed:.2f}s for {num_items} items")

            # Clear from tracking
            self.created_resources[:] = [r for r in self.created_resources
                                        if r['cache_id'] not in items]

        except Exception as e:
            # Clean up on failure
            for cache_id in items:
                if cache_id in [r['cache_id'] for r in self.created_resources]:
                    delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
                    requests.delete(delete_url, headers=self.headers)
            raise e

    def test_10_delete_binary_data(self):                                   # Test deleting binary data
        """Test delete works with binary data"""
        namespace   = f"delete-binary-{int(time.time())}"
        binary_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100                  # Fake PNG header

        # Store binary
        store_url  = f"{self.base_url}/{namespace}/direct/store/binary"
        store_resp = requests.post(store_url  ,
                                  data    = binary_data                      ,
                                  headers = {**self.headers, "Content-Type": "application/octet-stream"})

        self.assertEqual(store_resp.status_code, 200)
        cache_id = store_resp.json()['cache_id']

        # Track for cleanup
        self.created_resources.append({'cache_id': cache_id, 'namespace': namespace})

        # Delete binary
        delete_url  = f"{self.base_url}/{namespace}/delete/{cache_id}"
        delete_resp = requests.delete(delete_url, headers=self.headers)

        self.assertEqual(delete_resp.status_code, 200)
        delete_result = delete_resp.json()
        self.assertEqual(delete_result['status'], 'success')
        self.assertEqual(delete_result['deleted_count'], 9)

        # Remove from tracking
        self.created_resources[:] = [r for r in self.created_resources
                                    if r['cache_id'] != cache_id]

    def test_11_delete_json_data(self):                                     # Test deleting JSON data
        """Test delete works with JSON data"""
        namespace = f"delete-json-{int(time.time())}"
        json_data = {"test": "delete", "nested": {"key": "value"}, "list": [1, 2, 3]}

        # Store JSON
        store_url  = f"{self.base_url}/{namespace}/temporal_versioned/store/json"
        store_resp = requests.post(store_url  ,
                                  json    = json_data                        ,
                                  headers = self.headers                    )

        self.assertEqual(store_resp.status_code, 200)
        cache_id = store_resp.json()['cache_id']

        # Track for cleanup
        self.created_resources.append({'cache_id': cache_id, 'namespace': namespace})

        # Delete JSON
        delete_url  = f"{self.base_url}/{namespace}/delete/{cache_id}"
        delete_resp = requests.delete(delete_url, headers=self.headers)

        self.assertEqual(delete_resp.status_code, 200)
        delete_result = delete_resp.json()
        self.assertEqual(delete_result['status'], 'success')

        # Remove from tracking
        self.created_resources[:] = [r for r in self.created_resources
                                    if r['cache_id'] != cache_id]

    def test_12_integration_with_all_routes(self):                          # Test integration with other routes
        """Test delete endpoint integrates with store, retrieve, and exists endpoints"""
        namespace = f"integration-{int(time.time())}"
        test_data = "integration test data"

        # Store via Routes__Store
        store_url  = f"{self.base_url}/{namespace}/temporal/store/string"
        store_resp = requests.post(store_url  ,
                                  json    = test_data                        ,
                                  headers = self.headers                    )

        self.assertEqual(store_resp.status_code, 200)
        store_result = store_resp.json()
        cache_id     = store_result['cache_id']
        cache_hash   = store_result['hash']

        # Track for cleanup
        self.created_resources.append({'cache_id': cache_id, 'namespace': namespace})

        # Check exists via Routes__Exists
        exists_url  = f"{self.base_url}/{namespace}/exists/hash/{cache_hash}"
        exists_resp = requests.get(exists_url, headers=self.headers)

        self.assertEqual(exists_resp.status_code, 200)
        self.assertTrue(exists_resp.json()['exists'])

        # Retrieve via Routes__Retrieve
        retrieve_url  = f"{self.base_url}/{namespace}/retrieve/{cache_id}"
        retrieve_resp = requests.get(retrieve_url, headers=self.headers)

        self.assertEqual(retrieve_resp.status_code, 200)
        self.assertEqual(retrieve_resp.json()['data'], test_data)

        # Delete via Routes__Delete
        delete_url  = f"{self.base_url}/{namespace}/delete/{cache_id}"
        delete_resp = requests.delete(delete_url, headers=self.headers)

        self.assertEqual(delete_resp.status_code, 200)
        self.assertEqual(delete_resp.json()['status'], 'success')

        # Verify gone via all routes
        exists_after = requests.get(exists_url, headers=self.headers)
        self.assertFalse(exists_after.json()['exists'])

        retrieve_after = requests.get(retrieve_url, headers=self.headers)
        self.assertEqual(retrieve_after.json()['status'], 'not_found')

        # Remove from tracking
        self.created_resources[:] = [r for r in self.created_resources
                                    if r['cache_id'] != cache_id]

    def test_99_final_cleanup_verification(self):                           # Verify cleanup worked
        """Verify all test data has been properly cleaned up"""
        # Verify no tracked resources remain
        self.assertEqual(len(self.created_resources), 0,
                       f"Undeleted resources remain: {self.created_resources}")