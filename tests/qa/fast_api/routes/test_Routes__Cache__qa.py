# import pytest
# import requests
# import time
# from unittest                                   import TestCase
# from typing                                     import Dict, Any
# from osbot_fast_api.schemas.consts__Fast_API    import ENV_VAR__FAST_API__AUTH__API_KEY__NAME, ENV_VAR__FAST_API__AUTH__API_KEY__VALUE
# from osbot_utils.utils.Env                      import get_env, load_dotenv
# from osbot_utils.utils.Misc                     import is_guid
#
# ENV_VAR__MGRAPH__SERVICE__CACHE__TEST_SERVER = 'MGRAPH__SERVICE__CACHE__TEST_SERVER'
#
# class test_Routes__Cache__qa(TestCase):                                             # Live QA tests against cache.dev.mgraph.ai
#
#     @classmethod
#     def setUpClass(cls):
#         pytest.skip("Needs manual trigger")
#         load_dotenv()
#         cls.base_url  = get_env(ENV_VAR__MGRAPH__SERVICE__CACHE__TEST_SERVER)
#         if not cls.base_url:
#             pytest.skip("No base url set")
#         cls.key_name  = get_env(ENV_VAR__FAST_API__AUTH__API_KEY__NAME )
#         cls.key_value = get_env(ENV_VAR__FAST_API__AUTH__API_KEY__VALUE)
#         cls.auth_key  = {cls.key_name: cls.key_value }
#         if not cls.key_name or not cls.key_value:
#             pytest.skip("No Auth key name or key value provided")
#         cls.headers  = { "Content-Type": "application/json",
#                          **cls.auth_key                    }
#         cls.created_resources = []                                      # Track all created resources for cleanup
#         #cls.test_namespace = f"qa-test-{int(time.time())}"              # Test namespace with timestamp to avoid conflicts
#         cls.test_namespace = f'qa-test'                                  # todo: fix bug that we are not removing the namespace
#
#     @classmethod
#     def tearDownClass(cls):                                                         # Clean up all created resources
#         """Ensure all created files are deleted from the live server"""
#         for resource in cls.created_resources:
#             try:
#                 cache_id = resource.get('cache_id')
#                 namespace = resource.get('namespace', cls.test_namespace)
#
#                 if cache_id:
#                     delete_url = f"{cls.base_url}/cache/delete/by-id/{cache_id}/{namespace}"
#                     response = requests.delete(delete_url, headers=cls.headers)
#
#                     if response.status_code == 200:
#                         result = response.json()
#                         if result.get('status') != 'success':
#                             print(f"Warning: Failed to delete {cache_id}: {result}")
#                     else:
#                         print(f"Warning: Delete request failed for {cache_id}: {response.status_code}")
#
#             except Exception as e:
#                 print(f"Cleanup error for resource {resource}: {e}")
#
#     def _store_string(self, data: str, strategy: str = "temporal", namespace: str = None) -> Dict[str, Any]:
#         """Helper to store string and track for cleanup"""
#         namespace = namespace or self.test_namespace
#         url = f"{self.base_url}/cache/store/string/{strategy}/{namespace}"
#
#         response = requests.post(url, data=data, headers={"Content-Type": "text/plain", **self.auth_key})
#         self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")
#
#         result = response.json()
#         cache_id = result.get('cache_id')
#
#         # Track for cleanup
#         self.created_resources.append({
#             'cache_id': cache_id,
#             'namespace': namespace,
#             'type': 'string',
#             'strategy': strategy
#         })
#
#         return result
#
#     def _store_json(self, data: dict, strategy: str = "temporal", namespace: str = None) -> Dict[str, Any]:
#         """Helper to store JSON and track for cleanup"""
#         namespace = namespace or self.test_namespace
#         url = f"{self.base_url}/cache/store/json/{strategy}/{namespace}"
#
#         response = requests.post(url, json=data, headers=self.headers)
#         self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")
#
#         result = response.json()
#         cache_id = result.get('cache_id')
#
#         # Track for cleanup
#         self.created_resources.append({
#             'cache_id': cache_id,
#             'namespace': namespace,
#             'type': 'json',
#             'strategy': strategy
#         })
#
#         return result
#
#     def test_01_health_check(self):                                                 # Test API is accessible
#         """Verify the API is accessible and responding"""
#         # Try to list namespaces as a health check
#         url = f"{self.base_url}/cache/namespaces"
#         response = requests.get(url, headers=self.headers)
#
#         self.assertEqual(response.status_code, 200)
#         result = response.json()
#         self.assertIn('namespaces', result)
#         self.assertIn('count', result)
#         self.assertIsInstance(result['namespaces'], list)
#
#     def test_02_store_retrieve_delete_workflow(self):                               # Complete lifecycle test
#         """Test complete lifecycle: store -> retrieve -> verify -> delete"""
#         test_data = f"QA test data at {time.time()}"
#
#         # 1. Store data
#         store_result = self._store_string(test_data, strategy="direct")
#         cache_id     = store_result['cache_id']
#         cache_hash   = store_result['cache_hash']
#
#         self.assertTrue(is_guid(cache_id))
#         self.assertEqual(len(cache_hash), 16)
#         self.assertIn('paths', store_result)
#
#         # 2. Retrieve by ID
#         retrieve_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#         retrieve_response = requests.get(retrieve_url, headers=self.headers)
#
#         self.assertEqual(retrieve_response.status_code, 200)
#         retrieve_result = retrieve_response.json()
#         self.assertEqual(retrieve_result['data'], test_data)
#
#         # 3. Check exists
#         exists_url = f"{self.base_url}/cache/exists/{cache_hash}/{self.test_namespace}"
#         exists_response = requests.get(exists_url, headers=self.headers)
#
#         self.assertEqual(exists_response.status_code, 200)
#         exists_result = exists_response.json()
#         self.assertTrue(exists_result['exists'])
#
#         # 4. Delete
#         delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#         delete_response = requests.delete(delete_url, headers=self.headers)
#
#         self.assertEqual(delete_response.status_code, 200)
#         delete_result = delete_response.json()
#         self.assertEqual(delete_result['status'], 'success')
#         self.assertGreater(delete_result['deleted_count'], 0)
#
#         # 5. Verify deleted
#         verify_response = requests.get(retrieve_url, headers=self.headers)
#         self.assertEqual(verify_response.status_code, 200)
#         verify_result = verify_response.json()
#         self.assertEqual(verify_result['status'], 'not_found')
#
#         # Remove from tracking since we manually deleted
#         self.created_resources [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_03_multiple_strategies(self):                                          # Test all storage strategies
#         """Test different storage strategies and clean up properly"""
#         strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]
#         stored_items = []
#
#         try:
#             # Store with each strategy
#             for strategy in strategies:
#                 data = f"Strategy test: {strategy}"
#                 result = self._store_string(data, strategy=strategy)
#
#                 stored_items.append({
#                     'cache_id': result['cache_id'],
#                     'strategy': strategy,
#                     'data': data
#                 })
#
#                 # Verify paths structure differs by strategy
#                 self.assertIn('paths', result)
#                 self.assertIn('data', result['paths'])
#
#             # Verify all can be retrieved
#             for item in stored_items:
#                 url = f"{self.base_url}/cache/retrieve/by-id/{item['cache_id']}/{self.test_namespace}"
#                 response = requests.get(url, headers=self.headers)
#
#                 self.assertEqual(response.status_code, 200)
#                 result = response.json()
#                 self.assertEqual(result['data'], item['data'])
#
#         finally:
#             # Clean up all items
#             for item in stored_items:
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{item['cache_id']}/{self.test_namespace}"
#                 delete_response = requests.delete(delete_url, headers=self.headers)
#                 self.assertEqual(delete_response.status_code, 200)
#
#                 # Remove from tracking
#                 self.created_resources [:] = [r for r in self.created_resources
#                                               if r['cache_id'] != item['cache_id']]
#
#     # todo: add this back when we add back the method to calculate the hash
#     # def test_04_json_storage_and_hash_calculation(self):                           # Test JSON operations
#     #     """Test JSON storage, hash calculation, and cleanup"""
#     #     test_json = {
#     #         "test": "qa_data",
#     #         "timestamp": time.time(),
#     #         "nested": {"level": 2, "data": "nested_value"}
#     #     }
#     #
#     #     # 1. Calculate hash first
#     #     hash_url = f"{self.base_url}/cache/hash/calculate"
#     #     hash_response = requests.post(hash_url, json={"json_data": test_json}, headers=self.headers)
#     #
#     #     self.assertEqual(hash_response.status_code, 200)
#     #     calculated_hash = hash_response.json()['cache_hash']
#     #
#     #     # 2. Store JSON
#     #     store_result = self._store_json(test_json)
#     #     cache_id = store_result['cache_id']
#     #     stored_hash = store_result['cache_hash']
#     #
#     #     # Hashes should match
#     #     self.assertEqual(stored_hash, calculated_hash)
#     #
#     #     # 3. Retrieve and verify
#     #     retrieve_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#     #     retrieve_response = requests.get(retrieve_url, headers=self.headers)
#     #
#     #     self.assertEqual(retrieve_response.status_code, 200)
#     #     retrieve_result = retrieve_response.json()
#     #     self.assertEqual(retrieve_result['data'], test_json)
#     #
#     #     # 4. Clean up
#     #     delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#     #     delete_response = requests.delete(delete_url, headers=self.headers)
#     #
#     #     self.assertEqual(delete_response.status_code, 200)
#     #     self.assertEqual(delete_response.json()['status'], 'success')
#     #
#     #     # Remove from tracking
#     #     self.created_resources [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     @pytest.mark.skip(reason="this test also created new namespaces, which we are not deleting at the moment")
#     def test_05_namespace_isolation(self):                                          # Test namespace isolation
#         """Test that namespaces properly isolate data"""
#         ns1 = f"qa-ns1-{int(time.time())}"
#         ns2 = f"qa-ns2-{int(time.time())}"
#
#         try:
#             # Store same data in different namespaces
#             data = "namespace isolation test"
#
#             result1 = self._store_string(data, namespace=ns1)
#             result2 = self._store_string(data, namespace=ns2)
#
#             cache_id1 = result1['cache_id']
#             cache_id2 = result2['cache_id']
#
#             # Cache IDs should be different (unique per store)
#             self.assertNotEqual(cache_id1, cache_id2)
#
#             # But hashes should be the same (same data)
#             self.assertEqual(result1['cache_hash'], result2['cache_hash'])
#
#             # Cross-namespace retrieval should fail
#             cross_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id1}/{ns2}"
#             cross_response = requests.get(cross_url, headers=self.headers)
#
#             self.assertEqual(cross_response.status_code, 200)
#             cross_result = cross_response.json()
#             self.assertEqual(cross_result['status'], 'not_found')
#
#             # Check namespaces list includes both
#             list_url = f"{self.base_url}/cache/namespaces"
#             list_response = requests.get(list_url, headers=self.headers)
#
#             self.assertEqual(list_response.status_code, 200)
#             namespaces = list_response.json()['namespaces']
#             self.assertIn(ns1, namespaces)
#             self.assertIn(ns2, namespaces)
#
#         finally:
#             # Clean up both namespaces
#             for cache_id, ns in [(cache_id1, ns1), (cache_id2, ns2)]:
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{ns}"
#                 requests.delete(delete_url, headers=self.headers)
#
#             # Remove from tracking
#             self.created_resources [:] = [r for r in self.created_resources
#                                           if r['cache_id'] not in [cache_id1, cache_id2]]
#
#     def test_06_stats_endpoint(self):                                               # Test statistics tracking
#         """Test stats endpoint tracks entries correctly"""
#         # Get initial stats
#         stats_url = f"{self.base_url}/cache/stats/namespaces/{self.test_namespace}"
#         initial_response = requests.get(stats_url, headers=self.headers)
#
#         if initial_response.status_code == 200:
#             initial_stats = initial_response.json()
#             initial_temporal = initial_stats.get('temporal_files', 0)
#             initial_direct = initial_stats.get('direct_files', 0)
#         else:
#             initial_temporal = 0
#             initial_direct = 0
#
#         # Store some data
#         items_to_create = 3
#         created_ids = []
#
#         try:
#             for i in range(items_to_create):
#                 result = self._store_string(f"stats test {i}", strategy="temporal")
#                 created_ids.append(result['cache_id'])
#
#             # Check updated stats
#             final_response = requests.get(stats_url, headers=self.headers)
#             self.assertEqual(final_response.status_code, 200)
#
#             final_stats = final_response.json()
#             self.assertEqual(final_stats['namespace'], self.test_namespace)
#
#             # Temporal files should have increased
#             # Each store creates multiple files (data + refs)
#             final_temporal = final_stats.get('temporal_files', 0)
#             self.assertGreater(final_temporal, initial_temporal)
#
#         finally:
#             # Clean up
#             for cache_id in created_ids:
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#
#             # Remove from tracking
#             self.created_resources [:] = [r for r in self.created_resources
#                                           if r['cache_id'] not in created_ids]
#
#     def test_07_error_handling(self):                                               # Test error conditions
#         """Test API error handling without creating persistent data"""
#
#         # 1. Retrieve non-existent ID
#         fake_id = "00000000-0000-0000-0000-000000000000"
#         url = f"{self.base_url}/cache/retrieve/by-id/{fake_id}/{self.test_namespace}"
#         response = requests.get(url, headers=self.headers)
#
#         self.assertEqual(response.status_code, 200)
#         result = response.json()
#         self.assertEqual(result['status'], 'not_found')
#
#         # 2. Check non-existent hash
#         fake_hash = "0000000000000000"
#         exists_url = f"{self.base_url}/cache/exists/{fake_hash}/{self.test_namespace}"
#         exists_response = requests.get(exists_url, headers=self.headers)
#
#         self.assertEqual(exists_response.status_code, 200)
#         exists_result = exists_response.json()
#         self.assertFalse(exists_result['exists'])
#
#         # 3. Delete non-existent ID
#         delete_url = f"{self.base_url}/cache/delete/by-id/{fake_id}/{self.test_namespace}"
#         delete_response = requests.delete(delete_url, headers=self.headers)
#
#         self.assertEqual(delete_response.status_code, 200)
#         delete_result = delete_response.json()
#         self.assertEqual(delete_result['status'], 'not_found')
#
#     def test_08_concurrent_operations(self):                                        # Test concurrent access
#         """Test multiple operations can work concurrently"""
#         import concurrent.futures
#
#         def store_and_retrieve(index):
#             """Store data and immediately retrieve it"""
#             data = f"concurrent test {index}"
#
#             # Store
#             store_result = self._store_string(data, strategy="direct")
#             cache_id = store_result['cache_id']
#
#             # Retrieve
#             retrieve_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#             retrieve_response = requests.get(retrieve_url, headers=self.headers)
#
#             if retrieve_response.status_code == 200:
#                 retrieved_data = retrieve_response.json()['data']
#                 return cache_id, data == retrieved_data
#
#             return cache_id, False
#
#         # Run concurrent operations
#         with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
#             futures = [executor.submit(store_and_retrieve, i) for i in range(10)]
#             results = [future.result() for future in concurrent.futures.as_completed(futures)]
#
#         # Verify all succeeded
#         cache_ids = []
#         for cache_id, success in results:
#             cache_ids.append(cache_id)
#             self.assertTrue(success, f"Concurrent operation failed for {cache_id}")
#
#         # Clean up all
#         for cache_id in cache_ids:
#             delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#             requests.delete(delete_url, headers=self.headers)
#
#         # Remove from tracking
#         self.created_resources [:] = [r for r in self.created_resources
#                                       if r['cache_id'] not in cache_ids]
#
#     def test_99_final_cleanup_verification(self):                                   # Verify cleanup worked
#         """Verify all test data has been properly cleaned up"""
#         # Check our test namespace is empty or nearly empty
#         stats_url = f"{self.base_url}/cache/stats/{self.test_namespace}"
#         response = requests.get(stats_url, headers=self.headers)
#
#         if response.status_code == 200:
#             stats = response.json()
#
#             # Should have minimal files left
#             total_files = (stats.get('direct_files', 0) +
#                           stats.get('temporal_files', 0) +
#                           stats.get('temporal_latest_files', 0) +
#                           stats.get('temporal_versioned_files', 0))
#
#             # Allow for some timing issues but should be mostly clean
#             self.assertLess(total_files, 10,
#                           f"Too many files left after cleanup: {total_files}")
#
#         # Verify no tracked resources remain
#         self.assertEqual(len(self.created_resources), 0,
#                         f"Undeleted resources remain: {self.created_resources}")