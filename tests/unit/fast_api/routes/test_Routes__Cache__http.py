# import base64
# import gzip
# import json
# import pytest
# import requests
# import time
# import concurrent.futures
# from unittest                                  import TestCase
# from typing                                    import Dict, Any
# from osbot_fast_api.utils.Fast_API_Server      import Fast_API_Server
# from osbot_utils.utils.Env                     import  in_github_action
# from osbot_utils.utils.Misc                    import is_guid
# from tests.unit.Service__Cache__Test_Objs      import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE
#
# class test_Routes__Cache__http(TestCase):                                           # Local HTTP tests using temp FastAPI server
#
#     @classmethod
#     def setUpClass(cls):
#         #skip__if_not__in_github_actions()
#         if in_github_action():
#             pytest.skip("Skipping this test on GitHub Actions (because we are getting 404 on the routes below)")
#         #load_dotenv()
#         cls.key_name      = TEST_API_KEY__NAME #get_env(ENV_VAR__FAST_API__AUTH__API_KEY__NAME )
#         cls.key_value     = TEST_API_KEY__VALUE #get_env(ENV_VAR__FAST_API__AUTH__API_KEY__VALUE)
#         cls.auth_headers  = {cls.key_name: cls.key_value }
#         if not cls.key_name or not cls.key_value:
#             pytest.skip("No Auth key name or key value provided")
#         cls.service__fast_api = setup__service_fast_api_test_objs().fast_api
#         cls.service__app      = cls.service__fast_api.app()
#         cls.fast_api_server   = Fast_API_Server(app=cls.service__app)
#         cls.fast_api_server.start()
#
#         cls.base_url = cls.fast_api_server.url().rstrip("/")
#         cls.headers = cls.auth_headers
#
#         # Track all created resources for cleanup
#         cls.created_resources = []
#
#         # Test namespace with timestamp to avoid conflicts
#         cls.test_namespace = f"http-test-{int(time.time())}"
#
#     @classmethod
#     def tearDownClass(cls):                                                         # Clean up and stop server
#         """Ensure all created files are deleted and stop the server"""
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
#                         # Don't warn about "not_found" - it might have been cleaned up already
#                         if result.get('status') == 'not_found':
#                             continue  # Already deleted, that's fine
#                         elif result.get('status') != 'success':
#                             print(f"Warning: Failed to delete {cache_id}: {result}")
#                     else:
#                         print(f"Warning: Delete request failed for {cache_id}: {response.status_code}")
#
#             except Exception as e:
#                 print(f"Cleanup error for resource {resource}: {e}")
#
#         cls.fast_api_server.stop()
#
#     def setUp(self):
#         assert self.created_resources == []
#
#     def tearDown(self):
#         assert self.created_resources == []
#
#     def _store_string(self, data: str, strategy: str = "temporal", namespace: str = None) -> Dict[str, Any]:
#         """Helper to store string and track for cleanup"""
#         namespace = namespace or self.test_namespace
#         url = f"{self.base_url}/cache/store/string/{strategy}/{namespace}"
#
#         response = requests.post(url, data=data, headers={**self.headers, "Content-Type": "text/plain"})
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
#     def _store_binary(self, data: bytes, strategy: str = "temporal", namespace: str = None,
#                       content_encoding: str = None) -> Dict[str, Any]:
#         """Helper to store binary and track for cleanup"""
#         namespace = namespace or self.test_namespace
#         url = f"{self.base_url}/cache/store/binary/{strategy}/{namespace}"
#
#         headers = {**self.headers, "Content-Type": "application/octet-stream"}
#         if content_encoding:
#             headers["Content-Encoding"] = content_encoding
#
#         response = requests.post(url, data=data, headers=headers)
#         self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")
#
#         result = response.json()
#         cache_id = result.get('cache_id')
#
#         # Track for cleanup
#         self.created_resources.append({
#             'cache_id': cache_id,
#             'namespace': namespace,
#             'type': 'binary',
#             'strategy': strategy
#         })
#
#         return result
#
#     def test_01_health_check(self):                                                 # Test API is accessible
#         """Verify the API is accessible and responding"""
#         # Try health endpoint first
#         response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
#         self.assertEqual(response.status_code, 200)
#         self.assertEqual(response.json(), {'status': 'ok'})
#
#         # Try to list namespaces as additional check
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
#         test_data = f"HTTP test data at {time.time()}"
#
#         # 1. Store data
#         store_result = self._store_string(test_data, strategy="direct")
#         cache_id = store_result['cache_id']
#         cache_hash = store_result['cache_hash']
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
#         self.created_resources [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]               # mutate in place to avoid shadowing class attribute
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
#             # Remove ALL stored items from tracking in one go AFTER all deletes are done
#             stored_cache_ids = [item['cache_id'] for item in stored_items]
#             self.created_resources [:] = [r for r in self.created_resources
#                                           if r['cache_id'] not in stored_cache_ids]
#
#     # def test_04_json_storage_and_hash_calculation(self):                           # Test JSON operations
#     #     """Test JSON storage, hash calculation, and cleanup"""
#     #     test_json = {
#     #         "test": "http_data",
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
#     #     self.created_resources = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_05_namespace_isolation(self):                                          # Test namespace isolation
#         """Test that namespaces properly isolate data"""
#         ns1 = f"http-ns1-{int(time.time())}"
#         ns2 = f"http-ns2-{int(time.time())}"
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
#                 delete_url      = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 response_delete = requests.delete(delete_url, headers=self.headers)
#
#             # Remove from tracking
#             self.created_resources [:] = [r for r in self.created_resources
#                                           if r['cache_id'] not in created_ids]
#
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
#                                  if r['cache_id'] not in cache_ids]
#
#
#     def test_09_binary_data_redirect_pattern(self):                                     # Test binary redirect pattern
#         """Test that binary data redirects to binary endpoint in JSON responses"""
#         test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50  # Fake PNG header
#
#         try:
#             # Store binary data
#             store_result = self._store_binary(test_binary, strategy="direct")
#             cache_id = store_result['cache_id']
#             cache_hash = store_result['cache_hash']
#
#             # Try generic endpoint - should redirect
#             generic_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#             generic_response = requests.get(generic_url, headers=self.headers)
#
#             self.assertEqual(generic_response.status_code, 200)
#             generic_result = generic_response.json()
#
#             # Should get redirect message
#             self.assertEqual(generic_result['status'], 'binary_data')
#             self.assertEqual(generic_result['message'], 'Binary data cannot be returned in JSON response')
#             self.assertEqual(generic_result['data_type'], 'binary')
#             self.assertEqual(generic_result['size'], len(test_binary))
#             self.assertIn('binary_url', generic_result)
#             self.assertNotIn('data', generic_result)  # No actual data
#
#             # Binary endpoint should work
#             binary_url = f"{self.base_url}/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}"
#             binary_response = requests.get(binary_url, headers=self.headers)
#
#             self.assertEqual(binary_response.status_code, 200)
#             self.assertEqual(binary_response.content, test_binary)
#             self.assertEqual(binary_response.headers['content-type'], 'application/octet-stream')
#
#         finally:
#             # Clean up
#             if 'cache_id' in locals():
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#                 self.created_resources [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_10_compressed_binary_handling(self):                                       # Test compressed binary
#         """Test storing and retrieving gzip compressed binary data"""
#         original_data = b"Test data that will be compressed" * 100
#         compressed_data = gzip.compress(original_data)
#
#         try:
#             # Store compressed binary with content-encoding
#             store_result = self._store_binary(compressed_data, strategy="temporal",
#                                              content_encoding="gzip")
#             cache_id = store_result['cache_id']
#
#             # Size should be compressed size
#             self.assertLess(store_result['size'], len(original_data))
#
#             # Binary endpoint should return decompressed data
#             binary_url = f"{self.base_url}/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}"
#             binary_response = requests.get(binary_url, headers=self.headers)
#
#             self.assertEqual(binary_response.status_code, 200)
#             self.assertEqual(binary_response.content, original_data)  # Decompressed!
#             # Should NOT have Content-Encoding header (already decompressed)
#             self.assertNotIn('content-encoding', binary_response.headers)
#
#             # Generic endpoint should redirect
#             generic_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#             generic_response = requests.get(generic_url, headers=self.headers)
#
#             self.assertEqual(generic_response.status_code, 200)
#             result = generic_response.json()
#             self.assertEqual(result['status'], 'binary_data')
#             self.assertEqual(result['size'], len(original_data))  # Size of decompressed
#
#         finally:
#             if 'cache_id' in locals():
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#                 self.created_resources  [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_11_compressed_json_detection(self):                                        # Test compressed JSON detection
#         """Test that compressed JSON is detected as JSON after decompression"""
#         json_data = {"users": [{"id": i, "name": f"User_{i}", "data": "x" * 50} for i in range(20)]}
#         json_string = json.dumps(json_data)
#         compressed_data = gzip.compress(json_string.encode())
#
#         try:
#             # Store as compressed binary
#             store_result = self._store_binary(compressed_data, strategy="temporal_latest",
#                                              content_encoding="gzip")
#             cache_id = store_result['cache_id']
#
#             # Generic endpoint should recognize it as JSON after decompression
#             generic_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#             generic_response = requests.get(generic_url, headers=self.headers)
#
#             self.assertEqual(generic_response.status_code, 200)
#             result = generic_response.json()
#
#             # Should be detected as JSON and returned directly
#             self.assertEqual(result['data_type'], 'json')
#             self.assertEqual(result['data'], json_data)  # Decompressed and parsed
#             self.assertEqual(result['content_encoding'], 'gzip')
#
#             # JSON endpoint should work
#             json_url = f"{self.base_url}/cache/retrieve/json/by-id/{cache_id}/{self.test_namespace}"
#             json_response = requests.get(json_url, headers=self.headers)
#
#             self.assertEqual(json_response.status_code, 200)
#             self.assertEqual(json_response.json(), json_data)
#
#             # Binary endpoint should return as JSON bytes
#             binary_url = f"{self.base_url}/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}"
#             binary_response = requests.get(binary_url, headers=self.headers)
#
#             self.assertEqual(binary_response.status_code, 200)
#             self.assertEqual(binary_response.content, json_string.encode())
#
#         finally:
#             if 'cache_id' in locals():
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#                 self.created_resources  [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_12_type_specific_endpoints(self):                                          # Test type-specific endpoints
#         """Test all type-specific retrieval endpoints"""
#         # Test data
#         string_data = "Test string data"
#         json_data = {"test": "data", "number": 42, "array": [1, 2, 3]}
#         binary_data = b'\x00\x01\x02\x03\x04\x05'
#
#         stored_items = []
#
#         try:
#             # Store each type
#             string_result = self._store_string(string_data, strategy="direct")
#             stored_items.append(('string', string_result['cache_id'], string_data))
#
#             json_result = self._store_json(json_data, strategy="direct")
#             stored_items.append(('json', json_result['cache_id'], json_data))
#
#             binary_result = self._store_binary(binary_data, strategy="direct")
#             stored_items.append(('binary', binary_result['cache_id'], binary_data))
#
#             # Test each type-specific endpoint
#             for data_type, cache_id, expected_data in stored_items:
#
#                 # String endpoint
#                 string_url = f"{self.base_url}/cache/retrieve/string/by-id/{cache_id}/{self.test_namespace}"
#                 string_response = requests.get(string_url, headers=self.headers)
#
#                 if data_type == 'string':
#                     self.assertEqual(string_response.status_code, 200)
#                     self.assertEqual(string_response.text, expected_data)
#                 elif data_type == 'json':
#                     self.assertEqual(string_response.status_code, 200)
#                     self.assertEqual(string_response.text, json.dumps(expected_data))
#                 elif data_type == 'binary':
#                     self.assertEqual(string_response.status_code, 200)
#                     # Binary should be base64 encoded when retrieved as string
#                     try:
#                         # Try to decode as UTF-8 first
#                         decoded = expected_data.decode('utf-8')
#                         self.assertEqual(string_response.text, decoded)
#                     except:
#                         # Falls back to base64
#                         expected_b64 = base64.b64encode(expected_data).decode('utf-8')
#                         self.assertEqual(string_response.text, expected_b64)
#
#                 # JSON endpoint
#                 json_url = f"{self.base_url}/cache/retrieve/json/by-id/{cache_id}/{self.test_namespace}"
#                 json_response = requests.get(json_url, headers=self.headers)
#
#                 if data_type == 'json':
#                     self.assertEqual(json_response.status_code, 200)
#                     self.assertEqual(json_response.json(), expected_data)
#                 elif data_type == 'string':
#                     # String that's not valid JSON
#                     result = json_response.json()
#                     self.assertIn('error', result)
#                     self.assertEqual(result['data'], expected_data)
#                 elif data_type == 'binary':
#                     # Binary returned as base64 in JSON wrapper
#                     result = json_response.json()
#                     self.assertEqual(result['data_type'], 'binary')
#                     self.assertEqual(result['encoding'], 'base64')
#                     self.assertEqual(base64.b64decode(result['data']), expected_data)
#
#                 # Binary endpoint
#                 binary_url = f"{self.base_url}/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}"
#                 binary_response = requests.get(binary_url, headers=self.headers)
#
#                 self.assertEqual(binary_response.status_code, 200)
#                 if data_type == 'string':
#                     self.assertEqual(binary_response.content, expected_data.encode())
#                 elif data_type == 'json':
#                     self.assertEqual(binary_response.content, json.dumps(expected_data).encode())
#                 elif data_type == 'binary':
#                     self.assertEqual(binary_response.content, expected_data)
#
#         finally:
#             # Clean up all
#             for _, cache_id, _ in stored_items:
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#
#             cache_ids = [item[1] for item in stored_items]
#             self.created_resources  [:] = [r for r in self.created_resources if r['cache_id'] not in cache_ids]
#
#     def test_13_hash_retrieval_endpoints(self):                                         # Test hash-based retrieval
#         """Test retrieval by hash for all types"""
#         test_data = {"key": "value", "test": True}
#
#         try:
#             # Store JSON data
#             store_result = self._store_json(test_data, strategy="temporal_versioned")
#             cache_id = store_result['cache_id']
#             cache_hash = store_result['cache_hash']
#
#             # Generic hash endpoint
#             generic_url = f"{self.base_url}/cache/retrieve/by-hash/{cache_hash}/{self.test_namespace}"
#             generic_response = requests.get(generic_url, headers=self.headers)
#
#             self.assertEqual(generic_response.status_code, 200)
#             result = generic_response.json()
#             self.assertEqual(result['data'], test_data)
#             self.assertEqual(result['data_type'], 'json')
#
#             # Type-specific hash endpoints
#             json_url = f"{self.base_url}/cache/retrieve/json/by-hash/{cache_hash}/{self.test_namespace}"
#             json_response = requests.get(json_url, headers=self.headers)
#             self.assertEqual(json_response.json(), test_data)
#
#             string_url = f"{self.base_url}/cache/retrieve/string/by-hash/{cache_hash}/{self.test_namespace}"
#             string_response = requests.get(string_url, headers=self.headers)
#             self.assertEqual(string_response.text, json.dumps(test_data))
#
#             binary_url = f"{self.base_url}/cache/retrieve/binary/by-hash/{cache_hash}/{self.test_namespace}"
#             binary_response = requests.get(binary_url, headers=self.headers)
#             self.assertEqual(binary_response.content, json.dumps(test_data).encode())
#
#         finally:
#             if 'cache_id' in locals():
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#                 self.created_resources  [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_14_large_binary_file(self):                                                # Test large binary handling
#         """Test handling of larger binary files"""
#         # Create a 1MB binary file
#         large_binary = bytes([i % 256 for i in range(1024 * 1024)])
#
#         try:
#             # Store large binary
#             store_result = self._store_binary(large_binary, strategy="direct")
#             cache_id = store_result['cache_id']
#
#             # Should be stored successfully
#             self.assertEqual(store_result['size'], len(large_binary))
#
#             # Generic endpoint should redirect (not try to base64 encode 1MB)
#             generic_url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#             generic_response = requests.get(generic_url, headers=self.headers)
#
#             result = generic_response.json()
#             self.assertEqual(result['status'], 'binary_data')
#             self.assertEqual(result['size'], len(large_binary))
#
#             # Binary endpoint should stream it properly
#             binary_url = f"{self.base_url}/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}"
#             binary_response = requests.get(binary_url, headers=self.headers, stream=True)
#
#             self.assertEqual(binary_response.status_code, 200)
#
#             # Read in chunks to avoid memory issues
#             received_data = b''
#             for chunk in binary_response.iter_content(chunk_size=8192):
#                 received_data += chunk
#
#             self.assertEqual(len(received_data), len(large_binary))
#             self.assertEqual(received_data, large_binary)
#
#         finally:
#             if 'cache_id' in locals():
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#                 self.created_resources  [:] = [r for r in self.created_resources if r['cache_id'] != cache_id]
#
#     def test_15_mixed_content_workflow(self):                                           # Test mixed content types
#         """Test workflow with mixed content types in same namespace"""
#         items = []
#
#         try:
#             # Store various types
#             items.append(('text', self._store_string("Hello World", strategy="direct")))
#             items.append(('json', self._store_json({"msg": "Hello"}, strategy="direct")))
#             items.append(('binary', self._store_binary(b'\x00\xFF', strategy="direct")))
#
#             # Verify namespace contains all types
#             stats_url = f"{self.base_url}/cache/stats/namespaces/{self.test_namespace}"
#             stats_response = requests.get(stats_url, headers=self.headers)
#
#             self.assertEqual(stats_response.status_code, 200)
#             stats = stats_response.json()
#             self.assertGreater(stats['direct_files'], 0)
#
#             # Verify each can be retrieved with appropriate endpoint
#             for item_type, store_result in items:
#                 cache_id = store_result['cache_id']
#
#                 if item_type == 'binary':
#                     # Binary should redirect in generic endpoint
#                     url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#                     response = requests.get(url, headers=self.headers)
#                     result = response.json()
#                     self.assertEqual(result['status'], 'binary_data')
#                 else:
#                     # String and JSON should work normally
#                     url = f"{self.base_url}/cache/retrieve/by-id/{cache_id}/{self.test_namespace}"
#                     response = requests.get(url, headers=self.headers)
#                     self.assertEqual(response.status_code, 200)
#                     result = response.json()
#                     self.assertIn('data', result)
#
#         finally:
#             # Clean up all items
#             for _, store_result in items:
#                 cache_id = store_result['cache_id']
#                 delete_url = f"{self.base_url}/cache/delete/by-id/{cache_id}/{self.test_namespace}"
#                 requests.delete(delete_url, headers=self.headers)
#
#             cache_ids = [r['cache_id'] for _, r in items]
#             self.created_resources  [:] = [r for r in self.created_resources if r['cache_id'] not in cache_ids]
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