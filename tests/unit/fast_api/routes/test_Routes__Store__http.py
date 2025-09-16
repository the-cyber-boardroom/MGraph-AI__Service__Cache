import gzip
import json
import pytest
import requests
import time
import concurrent.futures
from unittest                                                                       import TestCase
from typing                                                                         import Dict, Any
from osbot_fast_api.utils.Fast_API_Server                                           import Fast_API_Server
from osbot_utils.utils.Env                                                          import in_github_action
from osbot_utils.utils.Misc                                                         import is_guid
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE

class test_Routes__Store__http(TestCase):                                           # Local HTTP tests using temp FastAPI server

    @classmethod
    def setUpClass(cls):
        if in_github_action():
            pytest.skip("Skipping this test on GitHub Actions (because we are getting 404 on the routes below)")

        cls.key_name      = TEST_API_KEY__NAME
        cls.key_value     = TEST_API_KEY__VALUE
        cls.auth_headers  = {cls.key_name: cls.key_value }
        if not cls.key_name or not cls.key_value:
            pytest.skip("No Auth key name or key value provided")
        cls.service__fast_api = setup__service_fast_api_test_objs().fast_api
        cls.service__app      = cls.service__fast_api.app()
        cls.fast_api_server   = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url = cls.fast_api_server.url()
        cls.headers = cls.auth_headers

        # Track all created resources for cleanup
        cls.created_resources = []

        # Test namespace with timestamp to avoid conflicts
        cls.test_namespace = f"http-store-test-{int(time.time())}"

    @classmethod
    def tearDownClass(cls):                                                         # Clean up and stop server
        """Stop the server"""
        cls.fast_api_server.stop()

    def setUp(self):
        assert self.created_resources == []

    def tearDown(self):
        assert self.created_resources == []

    def _store_string(self, data: str, strategy: str = "temporal", namespace: str = None) -> Dict[str, Any]:
        """Helper to store string and track for cleanup"""
        namespace = namespace or self.test_namespace
        url = f"{self.base_url}/{namespace}/{strategy}/store/string"

        response = requests.post(url, data=data, headers={**self.headers, "Content-Type": "text/plain"})
        self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")

        result = response.json()
        cache_id = result.get('cache_id')

        # Track for cleanup
        self.created_resources.append({
            'cache_id': cache_id,
            'namespace': namespace,
            'type': 'string',
            'strategy': strategy
        })

        return result

    def _store_json(self, data: dict, strategy: str = "temporal", namespace: str = None) -> Dict[str, Any]:
        """Helper to store JSON and track for cleanup"""
        namespace = namespace or self.test_namespace
        url = f"{self.base_url}/{namespace}/{strategy}/store/json"

        response = requests.post(url, json=data, headers=self.headers)
        self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")

        result = response.json()
        cache_id = result.get('cache_id')

        # Track for cleanup
        self.created_resources.append({
            'cache_id': cache_id,
            'namespace': namespace,
            'type': 'json',
            'strategy': strategy
        })

        return result

    def _store_binary(self, data: bytes, strategy: str = "temporal", namespace: str = None,
                      content_encoding: str = None) -> Dict[str, Any]:
        """Helper to store binary and track for cleanup"""
        namespace = namespace or self.test_namespace
        url = f"{self.base_url}/{namespace}/{strategy}/store/binary"

        headers = {**self.headers, "Content-Type": "application/octet-stream"}
        if content_encoding:
            headers["Content-Encoding"] = content_encoding

        response = requests.post(url, data=data, headers=headers)
        self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")

        result = response.json()
        cache_id = result.get('cache_id')

        # Track for cleanup
        self.created_resources.append({
            'cache_id': cache_id,
            'namespace': namespace,
            'type': 'binary',
            'strategy': strategy
        })

        return result

    def test_01_health_check(self):                                                 # Test API is accessible
        """Verify the API is accessible and responding"""
        # Try health endpoint first
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    def test_02_store_string(self):                                                 # Test string storage
        """Test storing string data"""
        test_data = f"HTTP test data at {time.time()}"

        # Store data
        store_result = self._store_string(test_data, strategy="direct")
        cache_id = store_result['cache_id']
        cache_hash = store_result['hash']

        self.assertTrue(is_guid(cache_id))
        self.assertEqual(len(cache_hash), 16)
        self.assertIn('paths', store_result)

        # Clean up tracking
        self.created_resources.clear()

    def test_03_store_json(self):                                                   # Test JSON storage
        """Test storing JSON data"""
        test_json = {
            "test": "http_data",
            "timestamp": time.time(),
            "nested": {"level": 2, "data": "nested_value"}
        }

        # Store JSON
        store_result = self._store_json(test_json)
        cache_id = store_result['cache_id']
        cache_hash = store_result['hash']

        self.assertTrue(is_guid(cache_id))
        self.assertEqual(len(cache_hash), 16)

        # Clean up tracking
        self.created_resources.clear()

    def test_04_store_binary(self):                                                 # Test binary storage
        """Test storing binary data"""
        test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50  # Fake PNG header

        # Store binary data
        store_result = self._store_binary(test_binary, strategy="direct")
        cache_id = store_result['cache_id']
        cache_hash = store_result['hash']

        self.assertTrue(is_guid(cache_id))
        self.assertEqual(store_result['size'], len(test_binary))

        # Clean up tracking
        self.created_resources.clear()

    def test_05_multiple_strategies(self):                                          # Test all storage strategies
        """Test different storage strategies"""
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            data = f"Strategy test: {strategy}"
            result = self._store_string(data, strategy=strategy)

            # Verify paths structure differs by strategy
            self.assertIn('paths', result)
            self.assertIn('data', result['paths'])
            self.assertTrue(is_guid(result['cache_id']))

        # Clean up tracking
        self.created_resources.clear()

    def test_06_namespace_isolation(self):                                          # Test namespace isolation
        """Test that namespaces properly isolate data"""
        ns1 = f"http-ns1-{int(time.time())}"
        ns2 = f"http-ns2-{int(time.time())}"

        # Store same data in different namespaces
        data = "namespace isolation test"

        result1 = self._store_string(data, namespace=ns1)
        result2 = self._store_string(data, namespace=ns2)

        cache_id1 = result1['cache_id']
        cache_id2 = result2['cache_id']

        # Cache IDs should be different (unique per store)
        self.assertNotEqual(cache_id1, cache_id2)

        # But hashes should be the same (same data)
        self.assertEqual(result1['hash'], result2['hash'])

        # Clean up tracking
        self.created_resources.clear()

    def test_07_concurrent_operations(self):                                        # Test concurrent access
        """Test multiple operations can work concurrently"""

        def store_data(index):
            """Store data"""
            data = f"concurrent test {index}"

            # Store
            store_result = self._store_string(data, strategy="direct")
            cache_id = store_result['cache_id']

            return cache_id, is_guid(cache_id)

        # Run concurrent operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(store_data, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Verify all succeeded
        for cache_id, success in results:
            self.assertTrue(success, f"Concurrent operation failed for {cache_id}")

        # Clean up tracking
        self.created_resources.clear()

    def test_08_compressed_binary_handling(self):                                   # Test compressed binary
        """Test storing gzip compressed binary data"""
        original_data = b"Test data that will be compressed" * 100
        compressed_data = gzip.compress(original_data)

        # Store compressed binary with content-encoding
        store_result = self._store_binary(compressed_data, strategy="temporal",
                                         content_encoding="gzip")
        cache_id = store_result['cache_id']

        # Size should be compressed size
        self.assertLess(store_result['size'], len(original_data))
        self.assertTrue(is_guid(cache_id))

        # Clean up tracking
        self.created_resources.clear()

    def test_09_large_json(self):                                                   # Test large JSON
        """Test storing large JSON data"""
        large_json = {
            f"key_{i}": {
                "data": f"value_{i}",
                "nested": {"level": 2, "items": list(range(10))}
            } for i in range(100)
        }

        store_result = self._store_json(large_json, strategy="temporal_versioned")

        self.assertTrue(is_guid(store_result['cache_id']))
        self.assertGreater(store_result['size'], 1000)

        # Clean up tracking
        self.created_resources.clear()

    def test_10_empty_data(self):                                                   # Test empty data
        # Empty string (NOT Supported)
        # empty_string_result = self._store_string("", strategy="direct")
        # self.assertEqual(empty_string_result['size'], 0)

        # Empty JSON
        empty_json_result = self._store_json({}, strategy="direct")
        self.assertTrue(is_guid(empty_json_result['cache_id']))

        # Empty binary (NOT Supported)
        # empty_binary_result = self._store_binary(b"", strategy="direct")
        # self.assertEqual(empty_binary_result['size'], 0)

        # Clean up tracking
        self.created_resources.clear()

    def test_11_special_characters(self):                                           # Test special characters
        """Test storing data with special characters"""
        special_string = "Test with special chars: 你好世界 🚀 \n\t\r"

        store_result = self._store_string(special_string, strategy="temporal")

        self.assertTrue(is_guid(store_result['cache_id']))
        self.assertIn('hash', store_result)

        # Clean up tracking
        self.created_resources.clear()

    def test_12_duplicate_data(self):                                               # Test duplicate data
        """Test storing same data multiple times"""
        test_data = "duplicate test data"

        # Store twice
        result1 = self._store_string(test_data, strategy="temporal")
        result2 = self._store_string(test_data, strategy="temporal")

        # Same hash (same data)
        self.assertEqual(result1['hash'], result2['hash'])

        # Different cache IDs (separate operations)
        self.assertNotEqual(result1['cache_id'], result2['cache_id'])

        # Clean up tracking
        self.created_resources.clear()

    def test_13_json_with_nulls(self):                                              # Test JSON with nulls
        """Test storing JSON with null values"""
        json_with_nulls = {
            "key1": None,
            "key2": "value",
            "key3": [1, None, 3],
            "key4": {"nested": None}
        }

        store_result = self._store_json(json_with_nulls, strategy="temporal_latest")

        self.assertTrue(is_guid(store_result['cache_id']))
        self.assertIn('hash', store_result)

        # Clean up tracking
        self.created_resources.clear()

    def test_14_large_binary_file(self):                                            # Test large binary handling
        """Test handling of larger binary files"""
        # Create a 1MB binary file
        large_binary = bytes([i % 256 for i in range(1024 * 1024)])

        store_result = self._store_binary(large_binary, strategy="direct")

        self.assertEqual(store_result['size'], len(large_binary))
        self.assertTrue(is_guid(store_result['cache_id']))

        # Clean up tracking
        self.created_resources.clear()

    def test_15_mixed_content_workflow(self):                                       # Test mixed content types
        """Test workflow with mixed content types in same namespace"""

        # Store various types
        string_result = self._store_string("Hello World", strategy="direct")
        json_result = self._store_json({"msg": "Hello"}, strategy="direct")
        binary_result = self._store_binary(b'\x00\xFF', strategy="direct")

        # Verify all have valid cache IDs
        self.assertTrue(is_guid(string_result['cache_id']))
        self.assertTrue(is_guid(json_result['cache_id']))
        self.assertTrue(is_guid(binary_result['cache_id']))

        # Verify all have hashes
        self.assertIn('hash', string_result)
        self.assertIn('hash', json_result)
        self.assertIn('hash', binary_result)

        # Clean up tracking
        self.created_resources.clear()