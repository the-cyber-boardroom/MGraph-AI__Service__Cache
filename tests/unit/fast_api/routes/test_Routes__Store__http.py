import gzip
import pytest
import requests
import time
import concurrent.futures
from unittest                                   import TestCase
from typing                                     import Dict, Any
from osbot_fast_api.utils.Fast_API_Server       import Fast_API_Server
from osbot_fast_api_serverless.utils.testing.skip_tests import skip__if_not__in_github_actions
from osbot_utils.utils.Env                      import in_github_action
from osbot_utils.utils.Misc                     import is_guid
from tests.unit.Service__Fast_API__Test_Objs    import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Store__http(TestCase):                                                     # Local HTTP tests using temp FastAPI server

    @classmethod
    def setUpClass(cls):                                                                      # ONE-TIME expensive setup
        #if in_github_action():
        #    pytest.skip("Skipping this test on GitHub Actions (because we are getting 404 on the routes below)")

        cls.test_objs         = setup__service_fast_api_test_objs()
        cls.cache_fixtures    = cls.test_objs.cache_fixtures
        cls.service__fast_api = cls.test_objs.fast_api
        cls.service__app      = cls.test_objs.fast_api__app

        cls.fast_api_server = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url       = cls.fast_api_server.url()
        cls.headers        = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        cls.test_namespace = f"http-store-test-{int(time.time())}"                            # Unique namespace

        cls.created_resources = []                                                            # Track created resources

    @classmethod
    def tearDownClass(cls):                                                                   # Stop server
        cls.fast_api_server.stop()

    def setUp(self):                                                                          # PER-TEST verification
        assert self.created_resources == []

    def tearDown(self):                                                                       # PER-TEST cleanup
        self.created_resources.clear()

    def _store_string(self, data     : str,
                           strategy : str = "temporal",
                           namespace: str = None
                     ) -> Dict[str, Any]:                                                     # Helper to store string
        namespace = namespace or self.test_namespace
        url       = f"{self.base_url}/{namespace}/{strategy}/store/string"

        response = requests.post(url     = url                                     ,
                                data    = data                                     ,
                                headers = {**self.headers, "Content-Type": "text/plain"})

        assert response.status_code == 200

        result   = response.json()
        cache_id = result.get('cache_id')

        self.created_resources.append({'cache_id' : cache_id ,                                # Track for cleanup
                                      'namespace': namespace ,
                                      'type'     : 'string'  ,
                                      'strategy' : strategy  })
        return result

    def _store_json(self, data     : dict,
                         strategy : str = "temporal",
                         namespace: str = None
                   ) -> Dict[str, Any]:                                                       # Helper to store JSON
        namespace = namespace or self.test_namespace
        url       = f"{self.base_url}/{namespace}/{strategy}/store/json"

        response = requests.post(url, json=data, headers=self.headers)
        assert response.status_code == 200

        result   = response.json()
        cache_id = result.get('cache_id')

        self.created_resources.append({'cache_id' : cache_id ,                                # Track for cleanup
                                      'namespace': namespace ,
                                      'type'     : 'json'    ,
                                      'strategy' : strategy  })
        return result

    def _store_binary(self, data            : bytes,
                           strategy         : str = "temporal",
                           namespace        : str = None      ,
                           content_encoding : str = None
                     ) -> Dict[str, Any]:                                                     # Helper to store binary
        namespace = namespace or self.test_namespace
        url       = f"{self.base_url}/{namespace}/{strategy}/store/binary"

        headers = {**self.headers, "Content-Type": "application/octet-stream"}
        if content_encoding:
            headers["Content-Encoding"] = content_encoding

        response = requests.post(url, data=data, headers=headers)
        assert response.status_code == 200

        result   = response.json()
        cache_id = result.get('cache_id')

        self.created_resources.append({'cache_id' : cache_id ,                                # Track for cleanup
                                      'namespace': namespace ,
                                      'type'     : 'binary'  ,
                                      'strategy' : strategy  })
        return result

    def test_01_health_check(self):                                                           # Test API is accessible
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)

        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test_02_store_string(self):                                                           # Test string storage
        test_data    = f"HTTP test data at {time.time()}"
        store_result = self._store_string(test_data, strategy="direct")

        cache_id   = store_result['cache_id']
        cache_hash = store_result['hash']

        assert is_guid(cache_id) is True
        assert len(cache_hash)   == 16
        assert 'paths'           in store_result

        self.created_resources.clear()                                                        # Clean up tracking

    def test_03_store_json(self):                                                             # Test JSON storage
        test_json = {"test"      : "http_data"         ,
                    "timestamp" : time.time()          ,
                    "nested"    : {"level": 2          ,
                                  "data" : "nested_value"}}

        store_result = self._store_json(test_json)
        cache_id     = store_result['cache_id']
        cache_hash   = store_result['hash']

        assert is_guid(cache_id) is True
        assert len(cache_hash)   == 16

        self.created_resources.clear()                                                        # Clean up tracking

    def test_04_store_binary(self):                                                           # Test binary storage
        test_binary  = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50                                   # Fake PNG header
        store_result = self._store_binary(test_binary, strategy="direct")

        cache_id   = store_result['cache_id']

        assert is_guid(cache_id)      is True
        assert store_result['size']   == len(test_binary)

        self.created_resources.clear()                                                        # Clean up tracking

    def test_05_multiple_strategies(self):                                                    # Test all strategies
        skip__if_not__in_github_actions()
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            data   = f"Strategy test: {strategy}"
            result = self._store_string(data, strategy=strategy)

            assert 'paths'    in result                                                       # Verify paths structure
            assert 'data'     in result['paths']
            assert is_guid(result['cache_id']) is True

        self.created_resources.clear()                                                        # Clean up tracking

    def test_06_namespace_isolation(self):                                                    # Test namespace isolation
        skip__if_not__in_github_actions()
        ns1  = f"http-ns1-{int(time.time())}"
        ns2  = f"http-ns2-{int(time.time())}"
        data = "namespace isolation test"

        result1 = self._store_string(data, namespace=ns1)
        result2 = self._store_string(data, namespace=ns2)

        cache_id1 = result1['cache_id']
        cache_id2 = result2['cache_id']

        assert cache_id1 != cache_id2                                                         # Different IDs
        assert result1['hash'] == result2['hash']                                             # Same hash

        self.created_resources.clear()                                                        # Clean up tracking

    def test_07_concurrent_operations(self):                                                  # Test concurrent access
        skip__if_not__in_github_actions()
        def store_data(index):                                                                # Store data concurrently
            data         = f"concurrent test {index}"
            store_result = self._store_string(data, strategy="direct")
            cache_id     = store_result['cache_id']

            return cache_id, is_guid(cache_id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:                # Run concurrent operations
            futures = [executor.submit(store_data, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        for cache_id, success in results:                                                     # Verify all succeeded
            assert success is True

        self.created_resources.clear()                                                        # Clean up tracking

    def test_08_compressed_binary_handling(self):                                             # Test compressed binary
        original_data   = b"Test data that will be compressed" * 100
        compressed_data = gzip.compress(original_data)

        store_result = self._store_binary(compressed_data       ,
                                          strategy="temporal"   ,
                                          content_encoding="gzip")

        cache_id = store_result['cache_id']

        assert store_result['size'] < len(original_data)                                      # Compressed size
        assert is_guid(cache_id)    is True

        self.created_resources.clear()                                                        # Clean up tracking

    def test_09_large_json(self):                                                             # Test large JSON
        large_json = {f"key_{i}": {"data"  : f"value_{i}"               ,
                                   "nested": {"level": 2                ,
                                             "items": list(range(10))   }}
                     for i in range(100)}

        store_result = self._store_json(large_json, strategy="temporal_versioned")

        assert is_guid(store_result['cache_id']) is True
        assert store_result['size']              > 1000

        self.created_resources.clear()                                                        # Clean up tracking

    def test_10_empty_data(self):                                                             # Test empty data
        empty_json_result = self._store_json({}, strategy="direct")                           # Empty JSON is valid

        assert is_guid(empty_json_result['cache_id']) is True

        self.created_resources.clear()                                                        # Clean up tracking

    def test_11_special_characters(self):                                                     # Test special characters
        special_string = "Test with special chars: 你好世界 🚀 \n\t\r"
        store_result   = self._store_string(special_string, strategy="temporal")

        assert is_guid(store_result['cache_id']) is True
        assert 'hash' in store_result

        self.created_resources.clear()                                                        # Clean up tracking

    def test_12_duplicate_data(self):                                                         # Test duplicate data
        skip__if_not__in_github_actions()
        test_data = "duplicate test data"

        result1 = self._store_string(test_data, strategy="temporal")                          # Store twice
        result2 = self._store_string(test_data, strategy="temporal")

        assert result1['hash']     == result2['hash']                                         # Same hash
        assert result1['cache_id'] != result2['cache_id']                                     # Different IDs

        self.created_resources.clear()                                                        # Clean up tracking

    def test_13_json_with_nulls(self):                                                        # Test JSON with nulls
        json_with_nulls = {"key1"  : None              ,
                          "key2"  : "value"            ,
                          "key3"  : [1, None, 3]       ,
                          "key4"  : {"nested": None}   }

        store_result = self._store_json(json_with_nulls, strategy="temporal_latest")

        assert is_guid(store_result['cache_id']) is True
        assert 'hash' in store_result

        self.created_resources.clear()                                                        # Clean up tracking

    def test_14_large_binary_file(self):                                                      # Test large binary
        skip__if_not__in_github_actions()
        large_binary = bytes([i % 256 for i in range(1024 * 1024)])                          # 1MB binary
        store_result = self._store_binary(large_binary, strategy="direct")

        assert store_result['size']              == len(large_binary)
        assert is_guid(store_result['cache_id']) is True

        self.created_resources.clear()                                                        # Clean up tracking

    def test_15_mixed_content_workflow(self):                                                 # Test mixed content types
        skip__if_not__in_github_actions()
        string_result = self._store_string("Hello World", strategy="direct")
        json_result   = self._store_json({"msg": "Hello"}, strategy="direct")
        binary_result = self._store_binary(b'\x00\xFF', strategy="direct")

        assert is_guid(string_result['cache_id']) is True                                     # Verify all valid
        assert is_guid(json_result['cache_id'])   is True
        assert is_guid(binary_result['cache_id']) is True

        assert 'hash' in string_result                                                        # Verify all have hashes
        assert 'hash' in json_result
        assert 'hash' in binary_result

        self.created_resources.clear()                                                        # Clean up tracking