import base64
import gzip
import pytest
import requests
import time
import concurrent.futures
from unittest                                                                       import TestCase
from osbot_fast_api.utils.Fast_API_Server                                           import Fast_API_Server
from osbot_fast_api_serverless.utils.testing.skip_tests import skip__if_not__in_github_actions
from osbot_utils.testing.__ import __, __SKIP__
from osbot_utils.utils.Objects import obj

from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Retrieve__http(TestCase):                                                  # Local HTTP tests using temp FastAPI server

    @classmethod
    def setUpClass(cls):                                                                      # ONE-TIME expensive setup
        # if in_github_action():
        #     pytest.skip("Skipping this test on GitHub Actions (because we are getting 404 on the routes below)")

        cls.test_objs         = setup__service_fast_api_test_objs()
        cls.cache_fixtures    = cls.test_objs.cache_fixtures
        cls.service__fast_api = cls.test_objs.fast_api
        cls.service__app      = cls.test_objs.fast_api__app

        cls.fast_api_server = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url          = cls.fast_api_server.url().rstrip("/")
        cls.headers           = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        cls.test_namespace    = f"http-retrieve-test-{int(time.time())}"                      # Unique namespace
        cls.created_resources = []                                                            # Track created resources

    @classmethod
    def tearDownClass(cls):                                                                   # Stop server
        cls.fast_api_server.stop()

    def setUp(self):                                                                          # PER-TEST verification
        assert self.created_resources == []

    def tearDown(self):                                                                       # PER-TEST cleanup
        self.created_resources.clear()

    def _store_test_data(self, data, data_type="string", namespace=None) -> tuple:            # Helper to store and track data
        namespace = namespace or self.test_namespace

        if data_type == "string":
            url      = f"{self.base_url}/{namespace}/temporal/store/string"
            response = requests.post(url     = url                                      ,
                                    data    = data                                      ,
                                    headers = {**self.headers, "Content-Type": "text/plain"})
        elif data_type == "json":
            url      = f"{self.base_url}/{namespace}/temporal/store/json"
            response = requests.post(url, json=data, headers=self.headers)
        elif data_type == "binary":
            url      = f"{self.base_url}/{namespace}/temporal/store/binary"
            response = requests.post(url     = url                                              ,
                                    data    = data                                              ,
                                    headers = {**self.headers, "Content-Type": "application/octet-stream"})

        assert response.status_code == 200

        result     = response.json()
        cache_id   = result.get('cache_id')
        cache_hash = result.get('hash')

        self.created_resources.append({'cache_id' : cache_id ,                                # Track for cleanup
                                      'namespace': namespace ,
                                      'type'     : data_type })

        return cache_id, cache_hash

    def test_01_health_check(self):                                                           # Test API is accessible
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)

        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test_02_retrieve_by_id(self):                                                         # Test retrieval by cache ID
        test_data = f"HTTP retrieve test data at {time.time()}"
        cache_id, cache_hash = self._store_test_data(test_data)

        retrieve_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)

        assert retrieve_response.status_code == 200
        result = retrieve_response.json()

        assert 'data'     in result
        assert 'metadata' in result
        assert result['data'] == test_data

    def test_03_retrieve_by_hash(self):                                                       # Test retrieval by hash
        test_data = "Test data for hash retrieval"
        cache_id, cache_hash = self._store_test_data(test_data)

        retrieve_url      = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{cache_hash}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)

        assert retrieve_response.status_code == 200
        result = retrieve_response.json()

        assert 'data' in result
        assert result['data'] == test_data

    def test_04_retrieve_not_found(self):                                                     # Test 404 handling
        fake_id  = "00000000-0000-0000-0000-000000000000"
        url      = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}"
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 404
        result = response.json()
        assert obj(result) == __(detail=__(resource_id   = None                 ,
                                           cache_hash    = None                 ,
                                           cache_id      = fake_id              ,
                                           namespace     = self.test_namespace  ,
                                           error_type    = 'NOT_FOUND'          ,
                                           message       = 'The requested cache entry was not found',
                                           timestamp     = __SKIP__             ,
                                           request_id    = __SKIP__             ,
                                           resource_type = 'cache_entry'        ))

    def test_05_retrieve_string_format(self):                                                 # Test string format retrieval
        test_string = "String format test"
        cache_id, cache_hash = self._store_test_data(test_string)

        url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/string"
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        assert response.text        == test_string
        assert 'text/plain'         in response.headers['content-type']

    def test_06_retrieve_json_format(self):                                                   # Test JSON format retrieval
        test_json = {"test": "json_data", "value": 123}
        cache_id, cache_hash = self._store_test_data(test_json, "json")

        url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/json"
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        assert response.json()       == test_json
        assert 'application/json'    in response.headers['content-type']

    def test_07_retrieve_binary_format(self):                                                 # Test binary format retrieval
        test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        response = requests.get(url, headers=self.headers)

        assert response.status_code              == 200
        assert response.content                  == test_binary
        assert response.headers['content-type']  == 'application/octet-stream'

    def test_08_retrieve_by_hash_with_format(self):                                           # Test hash retrieval with format
        skip__if_not__in_github_actions()
        test_data = {"key": "value", "number": 42}
        cache_id, cache_hash = self._store_test_data(test_data, "json")

        formats = [('string', 'text/plain'            ),                                      # Test different formats
                  ('json'  , 'application/json'      ),
                  ('binary', 'application/octet-stream')]

        for format_type, expected_content_type in formats:
            url      = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{cache_hash}/{format_type}"
            response = requests.get(url, headers=self.headers)

            assert response.status_code == 200
            assert expected_content_type in response.headers['content-type']

    def test_09_binary_redirect_pattern(self):                                                # Test binary redirect
        test_binary = b'\x00\x01\x02\x03\x04\x05'
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        generic_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"       # Try generic endpoint
        generic_response = requests.get(generic_url, headers=self.headers)
        result           = generic_response.json()
        assert generic_response.status_code == 200
        assert obj(result)                  == __( message      = 'Binary data requires separate endpoint',
                                                   data_type    = 'binary'              ,
                                                   size         = 0                     ,
                                                   cache_hash   = cache_hash            ,
                                                   cache_id     = cache_id              ,
                                                   namespace    = self.test_namespace   ,
                                                   binary_url   = f'/{self.test_namespace}/retrieve/{cache_id}/binary',
                                                   metadata     = __(cache_id           = cache_id                    ,
                                                                     cache_hash         = '17e88db187afd62c'          ,
                                                                     cache_key          = 'None'                      ,
                                                                     file_id            = cache_id                    ,
                                                                     namespace          = self.test_namespace         ,
                                                                     strategy           = 'temporal'                  ,
                                                                     stored_at          = __SKIP__                    ,
                                                                     file_type          ='binary'                     ,
                                                                     content_encoding   = None                        ,
                                                                     content_size       = 0                           ))

    def test_10_concurrent_retrieval(self):                                                   # Test concurrent access
        skip__if_not__in_github_actions()
        test_data = []                                                                        # Store test data first
        for i in range(5):
            data = f"concurrent test data {i}"
            cache_id, cache_hash = self._store_test_data(data)
            test_data.append((cache_id, cache_hash, data))

        def retrieve_data(item):                                                              # Retrieve by ID and hash
            cache_id, cache_hash, expected_data = item

            id_url       = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
            id_response  = requests.get(id_url, headers=self.headers)

            hash_url     = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{cache_hash}"
            hash_response = requests.get(hash_url, headers=self.headers)

            if id_response.status_code == 200 and hash_response.status_code == 200:
                id_data   = id_response.json().get('data')
                hash_data = hash_response.json().get('data')
                return id_data == expected_data and hash_data == expected_data

            return False

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:                # Run concurrent retrievals
            futures = [executor.submit(retrieve_data, item) for item in test_data]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        for success in results:                                                               # Verify all succeeded
            assert success is True

    def test_11_compressed_data_retrieval(self):                                              # Test compressed data
        original_data   = b"Test data that will be compressed" * 100
        compressed_data = gzip.compress(original_data)

        url     = f"{self.base_url}/{self.test_namespace}/temporal/store/binary"              # Store compressed
        headers = {**self.headers                    ,
                  "Content-Type"     : "application/octet-stream",
                  "Content-Encoding" : "gzip"               }

        store_response = requests.post(url, data=compressed_data, headers=headers)
        assert store_response.status_code == 200

        cache_id = store_response.json()['cache_id']

        binary_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        binary_response = requests.get(binary_url, headers=self.headers)

        assert binary_response.status_code == 200
        assert binary_response.content     == original_data                                   # Decompressed!
        assert 'content-encoding'          not in binary_response.headers                     # Already decompressed

        self.created_resources.append({'cache_id' : cache_id             ,                    # Track for cleanup
                                       'namespace': self.test_namespace  ,
                                       'type'     : 'binary'            })

    def test_12_cross_type_retrieval(self):                                                   # Test type conversion
        valid_json_string = '{"valid": "json", "data": 123}'                                  # String that is valid JSON
        cache_id, cache_hash = self._store_test_data(valid_json_string)

        string_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/string"
        string_response = requests.get(string_url, headers=self.headers)

        assert string_response.status_code == 200
        assert string_response.text        == valid_json_string

        json_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/json"
        json_response = requests.get(json_url, headers=self.headers)

        assert json_response.status_code == 200
        assert json_response.json()      == {"valid": "json", "data": 123}

        binary_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        binary_response = requests.get(binary_url, headers=self.headers)

        assert binary_response.status_code == 200
        assert binary_response.content     == valid_json_string.encode('utf-8')

    def test_13_base64_fallback(self):                                                        # Test base64 encoding
        non_utf8_binary = b'\xff\xfe\x00\x01\x02\x03'
        cache_id, cache_hash = self._store_test_data(non_utf8_binary, "binary")

        string_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/string"
        string_response = requests.get(string_url, headers=self.headers)

        assert string_response.status_code == 200
        expected_base64 = base64.b64encode(non_utf8_binary).decode('utf-8')
        assert string_response.text        == expected_base64

        json_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/json"
        json_response = requests.get(json_url, headers=self.headers)

        assert json_response.status_code == 200
        result = json_response.json()
        assert result['data_type'] == 'binary'
        assert result['encoding']  == 'base64'
        assert result['data']      == expected_base64

    def test_14_404_responses(self):                                                          # Test 404 handling
        fake_id   = "00000000-0000-0000-0000-000000000000"
        fake_hash = "0000000000000000"

        string_id_url   = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}/string"
        string_response = requests.get(string_id_url, headers=self.headers)
        assert string_response.status_code == 404

        string_hash_url = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{fake_hash}/string"
        hash_response   = requests.get(string_hash_url, headers=self.headers)
        assert hash_response.status_code == 404

        json_id_url   = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}/json"
        json_response = requests.get(json_id_url, headers=self.headers)
        assert json_response.status_code == 404
        assert json_response.json() == {'detail': 'Cache entry not found'}

        binary_id_url   = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}/binary"
        binary_response = requests.get(binary_id_url, headers=self.headers)
        assert binary_response.status_code == 404

    def test_15_large_binary_retrieval(self):                                                 # Test large binary
        #skip__if_not__in_github_actions()
        large_binary = bytes([i % 256 for i in range(1024 * 1024)])                          # 1MB binary
        cache_id, cache_hash = self._store_test_data(large_binary, "binary")

        generic_url      = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"       # Generic endpoint should redirect
        generic_response = requests.get(generic_url, headers=self.headers)

        result = generic_response.json()

        assert obj(result) == __(message    = 'Binary data requires separate endpoint'              ,
                                 data_type  = 'binary'                                              ,
                                 size       = 0                                                     ,
                                 cache_hash = 'fbbab289f7f94b25'                                    ,
                                 cache_id   = cache_id                                              ,
                                 namespace  = self.test_namespace                                   ,
                                 binary_url = f'/{self.test_namespace}/retrieve/{cache_id}/binary'  ,
                                 metadata   = __(cache_id          = cache_id                       ,
                                                 cache_hash        = 'fbbab289f7f94b25'             ,
                                                 cache_key         = 'None'                         ,
                                                 file_id           = cache_id                       ,
                                                 namespace         = self.test_namespace            ,
                                                 strategy          = 'temporal'                     ,
                                                 stored_at         = __SKIP__                       ,
                                                 file_type         = 'binary'                       ,
                                                 content_encoding  = None                           ,
                                                 content_size      = 0                              ))


        binary_url      =  f"{self.base_url}{obj(result).binary_url}"
        binary_response = requests.get(binary_url, headers=self.headers, stream=True)

        assert binary_response.status_code == 200

        received_data = b''                                                                   # Read in chunks
        for chunk in binary_response.iter_content(chunk_size=8192):
            received_data += chunk

        assert len(received_data) == len(large_binary)
        assert received_data      == large_binary