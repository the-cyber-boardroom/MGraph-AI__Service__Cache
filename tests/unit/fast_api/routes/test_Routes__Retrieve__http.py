import base64
import gzip
import pytest
import requests
import time
import concurrent.futures
from unittest                                                                       import TestCase
from osbot_fast_api.utils.Fast_API_Server                                           import Fast_API_Server
from osbot_utils.utils.Env                                                          import in_github_action
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE

# todo: refactor these tests to use assert ... instead of assertEqual ....

class test_Routes__Retrieve__http(TestCase):                                        # Local HTTP tests using temp FastAPI server

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
        cls.test_namespace = f"http-retrieve-test-{int(time.time())}"

    @classmethod
    def tearDownClass(cls):                                                         # Stop server
        """Stop the server"""
        cls.fast_api_server.stop()

    def setUp(self):
        assert self.created_resources == []

    def tearDown(self):
        self.created_resources.clear()                                              # todo: add the delete resources used here

    def _store_test_data(self, data, data_type="string", namespace=None) -> tuple:  # todo: we should have a helper class that has these methods (and then knows how to remove these temp files
        """Helper to store data and return cache_id and hash"""
        namespace = namespace or self.test_namespace

        if data_type == "string":
            url = f"{self.base_url}/{namespace}/temporal/store/string"
            response = requests.post(url, data=data,
                                   headers={**self.headers, "Content-Type": "text/plain"})
        elif data_type == "json":
            url = f"{self.base_url}/{namespace}/temporal/store/json"
            response = requests.post(url, json=data, headers=self.headers)
        elif data_type == "binary":
            url = f"{self.base_url}/{namespace}/temporal/store/binary"
            response = requests.post(url, data=data,
                                   headers={**self.headers, "Content-Type": "application/octet-stream"})

        self.assertEqual(response.status_code, 200, f"Store failed: {response.text}")
        result = response.json()

        cache_id = result.get('cache_id')
        cache_hash = result.get('hash')

        # Track for potential cleanup
        self.created_resources.append({'cache_id' : cache_id,
                                       'namespace': namespace,
                                       'type'     : data_type})

        return cache_id, cache_hash

    def test_01_health_check(self):                                                 # Test API is accessible
        """Verify the API is accessible and responding"""
        # Try health endpoint first
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    def test_02_retrieve_by_id(self):                                               # Test retrieval by cache ID
        """Test retrieving data by cache ID"""
        test_data = f"HTTP retrieve test data at {time.time()}"

        # Store data
        cache_id, cache_hash = self._store_test_data(test_data)

        # Retrieve by ID
        retrieve_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)

        self.assertEqual(retrieve_response.status_code, 200)
        result = retrieve_response.json()

        self.assertIn('data', result)
        self.assertEqual(result['data'], test_data)
        self.assertIn('metadata', result)

    def test_03_retrieve_by_hash(self):                                             # Test retrieval by hash
        """Test retrieving data by hash"""
        test_data = "Test data for hash retrieval"

        # Store data
        cache_id, cache_hash = self._store_test_data(test_data)

        # Retrieve by hash
        retrieve_url = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{cache_hash}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)

        self.assertEqual(retrieve_response.status_code, 200)
        result = retrieve_response.json()

        self.assertIn('data', result)
        self.assertEqual(result['data'], test_data)

    def test_04_retrieve_not_found(self):                                           # Test 404 handling
        """Test retrieving non-existent data"""
        fake_id = "00000000-0000-0000-0000-000000000000"

        # Try to retrieve non-existent ID
        url = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}"
        response = requests.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result['status'], 'not_found')
        self.assertEqual(result['message'], 'Cache entry not found')

    def test_05_retrieve_string_format(self):                                       # Test string format retrieval
        """Test retrieving data as string format"""
        test_string = "String format test"

        cache_id, cache_hash = self._store_test_data(test_string)

        # Retrieve as string
        url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/string"
        response = requests.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text, test_string)
        self.assertIn('text/plain', response.headers['content-type'])

    def test_06_retrieve_json_format(self):                                         # Test JSON format retrieval
        """Test retrieving data as JSON format"""
        test_json = {"test": "json_data", "value": 123}

        cache_id, cache_hash = self._store_test_data(test_json, "json")

        # Retrieve as JSON
        url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/json"
        response = requests.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), test_json)
        self.assertIn('application/json', response.headers['content-type'])

    def test_07_retrieve_binary_format(self):                                       # Test binary format retrieval
        """Test retrieving data as binary format"""
        test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50

        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Retrieve as binary
        url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        response = requests.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, test_binary)
        self.assertEqual(response.headers['content-type'], 'application/octet-stream')

    def test_08_retrieve_by_hash_with_format(self):                                 # Test hash retrieval with format
        """Test retrieving by hash with specific format"""
        test_data = {"key": "value", "number": 42}

        cache_id, cache_hash = self._store_test_data(test_data, "json")

        # Test different formats
        formats = [
            ('string', 'text/plain'),
            ('json', 'application/json'),
            ('binary', 'application/octet-stream')
        ]

        for format_type, expected_content_type in formats:
            url = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{cache_hash}/{format_type}"
            response = requests.get(url, headers=self.headers)

            self.assertEqual(response.status_code, 200)
            self.assertIn(expected_content_type, response.headers['content-type'])

    def test_09_binary_redirect_pattern(self):                                      # Test binary redirect
        """Test that binary data redirects to binary endpoint"""
        test_binary = b'\x00\x01\x02\x03\x04\x05'

        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Try generic endpoint - should redirect
        generic_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
        generic_response = requests.get(generic_url, headers=self.headers)

        self.assertEqual(generic_response.status_code, 200)
        result = generic_response.json()

        # Should get redirect message
        self.assertEqual(result['status'], 'binary_data')
        self.assertEqual(result['message'], 'Binary data cannot be returned in JSON response')
        self.assertEqual(result['data_type'], 'binary')
        self.assertEqual(result['size'], len(test_binary))
        self.assertIn('binary_url', result)
        self.assertNotIn('data', result)

    def test_10_concurrent_retrieval(self):                                         # Test concurrent access
        """Test multiple concurrent retrieval operations"""

        # Store some test data first
        test_data = []
        for i in range(5):
            data = f"concurrent test data {i}"
            cache_id, cache_hash = self._store_test_data(data)
            test_data.append((cache_id, cache_hash, data))

        def retrieve_data(item):
            """Retrieve data by both ID and hash"""
            cache_id, cache_hash, expected_data = item

            # Retrieve by ID
            id_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
            id_response = requests.get(id_url, headers=self.headers)

            # Retrieve by hash
            hash_url = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{cache_hash}"
            hash_response = requests.get(hash_url, headers=self.headers)

            if id_response.status_code == 200 and hash_response.status_code == 200:
                id_data = id_response.json().get('data')
                hash_data = hash_response.json().get('data')
                return id_data == expected_data and hash_data == expected_data

            return False

        # Run concurrent retrievals
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(retrieve_data, item) for item in test_data]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # Verify all succeeded
        for success in results:
            self.assertTrue(success, "Concurrent retrieval failed")

    def test_11_compressed_data_retrieval(self):                                    # Test compressed data
        """Test retrieving compressed binary data"""
        original_data = b"Test data that will be compressed" * 100
        compressed_data = gzip.compress(original_data)

        # Store compressed
        url = f"{self.base_url}/{self.test_namespace}/temporal/store/binary"
        headers = {**self.headers,
                  "Content-Type": "application/octet-stream",
                  "Content-Encoding": "gzip"}
        store_response = requests.post(url, data=compressed_data, headers=headers)

        self.assertEqual(store_response.status_code, 200)
        cache_id = store_response.json()['cache_id']

        # Binary endpoint should return decompressed data
        binary_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        binary_response = requests.get(binary_url, headers=self.headers)

        self.assertEqual(binary_response.status_code, 200)
        self.assertEqual(binary_response.content, original_data)  # Decompressed!
        # Should NOT have Content-Encoding header (already decompressed)
        self.assertNotIn('content-encoding', binary_response.headers)

        # Track for cleanup
        self.created_resources.append({
            'cache_id': cache_id,
            'namespace': self.test_namespace,
            'type': 'binary'
        })

    def test_12_cross_type_retrieval(self):                                         # Test type conversion
        """Test retrieving data as different types"""
        # Store string that is valid JSON
        valid_json_string = '{"valid": "json", "data": 123}'
        cache_id, cache_hash = self._store_test_data(valid_json_string)

        # Retrieve as string - returns as-is
        string_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/string"
        string_response = requests.get(string_url, headers=self.headers)

        self.assertEqual(string_response.status_code, 200)
        self.assertEqual(string_response.text, valid_json_string)

        # Retrieve as JSON - should parse it
        json_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/json"
        json_response = requests.get(json_url, headers=self.headers)

        self.assertEqual(json_response.status_code, 200)
        self.assertEqual(json_response.json(), {"valid": "json", "data": 123})

        # Retrieve as binary - should convert to bytes
        binary_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        binary_response = requests.get(binary_url, headers=self.headers)

        self.assertEqual(binary_response.status_code, 200)
        self.assertEqual(binary_response.content, valid_json_string.encode('utf-8'))

    def test_13_base64_fallback(self):                                              # Test base64 encoding
        """Test base64 encoding for non-UTF8 binary"""
        non_utf8_binary = b'\xff\xfe\x00\x01\x02\x03'

        cache_id, cache_hash = self._store_test_data(non_utf8_binary, "binary")

        # Retrieve as string - should return base64
        string_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/string"
        string_response = requests.get(string_url, headers=self.headers)

        self.assertEqual(string_response.status_code, 200)
        expected_base64 = base64.b64encode(non_utf8_binary).decode('utf-8')
        self.assertEqual(string_response.text, expected_base64)

        # Retrieve as JSON - should wrap in base64
        json_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/json"
        json_response = requests.get(json_url, headers=self.headers)

        self.assertEqual(json_response.status_code, 200)
        result = json_response.json()
        self.assertEqual(result['data_type'], 'binary')
        self.assertEqual(result['encoding'], 'base64')
        self.assertEqual(result['data'], expected_base64)

    def test_14_404_responses(self):                                                # Test 404 handling
        """Test 404 responses for type-specific endpoints"""
        fake_id = "00000000-0000-0000-0000-000000000000"
        fake_hash = "0000000000000000"

        # String endpoints should return 404
        string_id_url = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}/string"
        string_response = requests.get(string_id_url, headers=self.headers)
        self.assertEqual(string_response.status_code, 404)

        string_hash_url = f"{self.base_url}/{self.test_namespace}/retrieve/hash/{fake_hash}/string"
        hash_response = requests.get(string_hash_url, headers=self.headers)
        self.assertEqual(hash_response.status_code, 404)

        # JSON endpoints should return JSON error
        json_id_url = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}/json"
        json_response = requests.get(json_id_url, headers=self.headers)
        self.assertEqual(json_response.status_code, 200)
        self.assertEqual(json_response.json()['status'], 'not_found')

        # Binary endpoints should return 404
        binary_id_url = f"{self.base_url}/{self.test_namespace}/retrieve/{fake_id}/binary"
        binary_response = requests.get(binary_id_url, headers=self.headers)
        self.assertEqual(binary_response.status_code, 404)

    def test_15_large_binary_retrieval(self):                                       # Test large binary
        """Test retrieving large binary files"""
        # Create a 1MB binary file
        large_binary = bytes([i % 256 for i in range(1024 * 1024)])

        cache_id, cache_hash = self._store_test_data(large_binary, "binary")

        # Generic endpoint should redirect
        generic_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}"
        generic_response = requests.get(generic_url, headers=self.headers)

        result = generic_response.json()
        self.assertEqual(result['status'], 'binary_data')
        self.assertEqual(result['size'], len(large_binary))

        # Binary endpoint should stream it properly
        binary_url = f"{self.base_url}/{self.test_namespace}/retrieve/{cache_id}/binary"
        binary_response = requests.get(binary_url, headers=self.headers, stream=True)

        self.assertEqual(binary_response.status_code, 200)

        # Read in chunks to avoid memory issues
        received_data = b''
        for chunk in binary_response.iter_content(chunk_size=8192):
            received_data += chunk

        self.assertEqual(len(received_data), len(large_binary))
        self.assertEqual(received_data, large_binary)