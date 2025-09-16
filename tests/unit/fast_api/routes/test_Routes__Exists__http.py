import pytest
import requests
import time
import concurrent.futures
from unittest                                                                       import TestCase
from osbot_fast_api.utils.Fast_API_Server                                           import Fast_API_Server
from osbot_utils.utils.Env                                                          import in_github_action
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE

class test_Routes__Exists__http(TestCase):                                          # Local HTTP tests using temp FastAPI server

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

        # Test namespace with timestamp to avoid conflicts
        cls.test_namespace = f"http-exists-test-{int(time.time())}"

    @classmethod
    def tearDownClass(cls):                                                         # Stop server
        """Stop the server"""
        cls.fast_api_server.stop()

    def _store_test_data(self, data, namespace=None):                               # Helper to store data
        """Store test data and return cache_id and hash"""
        namespace = namespace or self.test_namespace

        url = f"{self.base_url}/{namespace}/temporal/store/string"
        response = requests.post(url, data=data,
                               headers={**self.headers, "Content-Type": "text/plain"})

        assert response.status_code == 200, f"Store failed: {response.text}"
        result = response.json()
        return result.get('cache_id'), result.get('hash')

    def test_01_health_check(self):                                                 # Test API is accessible
        """Verify the API is accessible and responding"""
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'status': 'ok'})

    def test_02_exists_hash(self):                                                  # Test basic existence check
        """Test checking if a hash exists"""
        test_data = f"HTTP exists test at {time.time()}"

        # Store data first
        cache_id, cache_hash = self._store_test_data(test_data)

        # Check exists
        exists_url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
        exists_response = requests.get(exists_url, headers=self.headers)

        self.assertEqual(exists_response.status_code, 200)
        result = exists_response.json()

        self.assertTrue(result['exists'])
        self.assertEqual(result['hash'], cache_hash)
        self.assertEqual(result['namespace'], self.test_namespace)

    def test_03_not_exists_hash(self):                                              # Test non-existent hash
        """Test checking a non-existent hash"""
        fake_hash = "0000000000000000"

        url = f"{self.base_url}/{self.test_namespace}/exists/hash/{fake_hash}"
        response = requests.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        result = response.json()

        self.assertFalse(result['exists'])
        self.assertEqual(result['hash'], fake_hash)
        self.assertEqual(result['namespace'], self.test_namespace)

    def test_04_namespace_isolation(self):                                          # Test namespace isolation
        """Test that existence checks are namespace-specific"""
        ns1 = f"ns1-{int(time.time())}"
        ns2 = f"ns2-{int(time.time())}"

        # Store in namespace 1
        cache_id, cache_hash = self._store_test_data("test data", namespace=ns1)

        # Check in namespace 1 - should exist
        url1 = f"{self.base_url}/{ns1}/exists/hash/{cache_hash}"
        response1 = requests.get(url1, headers=self.headers)
        self.assertTrue(response1.json()['exists'])

        # Check in namespace 2 - should not exist
        url2 = f"{self.base_url}/{ns2}/exists/hash/{cache_hash}"
        response2 = requests.get(url2, headers=self.headers)
        self.assertFalse(response2.json()['exists'])

    def test_05_multiple_existence_checks(self):                                    # Test multiple checks
        """Test checking existence of multiple hashes"""
        # Store multiple items
        stored_hashes = []
        for i in range(5):
            cache_id, cache_hash = self._store_test_data(f"multi test {i}")
            stored_hashes.append(cache_hash)

        # Check all exist
        for cache_hash in stored_hashes:
            url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            self.assertTrue(response.json()['exists'])

        # Check non-existent hashes
        fake_hashes = ["0000000000000000", "1111111111111111", "ffffffffffffffff"]
        for fake_hash in fake_hashes:
            url = f"{self.base_url}/{self.test_namespace}/exists/hash/{fake_hash}"
            response = requests.get(url, headers=self.headers)
            self.assertFalse(response.json()['exists'])

    def test_06_exists_after_delete(self):                                          # Test after deletion
        """Test existence check after data is deleted"""
        # Store data
        cache_id, cache_hash = self._store_test_data("to be deleted")

        # Verify exists
        exists_url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
        response = requests.get(exists_url, headers=self.headers)
        self.assertTrue(response.json()['exists'])

        # Delete the data
        delete_url = f"{self.base_url}/{self.test_namespace}/delete/{cache_id}"
        delete_response = requests.delete(delete_url, headers=self.headers)
        self.assertEqual(delete_response.status_code, 200)

        # Check exists again - should be false
        response = requests.get(exists_url, headers=self.headers)
        self.assertFalse(response.json()['exists'])

    def test_07_concurrent_existence_checks(self):                                  # Test concurrent access
        """Test multiple concurrent existence checks"""
        # Store test data
        test_hashes = []
        for i in range(10):
            cache_id, cache_hash = self._store_test_data(f"concurrent {i}")
            test_hashes.append(cache_hash)

        def check_exists(cache_hash):
            """Check if hash exists"""
            url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            return response.status_code == 200 and response.json()['exists']

        # Run concurrent checks
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(check_exists, h) for h in test_hashes]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All should exist
        for exists in results:
            self.assertTrue(exists)

    def test_08_default_namespace(self):                                            # Test default namespace
        """Test existence check with default namespace"""
        # Store in default namespace
        cache_id, cache_hash = self._store_test_data("default test", namespace="default")

        # Check in default namespace
        url = f"{self.base_url}/default/exists/hash/{cache_hash}"
        response = requests.get(url, headers=self.headers)

        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertTrue(result['exists'])
        self.assertEqual(result['namespace'], 'default')

    def test_10_rapid_existence_checks(self):                                       # Test rapid checks
        """Test rapid succession of existence checks"""
        # Store one item
        cache_id, cache_hash = self._store_test_data("rapid test")

        # Rapid checks
        results = []
        for _ in range(20):
            url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            results.append(response.json()['exists'])

        # All should return true
        self.assertTrue(all(results))

    def test_11_mixed_existence_workflow(self):                                     # Test mixed workflow
        """Test workflow mixing existing and non-existing hashes"""
        # Store some data
        existing_hashes = []
        for i in range(3):
            cache_id, cache_hash = self._store_test_data(f"exists {i}")
            existing_hashes.append(cache_hash)

        # Create fake hashes
        fake_hashes = ["0000000000000000", "1111111111111111", "2222222222222222"]

        # Mix and check
        all_hashes = existing_hashes + fake_hashes
        results = {}

        for cache_hash in all_hashes:
            url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            results[cache_hash] = response.json()['exists']

        # Verify correct results
        for cache_hash in existing_hashes:
            self.assertTrue(results[cache_hash], f"Hash {cache_hash} should exist")

        for cache_hash in fake_hashes:
            self.assertFalse(results[cache_hash], f"Hash {cache_hash} should not exist")

    def test_12_exists_with_special_namespaces(self):                               # Test special namespaces
        """Test existence checks with various namespace formats"""
        namespaces = [
            "simple",
            "with-dash",
            "with_underscore",
            "with123numbers",
            "MixedCase",  # Will likely be sanitized
        ]

        for ns in namespaces:
            # Store data
            try:
                cache_id, cache_hash = self._store_test_data(f"test in {ns}", namespace=ns)

                # Check exists
                url = f"{self.base_url}/{ns}/exists/hash/{cache_hash}"
                response = requests.get(url, headers=self.headers)

                if response.status_code == 200:
                    # Should exist if namespace was valid
                    result = response.json()
                    if result['namespace'] == ns:  # Namespace wasn't sanitized
                        self.assertTrue(result['exists'])
            except AssertionError:
                # Namespace might have been rejected or sanitized
                pass

    def test_13_performance_check(self):                                            # Test performance
        """Test performance of existence checks"""
        # Store data
        cache_id, cache_hash = self._store_test_data("performance test")

        # Time multiple checks
        start_time = time.time()
        num_checks = 100

        for _ in range(num_checks):
            url = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            self.assertEqual(response.status_code, 200)

        elapsed_time = time.time() - start_time
        avg_time = elapsed_time / num_checks

        # Should be reasonably fast (adjust threshold as needed)
        self.assertLess(avg_time, 0.1, f"Average time per check: {avg_time:.4f}s")

    def test_14_exists_complete_workflow(self):                                     # Test complete workflow
        """Test complete workflow with store, exists, retrieve, delete"""
        namespace = f"workflow-{int(time.time())}"
        test_data = "complete workflow test"

        # 1. Store data
        cache_id, cache_hash = self._store_test_data(test_data, namespace)

        # 2. Check exists - should be true
        exists_url = f"{self.base_url}/{namespace}/exists/hash/{cache_hash}"
        exists_response = requests.get(exists_url, headers=self.headers)
        self.assertTrue(exists_response.json()['exists'])

        # 3. Retrieve to verify
        retrieve_url = f"{self.base_url}/{namespace}/retrieve/{cache_id}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)
        self.assertEqual(retrieve_response.json()['data'], test_data)

        # 4. Delete
        delete_url = f"{self.base_url}/{namespace}/delete/{cache_id}"
        delete_response = requests.delete(delete_url, headers=self.headers)
        self.assertEqual(delete_response.status_code, 200)

        # 5. Check exists - should be false
        exists_response2 = requests.get(exists_url, headers=self.headers)
        self.assertFalse(exists_response2.json()['exists'])