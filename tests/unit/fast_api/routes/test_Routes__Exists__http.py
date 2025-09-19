import pytest
import requests
import time
import concurrent.futures
from unittest                                    import TestCase
from osbot_fast_api.utils.Fast_API_Server        import Fast_API_Server
from osbot_fast_api_serverless.utils.testing.skip_tests import skip__if_not__in_github_actions
from osbot_utils.utils.Env                       import in_github_action
from tests.unit.Service__Fast_API__Test_Objs     import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Exists__http(TestCase):                                            # Local HTTP tests using temp FastAPI server

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        if in_github_action():
            pytest.skip("Skipping this test on GitHub Actions (because we are getting 404 on the routes below)")

        cls.test_objs          = setup__service_fast_api_test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.service__fast_api  = cls.test_objs.fast_api
        cls.service__app       = cls.test_objs.fast_api__app

        cls.fast_api_server = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url       = cls.fast_api_server.url()
        cls.headers        = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        cls.test_namespace = f"http-exists-test-{int(time.time())}"                  # Test-specific namespace (not fixtures)

    @classmethod
    def tearDownClass(cls):                                                           # Stop server
        cls.fast_api_server.stop()

    def test_01_health_check(self):                                                   # Test API is accessible
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)

        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test_02_exists_hash_with_fixtures(self):                                      # Test existence check using fixtures
        fixture_hash = self.cache_fixtures.get_fixture_hash("string_simple")

        exists_url      = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{fixture_hash}"
        exists_response = requests.get(exists_url, headers=self.headers)

        assert exists_response.status_code == 200
        result = exists_response.json()
        assert result['exists']    is True
        assert result['hash']      == fixture_hash
        assert result['namespace'] == str(self.fixtures_namespace)

    def test_03_not_exists_hash(self):                                                # Test non-existent hash
        fake_hash = "0000000000000000"
        url       = f"{self.base_url}/{self.test_namespace}/exists/hash/{fake_hash}"
        response  = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        result = response.json()
        assert result['exists']    is False
        assert result['hash']      == fake_hash
        assert result['namespace'] == self.test_namespace

    def test_04_namespace_isolation_with_fixtures(self):                              # Test namespace isolation using fixtures
        fixture_hash = self.cache_fixtures.get_fixture_hash("json_simple")

        url_fixtures = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{fixture_hash}"
        url_test     = f"{self.base_url}/{self.test_namespace}/exists/hash/{fixture_hash}"

        response_fixtures = requests.get(url_fixtures, headers=self.headers)
        response_test     = requests.get(url_test    , headers=self.headers)

        assert response_fixtures.json()['exists'] is True                            # Exists in fixtures namespace
        assert response_test.json()['exists']     is False                           # Not in test namespace

    def test_05_multiple_existence_checks_with_fixtures(self):                        # Test multiple checks using fixtures
        fixture_names = ["string_simple", "json_complex", "binary_small", "string_medium", "json_empty"]
        stored_hashes = [self.cache_fixtures.get_fixture_hash(name) for name in fixture_names]

        for cache_hash in stored_hashes:                                              # Check all fixtures exist
            url      = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            assert response.json()['exists'] is True

        fake_hashes = ["0000000000000000", "1111111111111111", "ffffffffffffffff"]    # Check non-existent hashes
        for fake_hash in fake_hashes:
            url      = f"{self.base_url}/{self.test_namespace}/exists/hash/{fake_hash}"
            response = requests.get(url, headers=self.headers)
            assert response.json()['exists'] is False

    def test_06_concurrent_existence_checks_with_fixtures(self):                      # Test concurrent access using fixtures
        test_hashes = [self.cache_fixtures.get_fixture_hash(name)
                      for name in self.cache_fixtures.fixtures.keys()]

        def check_exists(cache_hash):                                                 # Check if hash exists
            url      = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{cache_hash}"
            response = requests.get(url, headers=self.headers)
            return response.status_code == 200 and response.json()['exists']

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:        # Run concurrent checks
            futures = [executor.submit(check_exists, h) for h in test_hashes]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        for exists in results:                                                        # All fixtures should exist
            assert exists is True

    def test_07_default_namespace(self):                                              # Test default namespace
        fake_hash = "abcdef1234567890"
        url       = f"{self.base_url}/default/exists/hash/{fake_hash}"
        response  = requests.get(url, headers=self.headers)

        assert response.status_code        == 200
        result = response.json()
        assert result['exists']            is False                                   # Should not exist
        assert result['namespace']         == 'default'

    def test_08_rapid_existence_checks_with_fixtures(self):                           # Test rapid checks using fixtures
        fixture_hash = self.cache_fixtures.get_fixture_hash("string_large")
        results      = []

        for _ in range(10):                                                           # Rapid checks
            url      = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{fixture_hash}"
            response = requests.get(url, headers=self.headers)
            results.append(response.json()['exists'])

        assert all(results) is True                                                   # All should return true

    def test_09_mixed_existence_workflow(self):                                       # Test mixed workflow with fixtures
        existing_hashes = [self.cache_fixtures.get_fixture_hash("string_simple"),     # Get fixture hashes
                          self.cache_fixtures.get_fixture_hash("json_complex") ,
                          self.cache_fixtures.get_fixture_hash("binary_large") ]

        fake_hashes = ["0000000000000000", "1111111111111111", "2222222222222222"]    # Create fake hashes

        all_hashes = existing_hashes + fake_hashes                                    # Mix and check
        results    = {}

        for cache_hash in existing_hashes:                                            # Check fixtures in fixtures namespace
            url               = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{cache_hash}"
            response          = requests.get(url, headers=self.headers)
            results[cache_hash] = response.json()['exists']

        for cache_hash in fake_hashes:                                                # Check fakes in test namespace
            url               = f"{self.base_url}/{self.test_namespace}/exists/hash/{cache_hash}"
            response          = requests.get(url, headers=self.headers)
            results[cache_hash] = response.json()['exists']

        for cache_hash in existing_hashes:                                            # Verify correct results
            assert results[cache_hash] is True

        for cache_hash in fake_hashes:
            assert results[cache_hash] is False

    def test_10_performance_check_with_fixtures(self):                                # Test performance using fixtures
        skip__if_not__in_github_actions()
        fixture_hash = self.cache_fixtures.get_fixture_hash("json_simple")
        start_time   = time.time()
        num_checks   = 100

        for _ in range(num_checks):                                                   # Time multiple checks
            url      = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{fixture_hash}"
            response = requests.get(url, headers=self.headers)
            assert response.status_code == 200

        elapsed_time = time.time() - start_time
        avg_time     = elapsed_time / num_checks

        assert avg_time < 0.1                                                         # Should be reasonably fast

    def test_11_all_fixture_types(self):                                              # Test all fixture types
        all_fixtures = self.cache_fixtures.fixtures.keys()

        for fixture_name in all_fixtures:
            with self.subTest(fixture=fixture_name):
                fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)
                url          = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{fixture_hash}"
                response     = requests.get(url, headers=self.headers)

                assert response.status_code        == 200
                result = response.json()
                assert result['exists']            is True
                assert result['hash']              == fixture_hash
                assert result['namespace']         == str(self.fixtures_namespace)

    def test_12_integration_with_retrieve(self):                                      # Test integration with retrieve using fixtures
        fixture_name = "json_complex"
        fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)
        fixture_id   = self.cache_fixtures.get_fixture_id(fixture_name)

        exists_url = f"{self.base_url}/{self.fixtures_namespace}/exists/hash/{fixture_hash}"
        exists_response = requests.get(exists_url, headers=self.headers)
        assert exists_response.json()['exists'] is True                               # Check exists

        retrieve_url      = f"{self.base_url}/{self.fixtures_namespace}/retrieve/{fixture_id}"
        retrieve_response = requests.get(retrieve_url, headers=self.headers)
        assert retrieve_response.status_code == 200                                   # Retrieve to verify

        expected_data = self.cache_fixtures.get_fixture_data(fixture_name)            # Should get back the fixture data
        assert retrieve_response.json()['data'] == expected_data