from unittest                                                                            import TestCase

from osbot_fast_api_serverless.utils.testing.skip_tests import skip__if_not__in_github_actions

from tests.unit.Service__Fast_API__Test_Objs                                             import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash


class test_Routes__Exists__client(TestCase):                                          # Test exists routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service_fast_api_test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.client             = cls.test_objs.fast_api__client
        cls.app                = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        # Test data
        cls.test_namespace     = "test-exists-client"                                # Use different namespace for test-specific data
        cls.test_hash          = Safe_Str__Cache_Hash("0000000000000000")           # Known non-existent hash

    def test__health_check(self):                                                     # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}


    def test__bucket_name(self):                                                     # Verify API is accessible
        response = self.client.get('/admin/storage/bucket-name')
        assert response.status_code == 200
        assert response.json()       == {'bucket-name':  'NA'}

    def test__exists__hash__using_fixture(self):                                      # Test checking existing hash from fixtures
        # Use fixture that already exists
        fixture_hash = self.cache_fixtures.get_fixture_hash("string_simple")
        response     = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists"    : True                         ,
                          "hash"      : fixture_hash                  ,
                          "namespace" : str(self.fixtures_namespace) }

    def test__exists__hash__non_existent(self):                                       # Test checking non-existent hash
        non_existent = "0000000000000000"
        response     = self.client.get(f'/{self.test_namespace}/exists/hash/{non_existent}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists"    : False                  ,
                         "hash"      : non_existent            ,
                         "namespace" : self.test_namespace     }

    def test__exists__hash__namespace_isolation(self):                                # Test namespace isolation with fixtures
        # Use fixture hash from fixtures namespace
        fixture_hash = self.cache_fixtures.get_fixture_hash("json_simple")

        # Check exists in fixtures namespace
        response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')
        assert response.status_code == 200
        assert response.json()["exists"] is True

        # Check doesn't exist in test namespace
        response = self.client.get(f'/{self.test_namespace}/exists/hash/{fixture_hash}')
        assert response.status_code == 200
        assert response.json()["exists"] is False

    def test__exists__hash__multiple_formats(self):                                   # Test different hash formats
        # Test with different hash lengths and formats
        test_hashes = ["abc123def456789"      ,  # 15 chars
                       "0123456789abcdef"     ,  # 16 chars (standard)
                       "a" * 32               ]  # 32 chars

        for test_hash in test_hashes:
            with self.subTest(hash=test_hash):
                response = self.client.get(f'/{self.test_namespace}/exists/hash/{test_hash}')
                assert response.status_code == 200
                result = response.json()
                assert result["exists"]    is False                                   # None should exist in test namespace
                assert result["namespace"] == self.test_namespace
                assert result["hash"] == test_hash

        # Test invalid hash format
        not_an_hash = "test-hash-with-dash"
        expected_error = {'detail': [{'input': 'test-hash-with-dash',
                                      'loc': ['query', 'cache_hash'],
                                      'msg': 'in Safe_Str__Cache_Hash, value does not match required '
                                             'pattern: ^[a-f0-9]{10,96}$',
                                      'type': 'value_error'}]}
        assert self.client.get(f'/{self.test_namespace}/exists/hash/{not_an_hash}').json() == expected_error

    def test__exists__with_all_fixture_types(self):                                   # Test exists with all fixture types
        fixtures_to_test = ["string_simple" ,
                            "string_medium" ,
                            "string_large"  ,
                            "json_simple"   ,
                            "json_complex"  ,
                            "json_empty"    ,
                            "binary_small"  ,
                            "binary_medium" ,
                            "binary_large"  ]

        for fixture_name in fixtures_to_test:
            with self.subTest(fixture=fixture_name):
                fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)
                response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')        # Check exists in fixtures namespace
                assert response.status_code == 200
                assert response.json()["exists"] is True

    def test__exists__with_special_namespace(self):                                   # Test special characters in namespace
        special_ns = "test-namespace__!@**__"                                         # Namespace with special characters (will be sanitized)
        test_hash  = "abc0123456789def"

        response = self.client.get(f'/{special_ns}/exists/hash/{test_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result["exists"] is False
        assert result["hash"]   == test_hash
        # Namespace should be sanitized
        assert result["namespace"] != special_ns                                      # Changed due to sanitization
        assert result["namespace"] == 'test-namespace________'

    def test__exists__auth_required(self):                                            # Test authentication requirement
        auth_header = self.client.headers.pop(TEST_API_KEY__NAME)                     # Remove auth header temporarily

        response = self.client.get(f'/{self.test_namespace}/exists/hash/{self.test_hash}')
        assert response.status_code == 401

        self.client.headers[TEST_API_KEY__NAME] = auth_header                         # Restore auth header

    def test__exists__performance_with_fixtures(self):                                # Test multiple rapid requests using fixtures
        import time

        # Use existing fixture hashes
        fixture_names = ["string_simple", "json_simple", "binary_small", "json_complex", "string_medium"]
        hashes = [self.cache_fixtures.get_fixture_hash(name) for name in fixture_names]

        # Check all exist rapidly
        start_time = time.time()
        for hash_val in hashes:
            response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{hash_val}')
            assert response.status_code     == 200
            assert response.json()["exists"] is True

        elapsed = time.time() - start_time
        assert elapsed < 1.0                                                          # Should be fast (< 1 second for 5 checks)

    def test__exists__case_sensitivity(self):                                         # Test hash case sensitivity with fixtures
        # Get a fixture hash in lowercase
        lowercase_hash = self.cache_fixtures.get_fixture_hash("string_simple")

        # Check original case should exist
        response_lower = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{lowercase_hash}')
        assert response_lower.json()["exists"] is True

        # Different cases should not be valid (hashes are case-sensitive and lowercase only)
        uppercase_hash = lowercase_hash.upper()
        response_upper = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{uppercase_hash}')
        assert response_upper.status_code == 400                                      # Validation error for uppercase
        assert response_upper.json() == {'detail': [{'input': 'E15B31F87DF1896E',
                                                     'loc'  : ['query', 'cache_hash'],
                                                     'msg'  :  'in Safe_Str__Cache_Hash, value does not match required '
                                                               'pattern: ^[a-f0-9]{10,96}$',
                                                     'type' : 'value_error'}]}

    def test__exists__concurrent_checks_with_fixtures(self):                          # Test concurrent existence checks
        skip__if_not__in_github_actions()
        import concurrent.futures

        def check_exists(fixture_name):                                               # Check if a fixture hash exists
            fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)
            response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')

            if response.status_code == 200:
                return response.json()["exists"]
            return None

        # Run concurrent checks on all fixtures
        fixture_names = list(self.cache_fixtures.fixtures.keys())

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(check_exists, name) for name in fixture_names]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All should exist
        assert all(result is True for result in results)

    def test__exists__mixed_fixtures_and_non_existent(self):                          # Test mix of existing and non-existing
        # Mix fixture hashes with non-existent ones
        existing_hashes = [
            self.cache_fixtures.get_fixture_hash("string_simple"),
            self.cache_fixtures.get_fixture_hash("json_complex"),
            self.cache_fixtures.get_fixture_hash("binary_small")
        ]

        non_existent_hashes = [
            "0000000000000000",
            "1111111111111111",
            "ffffffffffffffff"
        ]

        # Check existing ones
        for hash_val in existing_hashes:
            response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{hash_val}')
            assert response.status_code == 200
            assert response.json()["exists"] is True

        # Check non-existing ones
        for hash_val in non_existent_hashes:
            response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{hash_val}')
            assert response.status_code == 200
            assert response.json()["exists"] is False