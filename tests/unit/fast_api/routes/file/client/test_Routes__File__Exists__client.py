from unittest                                                                            import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                                  import skip__if_not__in_github_actions
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                       import Cache_Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from tests.unit.Service__Cache__Test_Objs                                                import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__File__Exists__client(TestCase):                                          # Test exists routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        cls.test_objs          = setup__service__cache__test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.client             = cls.test_objs.fast_api__client
        cls.app                = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        # Test data
        cls.test_namespace     = "test-exists-client"                                # Use different namespace for test-specific data
        cls.test_hash          = Safe_Str__Cache_Hash("0000000000000000")           # Known non-existent hash
        cls.test_cache_id      = Cache_Id()                                          # Random non-existent cache_id

    def test__health_check(self):                                                     # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__bucket_name(self):                                                     # Verify API is accessible
        response = self.client.get('/admin/storage/bucket-name')
        assert response.status_code == 200
        assert response.json()       == {'bucket-name':  'NA'}

    # ═══════════════════════════════════════════════════════════════════════════
    # exists__cache_id tests
    # ═══════════════════════════════════════════════════════════════════════════

    def test__exists__cache_id__using_fixture(self):                                  # Test checking existing cache_id from fixtures
        fixture_id = self.cache_fixtures.get_fixture_id("string_simple")
        response   = self.client.get(f'/{self.fixtures_namespace}/exists/id/{fixture_id}')

        assert response.status_code == 200
        result = response.json()
        assert result == { "exists"     : True                         ,
                           "cache_id"   : str(fixture_id)              ,
                           "cache_hash" : None                         ,
                           "namespace"  : str(self.fixtures_namespace) }

    def test__exists__cache_id__non_existent(self):                                   # Test checking non-existent cache_id
        non_existent_id = Random_Guid()
        response        = self.client.get(f'/{self.test_namespace}/exists/id/{non_existent_id}')

        assert response.status_code == 200
        assert response.json()      == { "exists"     : False                  ,
                                         "cache_id"   : str(non_existent_id)   ,
                                         "cache_hash" : None                   ,
                                         "namespace"  : self.test_namespace    }

    def test__exists__cache_id__namespace_isolation(self):                            # Test namespace isolation with fixtures
        fixture_id = self.cache_fixtures.get_fixture_id("json_simple")

        # Check exists in fixtures namespace
        response = self.client.get(f'/{self.fixtures_namespace}/exists/id/{fixture_id}')
        assert response.status_code == 200
        assert response.json()["exists"] is True

        # Check doesn't exist in test namespace
        response = self.client.get(f'/{self.test_namespace}/exists/id/{fixture_id}')
        assert response.status_code == 200
        assert response.json()["exists"] is False

    def test__exists__cache_id__with_all_fixture_types(self):                         # Test exists with all fixture types
        skip__if_not__in_github_actions()
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
            fixture_id = self.cache_fixtures.get_fixture_id(fixture_name)
            response   = self.client.get(f'/{self.fixtures_namespace}/exists/id/{fixture_id}')
            assert response.status_code == 200
            assert response.json()["exists"] is True

    # ═══════════════════════════════════════════════════════════════════════════
    # exists__hash__cache_hash tests
    # ═══════════════════════════════════════════════════════════════════════════

    def test__exists__hash__using_fixture(self):                                      # Test checking existing hash from fixtures
        fixture_hash = self.cache_fixtures.get_fixture_hash("string_simple")
        response     = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result == { "exists"     : True                         ,
                           "cache_id"   : None                         ,
                           "cache_hash" : fixture_hash                 ,
                           "namespace"  : str(self.fixtures_namespace) }

    def test__exists__hash__non_existent(self):                                       # Test checking non-existent hash
        non_existent = "0000000000000000"
        response     = self.client.get(f'/{self.test_namespace}/exists/hash/{non_existent}')

        assert response.status_code == 200
        result = response.json()
        assert result == { "exists"     : False              ,
                           "cache_id"   : None               ,
                           "cache_hash" : non_existent       ,
                           "namespace"  : self.test_namespace}

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
            response = self.client.get(f'/{self.test_namespace}/exists/hash/{test_hash}')
            assert response.status_code == 200
            result = response.json()
            assert result["exists"    ] is False                                  # None should exist in test namespace
            assert result["namespace" ] == self.test_namespace
            assert result["cache_hash"] == test_hash
            assert result["cache_id"  ] is None

        # Test invalid hash format
        not_an_hash = "test-hash-with-dash"
        expected_error = {'detail': [{'input': 'test-hash-with-dash',
                                      'loc': ['query', 'cache_hash'],
                                      'msg': 'in Safe_Str__Cache_Hash, value does not match required '
                                             'pattern: ^[a-f0-9]{10,96}$',
                                      'type': 'value_error'}]}
        assert self.client.get(f'/{self.test_namespace}/exists/hash/{not_an_hash}').json() == expected_error

    def test__exists__with_all_fixture_types(self):                                   # Test exists with all fixture types
        skip__if_not__in_github_actions()
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
            fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)
            response = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')
            assert response.status_code == 200
            assert response.json()["exists"] is True

    def test__exists__with_special_namespace(self):                                   # Test special characters in namespace
        special_ns = "test-namespace__!@**__"                                         # Namespace with special characters (will be sanitized)
        test_hash  = "abc0123456789def"

        response = self.client.get(f'/{special_ns}/exists/hash/{test_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result["exists"    ] is False
        assert result["cache_hash"]   == test_hash
        assert result["cache_id"  ] is None
        # Namespace should be sanitized
        assert result["namespace" ] != special_ns                                      # Changed due to sanitization
        assert result["namespace" ] == 'test-namespace________'

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
        skip__if_not__in_github_actions()
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

    # ═══════════════════════════════════════════════════════════════════════════
    # Combined cache_id and hash tests
    # ═══════════════════════════════════════════════════════════════════════════

    def test__exists__both_id_and_hash__same_fixture(self):                           # Test both endpoints return consistent results
        fixture_name = "json_complex"
        fixture_id   = self.cache_fixtures.get_fixture_id(fixture_name)
        fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)

        # Both should report exists=True
        response_id   = self.client.get(f'/{self.fixtures_namespace}/exists/id/{fixture_id}')
        response_hash = self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')

        assert response_id.status_code   == 200
        assert response_hash.status_code == 200
        assert response_id.json()["exists"]   is True
        assert response_hash.json()["exists"] is True

    def test__exists__performance_id_vs_hash(self):                                   # Compare performance of id vs hash lookups
        import time

        fixture_names = ["string_simple", "json_simple", "binary_small"]

        # Time ID lookups
        start_id = time.time()
        for name in fixture_names:
            fixture_id = self.cache_fixtures.get_fixture_id(name)
            self.client.get(f'/{self.fixtures_namespace}/exists/{fixture_id}')
        elapsed_id = time.time() - start_id

        # Time hash lookups
        start_hash = time.time()
        for name in fixture_names:
            fixture_hash = self.cache_fixtures.get_fixture_hash(name)
            self.client.get(f'/{self.fixtures_namespace}/exists/hash/{fixture_hash}')
        elapsed_hash = time.time() - start_hash

        # Both should be fast
        assert elapsed_id   < 1.0
        assert elapsed_hash < 1.0