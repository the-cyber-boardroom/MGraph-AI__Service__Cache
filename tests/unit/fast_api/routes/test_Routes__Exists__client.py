from unittest                                                           import TestCase
from osbot_aws.AWS_Config                                               import aws_config
from osbot_aws.testing.Temp__Random__AWS_Credentials                    import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash        import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator       import Cache__Hash__Generator
from tests.unit.Service__Fast_API__Test_Objs                            import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Exists__client(TestCase):                                # Test exists routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME expensive setup
        with setup__service_fast_api_test_objs() as _:
            cls.client = _.fast_api__client                                 # Reuse TestClient
            cls.app    = _.fast_api__app                                    # Reuse FastAPI app
            cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_namespace = "test-exists-client"
        cls.test_string    = "test exists data"
        cls.test_hash      = Safe_Str__Cache_Hash("0000000000000000")      # Known non-existent hash

    def setUp(self):                                                        # PER-TEST lightweight setup
        # Store data for tests that need existing hash
        self.stored_data  = "client test data"
        response_store    = self.client.post(f'{self.test_namespace}/direct/store/string/',
                                            content = self.stored_data                          ,
                                            headers = {"Content-Type": "text/plain"}            )
        assert response_store.status_code == 200
        result           = response_store.json()
        self.stored_hash = result['hash']
        self.stored_id   = result['cache_id']

    def tearDown(self):                                                     # PER-TEST cleanup
        # Clean up stored test data
        response_delete = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}/')
        assert response_delete.status_code == 200

    def test__health_check(self):                                           # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__exists__hash__existing(self):                                 # Test checking existing hash
        response = self.client.get(f'/{self.test_namespace}/exists/hash/{self.stored_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists"    : True                   ,
                         "hash"      : self.stored_hash        ,
                         "namespace" : self.test_namespace     }

    def test__exists__hash__non_existent(self):                            # Test checking non-existent hash
        non_existent = "0000000000000000"
        response     = self.client.get(f'/{self.test_namespace}/exists/hash/{non_existent}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists"    : False                  ,
                         "hash"      : non_existent            ,
                         "namespace" : self.test_namespace     }

    def test__exists__hash__namespace_isolation(self):                      # Test namespace isolation
        other_namespace = "other-namespace"

        # Check stored hash doesn't exist in other namespace
        response = self.client.get(f'/{other_namespace}/exists/hash/{self.stored_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists"    : False                  ,
                         "hash"      : self.stored_hash        ,
                         "namespace" : other_namespace         }

    def test__exists__hash__multiple_formats(self):                         # Test different hash formats
        # Test with different hash lengths and formats
        test_hashes = [
            "abc123def456789"      ,  # 15 chars
            "0123456789abcdef"     ,  # 16 chars (standard)
            "a" * 32               ,  # 32 chars
            #"test-hash-with-dash"  ,  # With special chars                 # these will fail validation
            #""                        # Empty hash                         # this can't be called due to routing restrictions
        ]

        for test_hash in test_hashes:
            with self.subTest(hash=test_hash):
                response = self.client.get(f'/{self.test_namespace}/exists/hash/{test_hash}')
                assert response.status_code == 200
                result = response.json()
                assert result["exists"]    is False                         # None should exist
                assert result["namespace"] == self.test_namespace
                assert result["hash"] == test_hash

        not_an_hash = "test-hash-with-dash"
        expected_error = {'detail': [{'input': 'test-hash-with-dash',
                                      'loc': ['query', 'cache_hash'],
                                      'msg': 'in Safe_Str__Cache_Hash, value does not match required '
                                             'pattern: ^[a-f0-9]{10,96}$',
                                      'type': 'value_error'}]}
        assert self.client.get(f'/{self.test_namespace}/exists/hash/{not_an_hash}').json() == expected_error

    def test__exists__workflow_complete(self):                              # Test complete workflow
        namespace = "workflow-exists"
        test_data = "workflow test data!"

        # 1. Store data
        response_store = self.client.post(url     = f'/{namespace}/temporal/store/string',
                                          content = test_data                         ,
                                          headers = {"Content-Type": "text/plain"}    )
        assert response_store.status_code == 200
        store_result = response_store.json()
        cache_id     = store_result['cache_id']
        cache_hash   = store_result['hash']

        # 2. Check exists via exists endpoint
        response_exists = self.client.get(f'/{namespace}/exists/hash/{cache_hash}')
        assert response_exists.status_code == 200
        exists_result = response_exists.json()
        assert exists_result == {"exists"    : True      ,
                                "hash"      : cache_hash ,
                                "namespace" : namespace  }

        # 3. Verify via cache/exists endpoint (alternate route)
        response_cache_exists = self.client.get(f'/{namespace}/exists/hash/{cache_hash}/')
        assert response_cache_exists.status_code == 200
        cache_exists_result = response_cache_exists.json()
        assert cache_exists_result["exists"] is True

        # 4. Delete
        response_delete = self.client.delete(f'/{namespace}/delete/{cache_id}/')
        assert response_delete.status_code == 200
        delete_result = response_delete.json()
        assert delete_result['status'] == 'success'

        # 5. Verify no longer exists
        response_final = self.client.get(f'/{namespace}/exists/hash/{cache_hash}')
        assert response_final.status_code == 200
        final_result = response_final.json()
        assert final_result == {"exists"    : False     ,
                                "hash"      : cache_hash ,
                                "namespace" : namespace  }

    def test__exists__with_special_namespace(self):                         # Test special characters in namespace
        # Namespace with special characters (will be sanitized)
        special_ns = "test-namespace__!@**__"
        test_hash  = "abc0123456789def"

        response = self.client.get(f'/{special_ns}/exists/hash/{test_hash}')

        assert response.status_code == 200
        result = response.json()
        assert result["exists"] is False
        assert result["hash"]   == test_hash
        # Namespace should be sanitized
        assert result["namespace"] != special_ns                            # Changed due to sanitization
        assert result["namespace"] == 'test-namespace________'

    def test__exists__auth_required(self):                                  # Test authentication requirement
        # Remove auth header temporarily
        auth_header = self.client.headers.pop(TEST_API_KEY__NAME)

        response = self.client.get(f'/{self.test_namespace}/exists/hash/{self.test_hash}')
        assert response.status_code == 401

        self.client.headers[TEST_API_KEY__NAME] = auth_header               # Restore auth header

    def test__exists__performance_check(self):                              # Test multiple rapid requests
        import time

        # Store multiple items
        hashes = []
        for i in range(5):
            response = self.client.post(url     = f'/{self.test_namespace}/direct/store/string',
                                        content = f"perf test {i}"                          ,
                                        headers = {"Content-Type": "text/plain"}            )
            assert response.status_code == 200
            hashes.append(response.json()['hash'])

        # Check all exist rapidly
        start_time = time.time()
        for hash_val in hashes:
            response = self.client.get(f'/{self.test_namespace}/exists/hash/{hash_val}')
            assert response.status_code        == 200
            assert response.json()["exists"] is True

        elapsed = time.time() - start_time
        assert elapsed < 1.0                                                # Should be fast (< 1 second for 5 checks)


    def test__exists__case_sensitivity(self):                               # Test hash case sensitivity
        # Store with lowercase hash
        test_data      = "case sensitivity test"
        response_store = self.client.post(url     = f'/{self.test_namespace}/direct/store/string',
                                          content = test_data                                 ,
                                          headers = {"Content-Type": "text/plain"}            )
        assert response_store.status_code == 200
        lowercase_hash = response_store.json()['hash']

        # Check with different cases
        uppercase_hash = lowercase_hash.upper()
        mixedcase_hash = lowercase_hash[:8] + lowercase_hash[8:].upper()

        # Original case should exist
        response_lower = self.client.get(f'/{self.test_namespace}/exists/hash/{lowercase_hash}')
        assert response_lower.json()["exists"] is True

        # Different cases should not exist (hashes are case-sensitive)

        response_upper = self.client.get(f'/{self.test_namespace}/exists/hash/{uppercase_hash}')
        assert response_upper.json() == { 'detail': [ { 'input': '44159F63CBF3417D',
                                                        'loc': ['query', 'cache_hash'],
                                                        'msg': 'in Safe_Str__Cache_Hash, value does not match required '
                                                               'pattern: ^[a-f0-9]{10,96}$',
                                                        'type': 'value_error'}]}




        response_mixed = self.client.get(f'/{self.test_namespace}/exists/hash/{mixedcase_hash}')
        assert response_mixed.json() == {'detail': [{'input': '44159f63CBF3417D',
                                                     'loc': ['query', 'cache_hash'],
                                                     'msg': 'in Safe_Str__Cache_Hash, value does not match required '
                                                            'pattern: ^[a-f0-9]{10,96}$',
                                                     'type': 'value_error'}]}

    def test__exists__concurrent_checks(self):                              # Test concurrent existence checks
        import concurrent.futures

        def check_exists(index):                                                            # Check if a hash exists
            test_hash = Cache__Hash__Generator().from_string("concurrent{index:04d}hash")
            response  = self.client.get(f'/{self.test_namespace}/exists/hash/{test_hash}')

            if response.status_code == 200:
                return response.json()["exists"]
            return None

        # Run concurrent checks
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(check_exists, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All should complete successfully
        assert all(result is False for result in results)                   # None exist

    def test__exists__with_all_strategies(self):                            # Test exists works with all storage strategies
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]
        namespace  = "strategy-test"

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                # Store with strategy
                test_data      = f"strategy {strategy} data"
                response_store = self.client.post(url     = f'{namespace}/{strategy}/store/string',
                                                  content = test_data                            ,
                                                  headers = {"Content-Type": "text/plain"}       )
                assert response_store.status_code == 200
                cache_hash = response_store.json()['hash']

                # Check exists
                response_exists = self.client.get(f'/{namespace}/exists/hash/{cache_hash}')
                assert response_exists.status_code == 200
                result = response_exists.json()
                assert result["exists"]    is True
                assert result["namespace"] == namespace