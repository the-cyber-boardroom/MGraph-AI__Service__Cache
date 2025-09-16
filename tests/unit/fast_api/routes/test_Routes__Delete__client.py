from unittest                                                           import TestCase
from osbot_aws.AWS_Config                                               import aws_config
from osbot_aws.testing.Temp__Random__AWS_Credentials                    import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid   import Random_Guid
from osbot_utils.utils.Misc                                             import is_guid
from tests.unit.Service__Fast_API__Test_Objs                            import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Delete__client(TestCase):                                # Test delete routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME expensive setup
        with setup__service_fast_api_test_objs() as _:
            cls.client = _.fast_api__client                                 # Reuse TestClient
            cls.app    = _.fast_api__app                                    # Reuse FastAPI app
            cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_namespace = "test-delete-client"
        cls.test_string    = "test delete data"

    def setUp(self):                                                        # PER-TEST lightweight setup
        # Store data for tests that need something to delete
        self.stored_data  = "client test data to delete"
        response_store    = self.client.post(f'/{self.test_namespace}/direct/store/string',
                                            json = self.stored_data                       )
        assert response_store.status_code == 200
        result         = response_store.json()
        self.stored_id = result['cache_id']

    def tearDown(self):                                                     # PER-TEST cleanup
        # Try to clean up if not already deleted
        response_delete = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}')
        # Don't assert here as test may have already deleted it

    def test__health_check(self):                                           # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__delete__basic(self):                                          # Test basic delete operation
        # Verify exists before delete
        response_retrieve = self.client.get(f'/{self.test_namespace}/retrieve/{self.stored_id}')
        assert response_retrieve.status_code == 200
        assert response_retrieve.json()['data'] == self.stored_data

        # Delete
        response = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']        == 'success'
        assert result['cache_id']      == self.stored_id
        assert result['deleted_count'] > 0
        assert result['failed_count']  == 0
        assert len(result['deleted_paths']) > 0

        # Verify deleted
        response_after = self.client.get(f'/{self.test_namespace}/retrieve/{self.stored_id}')
        assert response_after.status_code == 200
        assert response_after.json()      == {"status": "not_found", "message": "Cache entry not found"}

    def test__delete__not_found(self):                                      # Test deleting non-existent entry
        non_existent_id = Random_Guid()
        response        = self.client.delete(f'/{self.test_namespace}/delete/{non_existent_id}')

        assert response.status_code == 200
        result = response.json()
        assert result == {'status'  : 'not_found'                              ,
                         'message' : f'Cache ID {non_existent_id} not found'  }

    def test__delete__namespace_isolation(self):                            # Test namespace isolation
        other_namespace = "other-delete-namespace"

        # Try to delete from wrong namespace
        response = self.client.delete(f'/{other_namespace}/delete/{self.stored_id}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']  == 'not_found'
        assert result['message'] == f'Cache ID {self.stored_id} not found'

        # Original should still exist in correct namespace
        response_check = self.client.get(f'/{self.test_namespace}/retrieve/{self.stored_id}')
        assert response_check.status_code == 200
        assert response_check.json()['data'] == self.stored_data

    def test__delete__workflow_complete(self):                              # Test complete workflow
        namespace = "workflow-delete"
        test_data = "workflow delete test data"

        # 1. Store data using Routes__Store
        response_store = self.client.post(f'/{namespace}/temporal/store/string',
                                         json = test_data                      )
        assert response_store.status_code == 200
        store_result = response_store.json()
        cache_id     = store_result['cache_id']
        cache_hash   = store_result['hash']

        assert is_guid(cache_id)
        assert len(cache_hash) == 16

        # 2. Verify exists via Routes__Retrieve
        response_retrieve = self.client.get(f'/{namespace}/retrieve/{cache_id}')
        assert response_retrieve.status_code == 200
        assert response_retrieve.json()['data'] == test_data

        # 3. Delete via Routes__Delete
        response_delete = self.client.delete(f'/{namespace}/delete/{cache_id}')
        assert response_delete.status_code == 200
        delete_result = response_delete.json()
        assert delete_result == {'cache_id'      : cache_id                          ,
                                'deleted_count' : 9                                  ,  # Standard count for temporal
                                'deleted_paths' : delete_result['deleted_paths']     ,
                                'failed_count'  : 0                                  ,
                                'failed_paths'  : []                                 ,
                                'status'        : 'success'                          }

        # 4. Verify deleted
        response_final = self.client.get(f'/{namespace}/retrieve/{cache_id}')
        assert response_final.status_code == 200
        assert response_final.json()      == {"status": "not_found", "message": "Cache entry not found"}

    def test__delete__all_strategies(self):                                 # Test delete works with all storage strategies
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]
        namespace  = "strategy-delete"

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                # Store with strategy
                test_data      = f"delete strategy {strategy} data"
                response_store = self.client.post(f'/{namespace}/{strategy}/store/string',
                                                 json = test_data                        )
                assert response_store.status_code == 200
                cache_id = response_store.json()['cache_id']

                # Verify stored
                response_check = self.client.get(f'/{namespace}/retrieve/{cache_id}')
                assert response_check.status_code == 200
                assert response_check.json()['data'] == test_data

                # Delete
                response_delete = self.client.delete(f'/{namespace}/delete/{cache_id}')
                assert response_delete.status_code == 200
                result = response_delete.json()
                assert result['status']        == 'success'
                assert result['deleted_count'] > 8                         # All strategies create at least 9 files

                # Verify deleted
                response_after = self.client.get(f'/{namespace}/retrieve/{cache_id}')
                assert response_after.json()['status'] == 'not_found'

    def test__delete__multiple_times(self):                                 # Test deleting same ID multiple times
        # First delete should succeed
        response1 = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}')
        assert response1.status_code == 200
        result1 = response1.json()
        assert result1['status'] == 'success'

        # Second delete should not find it
        response2 = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}')
        assert response2.status_code == 200
        result2 = response2.json()
        assert result2['status']  == 'not_found'
        assert result2['message'] == f'Cache ID {self.stored_id} not found'

        # Third delete should also not find it
        response3 = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}')
        assert response3.status_code == 200
        assert response3.json() == result2                                  # Same as second

    def test__delete__auth_required(self):                                  # Test authentication requirement
        auth_header = self.client.headers.pop(TEST_API_KEY__NAME)                       # Remove auth header temporarily
        response = self.client.delete(f'/{self.test_namespace}/delete/{self.stored_id}')
        assert response.status_code == 401
        self.client.headers[TEST_API_KEY__NAME] = auth_header                       # Restore auth header

    def test__delete__binary_data(self):                                    # Test deleting binary data
        namespace   = "delete-binary"
        binary_data = b'\x00\x01\x02\x03\x04\x05'

        # Store binary
        response_store = self.client.post(f'/{namespace}/direct/store/binary',
                                         content = binary_data                ,
                                         headers = {"Content-Type": "application/octet-stream"})
        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Delete binary
        response_delete = self.client.delete(f'/{namespace}/delete/{cache_id}')
        assert response_delete.status_code == 200
        result = response_delete.json()
        assert result['status']        == 'success'
        assert result['deleted_count'] == 9

    def test__delete__json_data(self):                                      # Test deleting JSON data
        namespace = "delete-json"
        json_data = {"test": "delete", "value": 123, "nested": {"key": "value"}}

        # Store JSON
        response_store = self.client.post(f'/{namespace}/temporal_latest/store/json',
                                         json = json_data                           )
        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Delete JSON
        response_delete = self.client.delete(f'/{namespace}/delete/{cache_id}')
        assert response_delete.status_code == 200
        result = response_delete.json()
        assert result['status']        == 'success'
        assert result['deleted_count'] > 8

    def test__delete__concurrent_operations(self):                          # Test concurrent delete operations
        import concurrent.futures

        # Create multiple items to delete
        items = []
        for i in range(5):
            response = self.client.post(f'/{self.test_namespace}/direct/store/string',
                                       json = f"concurrent delete {i}"               )
            assert response.status_code == 200
            items.append(response.json()['cache_id'])

        def delete_item(cache_id):
            """Delete a cache item"""
            response = self.client.delete(f'/{self.test_namespace}/delete/{cache_id}')
            if response.status_code == 200:
                return response.json()['status']
            return 'error'

        # Run concurrent deletes
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(delete_item, cache_id) for cache_id in items]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All should succeed
        assert all(status == 'success' for status in results)

    def test__delete__performance_batch(self):                              # Test performance with batch deletes
        import time

        # Create items to delete
        items = []
        for i in range(5):
            response = self.client.post(f'/{self.test_namespace}/direct/store/string',
                                       json = f"perf delete {i}"                     )
            assert response.status_code == 200
            items.append(response.json()['cache_id'])

        # Delete all rapidly
        start_time = time.time()

        for cache_id in items:
            response = self.client.delete(f'/{self.test_namespace}/delete/{cache_id}')
            assert response.status_code        == 200
            assert response.json()['status'] == 'success'

        elapsed = time.time() - start_time
        assert elapsed < 1.0                                                # Should be fast (< 2 seconds for 5 deletes)

    def test__delete__special_namespace_characters(self):                   # Test special characters in namespace
        # Namespace with special characters (should be sanitized)
        special_ns = "delete-namespace!*&"

        # Store in special namespace
        response_store = self.client.post(url  = f'/{special_ns}/direct/store/string',
                                          json = "special namespace data"           )
        assert response_store.status_code == 200

        cache_id = response_store.json()['cache_id']
        response_delete = self.client.delete(f'/{special_ns}/delete/{cache_id}')
        assert response_delete.status_code == 200
        assert response_delete.json().get('deleted_count') == 9

    def test__delete__integration_with_all_routes(self):                    # Test delete integrates with other routes
        namespace = "integration"
        test_data = {"key": "value", "test": True}

        # Store via Routes__Store
        response_store = self.client.post(f'/{namespace}/temporal_versioned/store/json',
                                         json = test_data                              )
        assert response_store.status_code == 200
        cache_id   = response_store.json()['cache_id']
        cache_hash = response_store.json()['hash']

        # Check exists via Routes__Exists
        response_exists = self.client.get(f'/{namespace}/exists/hash/{cache_hash}')
        assert response_exists.status_code == 200
        assert response_exists.json()['exists'] is True

        # Retrieve via Routes__Retrieve
        response_retrieve = self.client.get(f'/{namespace}/retrieve/{cache_id}')
        assert response_retrieve.status_code == 200
        assert response_retrieve.json()['data'] == test_data

        # Delete via Routes__Delete
        response_delete = self.client.delete(f'/{namespace}/delete/{cache_id}')
        assert response_delete.status_code == 200
        assert response_delete.json()['status'] == 'success'

        # Verify gone via all routes
        response_exists_after = self.client.get(f'/{namespace}/exists/hash/{cache_hash}')
        assert response_exists_after.json()['exists'] is False

        response_retrieve_after = self.client.get(f'/{namespace}/retrieve/{cache_id}')
        assert response_retrieve_after.json()['status'] == 'not_found'