from unittest                                                                import TestCase
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Success  import Schema__Cache__Retrieve__Success
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve import Cache__Service__Retrieve
from osbot_fast_api_serverless.utils.testing.skip_tests                      import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                  import __, __SKIP__
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid        import Random_Guid
from osbot_utils.utils.Objects                                               import obj
from tests.unit.Service__Cache__Test_Objs                                    import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__File__Delete__client(TestCase):                                          # Test delete routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs      = setup__service__cache__test_objs()
        cls.cache_fixtures = cls.test_objs.cache_fixtures
        cls.client         = cls.test_objs.fast_api__client
        cls.app            = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        cls.fixtures_namespace = cls.cache_fixtures.namespace                           # Use fixtures namespace for reading, separate namespace for deletion
        cls.test_namespace     = "test-delete-client"

        # Track created items
        cls.created_items = []

    @classmethod
    def tearDownClass(cls):                                                           # Clean up created items
        for item in cls.created_items:
            try:
                cls.client.delete(f'/{item["namespace"]}/delete/{item["cache_id"]}')
            except:
                pass

    def _store_for_deletion(self, data="test data", namespace=None, strategy="direct"):  # Helper to create deletable items
        namespace = namespace or self.test_namespace
        response  = self.client.post(f'/{namespace}/{strategy}/store/string', json=data)
        assert response.status_code == 200

        cache_id = response.json()['cache_id']
        self.created_items.append({'cache_id': cache_id, 'namespace': namespace})
        return cache_id

    def test__health_check(self):                                                     # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__delete__basic(self):                                                    # Test basic delete operation
        cache_id = self._store_for_deletion("client test data to delete")

        with Cache__Service__Retrieve(cache_service=self.test_objs.cache_service) as _:
            assert type(_.retrieve_by_id(cache_id, namespace=self.test_namespace)) is Schema__Cache__Retrieve__Success
            assert _.retrieve_by_id(cache_id, namespace=self.fixtures_namespace) is None                               # BUG

        response_retrieve = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}')              # Verify exists before delete
        # assert response_retrieve.status_code == 404
        # assert obj(response_retrieve.json()) == __(detail = __(cache_hash     = None                                  ,
        #                                                        cache_id       = cache_id                              ,
        #                                                        error_type     = 'NOT_FOUND'                           ,
        #                                                        message        = 'The requested cache entry was not found',
        #                                                        namespace      = 'test-delete-client'                  ,
        #                                                        request_id     = __SKIP__                              ,
        #                                                        resource_id    = None                                  ,
        #                                                        resource_type  = 'cache_entry'                         ,
        #                                                        timestamp      = __SKIP__                              ))
        #
        # return
        assert response_retrieve.status_code == 200


        response = self.client.delete(f'/{self.test_namespace}/delete/{cache_id}')                      # Delete
        assert response.status_code == 200

        result = response.json()

        assert result['status']        == 'success'
        assert result['cache_id']      == cache_id
        assert result['deleted_count'] > 0

        # Verify deleted
        response_after = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}')
        assert obj(response_after.json()) == __(detail=__(resource_id       = None                ,         # this is the output of HTTPException(status_code=404, detail=error.json())
                                                          cache_hash        = None                ,
                                                          cache_id          = cache_id            ,
                                                          namespace         = 'test-delete-client',
                                                          error_type        = 'NOT_FOUND'         ,
                                                          message           = 'The requested cache entry was not found',
                                                          timestamp         = __SKIP__                                 ,
                                                          request_id        = __SKIP__,
                                                          resource_type     ='cache_entry'))

        # Remove from tracking
        self.created_items = [i for i in self.created_items if i['cache_id'] != cache_id]

    def test__delete__not_found(self):                                                # Test deleting non-existent entry
        non_existent_id = Random_Guid()
        response        = self.client.delete(f'/{self.test_namespace}/delete/{non_existent_id}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']  == 'not_found'
        assert result['message'] == f'Cache ID {non_existent_id} not found'

    def test__delete__all_strategies(self):                                           # Test delete works with all storage strategies
        skip__if_not__in_github_actions()
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                cache_id = self._store_for_deletion(f"delete {strategy}", strategy=strategy)

                response = self.client.delete(f'/{self.test_namespace}/delete/{cache_id}')
                assert response.status_code == 200
                assert response.json()['status'] == 'success'

                self.created_items = [i for i in self.created_items if i['cache_id'] != cache_id]

    def test__delete__concurrent_operations(self):                                    # Test concurrent delete operations
        skip__if_not__in_github_actions()
        import concurrent.futures

        # Create multiple items
        cache_ids = [self._store_for_deletion(f"concurrent {i}") for i in range(5)]

        def delete_item(cache_id):
            response = self.client.delete(f'/{self.test_namespace}/delete/{cache_id}')
            return response.json()['status'] if response.status_code == 200 else 'error'

        # Run concurrent deletes
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(delete_item, cid) for cid in cache_ids]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        assert all(status == 'success' for status in results)

        # Clear from tracking
        self.created_items = [i for i in self.created_items if i['cache_id'] not in cache_ids]