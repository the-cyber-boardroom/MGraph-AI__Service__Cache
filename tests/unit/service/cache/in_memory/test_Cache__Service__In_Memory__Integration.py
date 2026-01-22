from unittest                                                                  import TestCase
from mgraph_ai_service_cache.service.cache.in_memory.Cache__Service__In_Memory import cache_client__in_memory


class test_Cache__Service__In_Memory__Integration(TestCase):

    @classmethod
    def setUpClass(cls):                                                        # Setup once for all tests
        cls.cache_client = cache_client__in_memory()

    def test__health_check(self):                                               # Test health endpoint via client
        health = self.cache_client.info().health()
        assert health == {'status': 'ok'}

    def test__store_and_retrieve(self):                                         # Test actual cache operations
        namespace = 'test-namespace'
        cache_key = 'test-key'
        test_data = {"test"  : "data"         ,
                     'key' : 'this/is/the-key'}

        result = self.cache_client.store().store__json__cache_key(namespace       = namespace ,
                                                                  strategy = 'direct',
                                                                  cache_key       = cache_key ,
                                                                  body            = test_data ,
                                                                  file_id         = 'test'    ,
                                                                  json_field_path = 'key'     )

        assert result is not None                                               # Store succeeded

        retrieved = self.cache_client.retrieve().retrieve__cache_id(namespace = namespace       ,
                                                                    cache_id  = result.cache_id )

        assert retrieved is not None                                            # Retrieve succeeded
