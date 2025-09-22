from unittest                                                                       import TestCase
from osbot_fast_api.api.routes.Fast_API__Routes                                     import Fast_API__Routes
from osbot_fast_api_serverless.utils.testing.skip_tests import skip__if_not__in_github_actions
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.utils.Objects                                                      import base_classes
from mgraph_ai_service_cache.fast_api.routes.Routes__Delete                         import Routes__Delete, TAG__ROUTES_DELETE, PREFIX__ROUTES_DELETE, BASE_PATH__ROUTES_DELETE, ROUTES_PATHS__DELETE
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs


class test_Routes__Delete(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service_fast_api_test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.routes             = Routes__Delete(cache_service=cls.cache_service)

        cls.test_namespace     = Safe_Str__Id("test-delete-routes")                     # Use different namespace for deletable test data

        cls.created_cache_ids  = []                                                     # Track items we create for deletion testing

    @classmethod
    def tearDownClass(cls):                                                             # Clean up only what we created
        for cache_id in cls.created_cache_ids:
            try:
                cls.cache_service.delete_by_id(cache_id, cls.test_namespace)
            except:
                pass

    def _create_deletable_item(self, data="deletable data", strategy="direct"):         # Helper to create items for deletion
        cache_hash = self.cache_service.hash_from_string(data)
        cache_id   = Random_Guid()

        self.cache_service.store_with_strategy(storage_data = data              ,
                                               cache_hash   = cache_hash        ,
                                               cache_id     = cache_id          ,
                                               strategy     = strategy          ,
                                               namespace    = self.test_namespace)

        self.created_cache_ids.append(cache_id)
        return cache_id

    def test__init__(self):                                                           # Test initialization
        with Routes__Delete() as _:
            assert type(_)               is Routes__Delete
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_DELETE
            assert _.prefix              == PREFIX__ROUTES_DELETE
            assert type(_.cache_service) is Cache__Service

    def test__class_constants(self):                                                  # Test module-level constants
        assert TAG__ROUTES_DELETE       == 'delete'
        assert PREFIX__ROUTES_DELETE    == '/{namespace}'
        assert BASE_PATH__ROUTES_DELETE == '/{namespace}/delete/'
        assert ROUTES_PATHS__DELETE     == ['/{namespace}/delete/{cache_id}']

    def test_delete__cache_id(self):                                                  # Test delete by cache ID
        with self.routes as _:
            cache_id = self._create_deletable_item("data to delete")

            # Verify it exists
            retrieve_result = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieve_result is not None

            # Delete it
            result = _.delete__cache_id(cache_id, self.test_namespace)

            assert result["status"]        == "success"
            assert result["cache_id"]      == cache_id
            assert result["deleted_count"] > 0
            assert result["failed_count"]  == 0

            # Verify it's deleted
            retrieve_after = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieve_after is None

            # Remove from tracking since deleted
            self.created_cache_ids.remove(cache_id)

    def test_delete__cache_id__not_found(self):                                       # Test deleting non-existent entry
        with self.routes as _:
            non_existent_id = Random_Guid()

            result = _.delete__cache_id(non_existent_id, self.test_namespace)

            assert result["status"]  == "not_found"
            assert result["message"] == f"Cache ID {non_existent_id} not found"

    def test_delete__all_strategies(self):                                            # Test delete works with all storage strategies
        skip__if_not__in_github_actions()
        with self.routes as _:
            strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

            for strategy in strategies:
                with self.subTest(strategy=strategy):
                    cache_id = self._create_deletable_item(f"delete test {strategy}", strategy)

                    result = _.delete__cache_id(cache_id, self.test_namespace)

                    assert result["status"]        == "success"
                    assert result["deleted_count"] > 0

                    self.created_cache_ids.remove(cache_id)