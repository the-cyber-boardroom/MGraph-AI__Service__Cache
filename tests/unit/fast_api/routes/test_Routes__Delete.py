from unittest                                                           import TestCase
from osbot_aws.AWS_Config                                               import aws_config
from osbot_aws.testing.Temp__Random__AWS_Credentials                    import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                   import str_to_valid_s3_bucket_name
from osbot_fast_api.api.routes.Fast_API__Routes                         import Fast_API__Routes
from osbot_utils.type_safe.Type_Safe                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid   import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id       import Safe_Id
from osbot_utils.utils.Misc                                             import random_string_short
from osbot_utils.utils.Objects                                          import base_classes, __
from mgraph_ai_service_cache.fast_api.routes.Routes__Delete             import Routes__Delete, TAG__ROUTES_DELETE, PREFIX__ROUTES_DELETE, BASE_PATH__ROUTES_DELETE, ROUTES_PATHS__DELETE
from mgraph_ai_service_cache.service.cache.Cache__Service               import Cache__Service
from tests.unit.Service__Fast_API__Test_Objs                            import setup__service_fast_api_test_objs


class test_Routes__Delete(TestCase):

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-delete-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service  = Cache__Service(default_bucket=cls.test_bucket)
        cls.routes         = Routes__Delete(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace = Safe_Id("test-delete")
        cls.test_string    = "test delete data"

    @classmethod
    def tearDownClass(cls):                                                 # ONE-TIME cleanup
        for handler in cls.routes.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    def test__init__(self):                                                 # Test initialization
        with Routes__Delete() as _:
            assert type(_)               is Routes__Delete
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_DELETE
            assert _.prefix              == PREFIX__ROUTES_DELETE
            assert type(_.cache_service) is Cache__Service

    def test__class_constants(self):                                        # Test module-level constants
        assert TAG__ROUTES_DELETE       == 'delete'
        assert PREFIX__ROUTES_DELETE    == '/{namespace}'
        assert BASE_PATH__ROUTES_DELETE == '/{namespace}/delete/'
        assert ROUTES_PATHS__DELETE     == ['/{namespace}/delete/{cache_id}']

    def test_delete__cache_id(self):                                        # Test delete by cache ID
        with self.routes as _:
            # Store data first to have something to delete
            test_data  = "data to delete"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id   = Random_Guid()

            # Store the data
            store_result = self.cache_service.store_with_strategy(storage_data   = test_data         ,
                                                                  cache_hash     = cache_hash        ,
                                                                  cache_id       = cache_id          ,
                                                                  strategy       = "direct"          ,
                                                                  namespace      = self.test_namespace)

            assert store_result.cache_id == cache_id

            # Verify it exists
            retrieve_result = self.cache_service.retrieve_by_id(cache_id = cache_id        ,
                                                               namespace = self.test_namespace)
            assert retrieve_result is not None
            assert retrieve_result["data"] == test_data

            # Delete it
            result = _.delete__cache_id(cache_id = cache_id        ,
                                       namespace = self.test_namespace)

            assert result["status"]        == "success"
            assert result["cache_id"]      == cache_id
            assert result["deleted_count"] > 0
            assert result["failed_count"]  == 0
            assert len(result["deleted_paths"]) > 0

            # Verify it's deleted
            retrieve_after = self.cache_service.retrieve_by_id(cache_id = cache_id        ,
                                                              namespace = self.test_namespace)
            assert retrieve_after is None

    def test_delete__cache_id__default_namespace(self):                     # Test default namespace handling
        with self.routes as _:
            # Store data in default namespace
            test_data  = "default namespace data"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id   = Random_Guid()

            self.cache_service.store_with_strategy(storage_data   = test_data  ,
                                                   cache_hash     = cache_hash ,
                                                   cache_id       = cache_id   ,
                                                   strategy       = "direct"   ,
                                                   namespace      = None       )  # Default namespace

            # Delete with None namespace (should use default)
            result = _.delete__cache_id(cache_id = cache_id,
                                       namespace = None    )

            assert result["status"]        == "success"
            assert result["cache_id"]      == cache_id
            assert result["deleted_count"] > 0

    def test_delete__cache_id__not_found(self):                             # Test deleting non-existent entry
        with self.routes as _:
            non_existent_id = Random_Guid()

            result = _.delete__cache_id(cache_id = non_existent_id      ,
                                       namespace = self.test_namespace   )

            assert result["status"]  == "not_found"
            assert result["message"] == f"Cache ID {non_existent_id} not found"

    def test_delete__cache_id__namespace_isolation(self):                   # Test namespace isolation
        with self.routes as _:
            ns1 = Safe_Id("delete-ns1")
            ns2 = Safe_Id("delete-ns2")

            # Store same data in different namespaces
            test_data  = "namespace isolation test"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id1  = Random_Guid()
            cache_id2  = Random_Guid()

            # Store in ns1
            self.cache_service.store_with_strategy(storage_data   = test_data  ,
                                                   cache_hash     = cache_hash ,
                                                   cache_id       = cache_id1  ,
                                                   strategy       = "direct"   ,
                                                   namespace      = ns1        )

            # Store in ns2
            self.cache_service.store_with_strategy(storage_data   = test_data  ,
                                                   cache_hash     = cache_hash ,
                                                   cache_id       = cache_id2  ,
                                                   strategy       = "direct"   ,
                                                   namespace      = ns2        )

            # Delete from ns1
            result_ns1 = _.delete__cache_id(cache_id = cache_id1,
                                           namespace = ns1      )
            assert result_ns1["status"] == "success"

            # Should still exist in ns2
            retrieve_ns2 = self.cache_service.retrieve_by_id(cache_id = cache_id2,
                                                            namespace = ns2      )
            assert retrieve_ns2 is not None
            assert retrieve_ns2["data"] == test_data

            # Cannot delete ns2 item using ns1 namespace
            result_wrong = _.delete__cache_id(cache_id = cache_id2,
                                             namespace = ns1      )
            assert result_wrong["status"] == "not_found"

            # Clean up ns2
            _.delete__cache_id(cache_id2, ns2)

    def test_delete__cache_id__all_strategies(self):                        # Test delete works with all storage strategies
        with self.routes as _:
            strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

            for strategy in strategies:
                with self.subTest(strategy=strategy):
                    # Store with strategy
                    test_data  = f"delete test for {strategy}"
                    cache_hash = self.cache_service.hash_from_string(test_data)
                    cache_id   = Random_Guid()
                    namespace  = Safe_Id(f"delete-{strategy}")

                    self.cache_service.store_with_strategy(storage_data   = test_data  ,
                                                           cache_hash     = cache_hash ,
                                                           cache_id       = cache_id   ,
                                                           strategy       = strategy   ,
                                                           namespace      = namespace  )

                    # Verify stored
                    retrieve = self.cache_service.retrieve_by_id(cache_id, namespace)
                    assert retrieve is not None

                    # Delete
                    result = _.delete__cache_id(cache_id = cache_id ,
                                              namespace = namespace)

                    assert result["status"]        == "success"
                    assert result["cache_id"]      == cache_id
                    assert result["deleted_count"] > 0

                    # Verify deleted
                    retrieve_after = self.cache_service.retrieve_by_id(cache_id, namespace)
                    assert retrieve_after is None

    def test_type_enforcement(self):                                        # Test type safety of parameters
        with self.routes as _:
            # Valid Random_Guid
            valid_id = Random_Guid()
            result   = _.delete__cache_id(cache_id = valid_id         ,
                                         namespace = self.test_namespace)
            assert type(result) is dict
            assert result["status"] in ["success", "not_found"]

            # Valid Safe_Id namespace
            valid_namespace = Safe_Id("valid-namespace")
            result = _.delete__cache_id(cache_id = Random_Guid()  ,
                                       namespace = valid_namespace)
            assert type(result) is dict

            # Type_Safe should auto-convert compatible types
            # String to Random_Guid (auto-conversion)
            guid_string = "12345678-1234-1234-1234-123456789012"
            result = _.delete__cache_id(cache_id = guid_string        ,
                                       namespace = self.test_namespace)
            assert result["status"] == "not_found"                           # Valid GUID format accepted

            # String to Safe_Id (auto-conversion)
            result = _.delete__cache_id(cache_id = Random_Guid()       ,
                                       namespace = "string-namespace"  )
            assert result["status"] == "not_found"                           # Auto-converted namespace

    def test__edge_cases(self):                                             # Test edge cases
        with self.routes as _:
            # Empty GUID (if valid format)
            empty_guid = "00000000-0000-0000-0000-000000000000"
            result = _.delete__cache_id(cache_id = empty_guid         ,
                                       namespace = self.test_namespace)
            assert result["status"] == "not_found"

            # Special characters in namespace (should be sanitized)
            special_ns = "test!@#$%^&*()"
            result = _.delete__cache_id(cache_id = Random_Guid(),
                                       namespace = special_ns    )
            assert result["status"] == "not_found"                           # Won't find in sanitized namespace

    def test__delete_counts_by_strategy(self):                              # Test delete counts vary by strategy
        with self.routes as _:
            # Each strategy creates different numbers of files
            test_cases = [
                ("direct"            , 9),  # data + by_hash + by_id (3 files each)
                ("temporal"          , 9),  # Same structure
                ("temporal_latest"   , 12),  # Same structure
                ("temporal_versioned", 15),  # Same structure
            ]

            for strategy, expected_count in test_cases:
                with self.subTest(strategy=strategy):
                    # Store data
                    test_data  = f"count test for {strategy}!"
                    cache_hash = self.cache_service.hash_from_string(test_data)
                    cache_id   = Random_Guid()
                    namespace  = Safe_Id(f"count-{strategy}")

                    self.cache_service.store_with_strategy(storage_data   = test_data  ,
                                                           cache_hash     = cache_hash ,
                                                           cache_id       = cache_id   ,
                                                           strategy       = strategy   ,
                                                           namespace      = namespace  )

                    # Delete and check count
                    result = _.delete__cache_id(cache_id = cache_id ,
                                              namespace = namespace)

                    assert result["status"]        == "success"
                    assert result["deleted_count"] == expected_count
                    assert len(result["deleted_paths"]) == expected_count

    def test__integration_with_cache_service(self):                         # Test integration with Cache__Service
        with self.routes as _:
            # Complete workflow using cache service and delete route
            test_data  = "integration test data"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id   = Random_Guid()
            namespace  = Safe_Id("integration-delete")

            # Store via cache service
            store_result = self.cache_service.store_with_strategy(storage_data   = test_data ,
                                                                  cache_hash     = cache_hash,
                                                                  cache_id       = cache_id  ,
                                                                  strategy       = "temporal",
                                                                  namespace      = namespace )
            assert store_result.cache_id == cache_id

            # Verify exists
            exists_check = self.cache_service.retrieve_by_id(cache_id, namespace)
            assert exists_check is not None

            # Delete via route
            delete_result = _.delete__cache_id(cache_id = cache_id ,
                                              namespace = namespace)

            assert delete_result["status"]        == "success"
            assert delete_result["cache_id"]      == cache_id
            assert delete_result["deleted_count"] > 0

            # Verify gone
            gone_check = self.cache_service.retrieve_by_id(cache_id, namespace)
            assert gone_check is None

    def test__multiple_deletes_same_id(self):                               # Test deleting same ID multiple times
        with self.routes as _:
            # Store data
            test_data  = "multiple delete test"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id   = Random_Guid()

            self.cache_service.store_with_strategy(storage_data   = test_data         ,
                                                   cache_hash     = cache_hash        ,
                                                   cache_id       = cache_id          ,
                                                   strategy       = "direct"          ,
                                                   namespace      = self.test_namespace)

            # First delete should succeed
            result1 = _.delete__cache_id(cache_id = cache_id        ,
                                        namespace = self.test_namespace)
            assert result1["status"] == "success"

            # Second delete should not find it
            result2 = _.delete__cache_id(cache_id = cache_id        ,
                                        namespace = self.test_namespace)
            assert result2["status"]  == "not_found"
            assert result2["message"] == f"Cache ID {cache_id} not found"

            # Third delete should also not find it
            result3 = _.delete__cache_id(cache_id = cache_id        ,
                                        namespace = self.test_namespace)
            assert result3["status"] == "not_found"