import pytest
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
from mgraph_ai_service_cache.fast_api.routes.Routes__Exists             import Routes__Exists, TAG__ROUTES_EXISTS, PREFIX__ROUTES_EXISTS, BASE_PATH__ROUTES_EXISTS, ROUTES_PATHS__EXISTS
from memory_fs.schemas.Safe_Str__Cache_Hash        import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service               import Cache__Service
from tests.unit.Service__Fast_API__Test_Objs                            import setup__service_fast_api_test_objs


class test_Routes__Exists(TestCase):

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-exists-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service  = Cache__Service(default_bucket=cls.test_bucket)
        cls.routes         = Routes__Exists(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace = Safe_Id("test-exists")
        cls.test_string    = "test exists data"
        cls.test_hash      = Safe_Str__Cache_Hash("0000000000000000")      # Known non-existent hash

    @classmethod
    def tearDownClass(cls):                                                 # ONE-TIME cleanup
        for handler in cls.routes.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    def test__init__(self):                                                 # Test initialization
        with Routes__Exists() as _:
            assert type(_)               is Routes__Exists
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_EXISTS
            assert _.prefix              == PREFIX__ROUTES_EXISTS
            assert type(_.cache_service) is Cache__Service

            # Use .obj() for comprehensive comparison

            assert _.obj() ==__(tag           ='exists',
                                prefix        = '/{namespace}',
                                router        = 'APIRouter',
                                cache_service = __(default_bucket    = 'mgraph-ai-cache',
                                                   default_ttl_hours = 24,
                                                   cache_handlers    = __(),
                                                   hash_config       = __(algorithm='sha256', length=16),
                                                   hash_generator    = __(config=__(algorithm='sha256', length=16))),
                                app          = None ,
                                filter_tag   = True )

    def test__class_constants(self):                                        # Test module-level constants
        assert TAG__ROUTES_EXISTS       == 'exists'
        assert PREFIX__ROUTES_EXISTS    == '/{namespace}'
        assert BASE_PATH__ROUTES_EXISTS == '/{namespace}/exists/'
        assert ROUTES_PATHS__EXISTS     == ['/{namespace}/exists/hash/{cache_hash}']

    def test_exists__hash__cache_hash(self):                                # Test hash existence check
        with self.routes as _:
            # Store some data first to create a hash
            test_data  = "data for existence check"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id   = Random_Guid()

            # Store the data
            self.cache_service.store_with_strategy(storage_data   = test_data         ,
                                                   cache_hash     = cache_hash        ,
                                                   cache_id       = cache_id          ,
                                                   strategy       = "direct"          ,
                                                   namespace      = self.test_namespace)

            # Check it exists
            result = _.exists__hash__cache_hash(cache_hash = cache_hash        ,
                                                namespace  = self.test_namespace)

            assert result == {"exists"    : True                    ,
                             "hash"      : str(cache_hash)         ,
                             "namespace" : str(self.test_namespace)}

            # Check non-existent hash
            non_existent = _.exists__hash__cache_hash(cache_hash = self.test_hash      ,
                                                      namespace  = self.test_namespace)

            assert non_existent == {"exists"    : False                   ,
                                   "hash"      : str(self.test_hash)     ,
                                   "namespace" : str(self.test_namespace)}

    def test_exists__hash__cache_hash__default_namespace(self):             # Test default namespace handling
        with self.routes as _:
            # Test with None namespace (should use default)
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash,
                                                namespace  = None          )

            assert result == {"exists"    : False           ,
                             "hash"      : str(self.test_hash),
                             "namespace" : "default"        }

            # Store data in default namespace
            test_data  = "default namespace data"
            cache_hash = self.cache_service.hash_from_string(test_data)
            cache_id   = Random_Guid()

            self.cache_service.store_with_strategy(storage_data   = test_data   ,
                                                   cache_hash     = cache_hash  ,
                                                   cache_id       = cache_id    ,
                                                   strategy       = "direct"    ,
                                                   namespace      = None        )

            # Check it exists in default namespace
            exists_result = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                       namespace  = None       )

            assert exists_result["exists"]    is True
            assert exists_result["namespace"] == "default"

    def test_exists__hash__cache_hash__multiple_namespaces(self):           # Test namespace isolation
        with self.routes as _:
            ns1 = Safe_Id("exists-ns1")
            ns2 = Safe_Id("exists-ns2")

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

            # Check exists in ns1
            result_ns1 = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                    namespace  = ns1       )
            assert result_ns1["exists"] is True

            # Check doesn't exist in ns2
            result_ns2 = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                    namespace  = ns2       )
            assert result_ns2["exists"] is False

            # Now store in ns2
            self.cache_service.store_with_strategy(storage_data   = test_data  ,
                                                   cache_hash     = cache_hash ,
                                                   cache_id       = cache_id2  ,
                                                   strategy       = "direct"   ,
                                                   namespace      = ns2        )

            # Check now exists in both
            result_ns1_after = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                          namespace  = ns1       )
            result_ns2_after = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                          namespace  = ns2       )

            assert result_ns1_after["exists"] is True
            assert result_ns2_after["exists"] is True

    def test_type_enforcement(self):                                        # Test type safety of parameters
        with self.routes as _:
            # Valid Safe_Str__Cache_Hash
            valid_hash = Safe_Str__Cache_Hash("abc0123456789def")
            result = _.exists__hash__cache_hash(cache_hash = valid_hash        ,
                                                namespace  = self.test_namespace)
            assert type(result) is dict
            assert result["hash"] == str(valid_hash)

            # Valid Safe_Id namespace
            valid_namespace = Safe_Id("valid-namespace")
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash    ,
                                                namespace  = valid_namespace   )
            assert result["namespace"] == str(valid_namespace)

            # Type_Safe should auto-convert compatible types
            # String to Safe_Str__Cache_Hash (auto-conversion)
            result = _.exists__hash__cache_hash(cache_hash = "stringhash123456",
                                                namespace  = self.test_namespace)
            assert result["hash"] == "stringhash123456"                        # Auto-converted

            # String to Safe_Id (auto-conversion)
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash    ,
                                                namespace  = "string-namespace")
            assert result["namespace"] == "string-namespace"                   # Auto-converted

    def test__empty_hash(self):                                             # Test edge cases
        with self.routes as _:
            # Empty hash
            empty_hash       = Safe_Str__Cache_Hash("")
            expected_error_1 = 'Invalid ID: The ID must not be empty.'
            with pytest.raises(ValueError, match = expected_error_1):
                 _.exists__hash__cache_hash(cache_hash = empty_hash        ,
                                            namespace  = self.test_namespace)



    def test__integration_with_cache_service(self):                         # Test integration with Cache__Service
        with self.routes as _:
            # Create a complete cache entry workflow
            test_data   = "integration test data"
            cache_hash  = self.cache_service.hash_from_string(test_data)
            cache_id    = Random_Guid()
            namespace   = Safe_Id("integration-test")

            # Initially doesn't exist
            check_before = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                      namespace  = namespace  )
            assert check_before["exists"] is False

            # Store data
            store_result = self.cache_service.store_with_strategy(storage_data   = test_data ,
                                                                  cache_hash     = cache_hash,
                                                                  cache_id       = cache_id  ,
                                                                  strategy       = "temporal",
                                                                  namespace      = namespace )
            assert store_result.cache_id == cache_id

            # Now exists
            check_after = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                     namespace  = namespace  )
            assert check_after["exists"] is True

            # Delete and verify
            delete_result = self.cache_service.delete_by_id(cache_id = cache_id ,
                                                            namespace = namespace)
            assert delete_result["status"] == "success"

            # No longer exists
            check_deleted = _.exists__hash__cache_hash(cache_hash = cache_hash,
                                                       namespace  = namespace  )
            assert check_deleted["exists"] is False