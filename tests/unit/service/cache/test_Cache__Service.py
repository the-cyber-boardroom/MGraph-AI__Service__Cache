from unittest                                                                        import TestCase
import pytest
import re

from memory_fs.path_handlers.Path__Handler__Temporal import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.testing.__ import __
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path      import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_utils.type_safe.primitives.safe_str.text.Safe_Str__Text                  import Safe_Str__Text
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__Dict               import Type_Safe__Dict
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__List               import Type_Safe__List
from osbot_utils.utils.Dev import pprint
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import random_string_short, str_to_base64, list_set
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_aws.AWS_Config                                                           import aws_config
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service, DEFAULT__CACHE__SERVICE__BUCKET_NAME, DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler
from mgraph_ai_service_cache.service.cache.Cache__Hash__Config                      import Cache__Hash__Config
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator                   import Cache__Hash__Generator
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request            import Schema__Cache__Store__Request
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Request         import Schema__Cache__Retrieve__Request
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs


class test_Cache__Service(TestCase):                                                # Test main cache service orchestrator with all APIs

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        cls.test_objs   = setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-service-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        with Cache__Service() as _:
            cls.service          = _
            _.default_bucket     = cls.test_bucket
            _.default_ttl_hours  = 12

        # Test data shared across tests
        cls.test_namespace       = Safe_Id("test-namespace")
        cls.test_data_string     = "test cache data"
        cls.test_data_json       = {"key": "value", "number": 123, "nested": {"inner": "data"}}
        cls.test_data__base_64   = str_to_base64(json_to_str(cls.test_data_json))
        cls.test_metadata        = {Safe_Id("author"): Safe_Str__Text("test-user"),
                                    Safe_Id("version"): Safe_Str__Text("1.0")}
        cls.test_tags            = [Safe_Id("test"), Safe_Id("cache")]
        cls.path_now             = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    @classmethod
    def tearDownClass(cls):                                                             # ONE-TIME cleanup
        # Clean up all created handlers
        for handler in cls.service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)                         # todo: review this delete workflow

        # Delete test bucket
        if cls.service.cache_handlers:
            handler = list(cls.service.cache_handlers.values())[0]
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete(cls.test_bucket)                                   # todo: since this should be all we need (delete the test bucket)

    def test__init__(self):                                                          # Test auto-initialization
        with Cache__Service() as _:
            assert type(_)                is Cache__Service
            assert base_classes(_)        == [Type_Safe, object]
            assert type(_.cache_handlers) is Type_Safe__Dict                         # New API uses regular dict
            assert _.default_bucket       == DEFAULT__CACHE__SERVICE__BUCKET_NAME
            assert _.default_ttl_hours    == DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
            assert _.default_bucket       == "mgraph-ai-cache"                      # Default bucket
            assert _.default_ttl_hours    == 24                                     # Default TTL
            assert type(_.hash_config)    is Cache__Hash__Config
            assert type(_.hash_generator) is Cache__Hash__Generator

    def test_setup(self):                                                            # Test service setup initializes components
        with self.service as _:
            assert type(_.hash_config)    is Cache__Hash__Config
            assert type(_.hash_generator) is Cache__Hash__Generator
            assert _.hash_config.length   == 16                                     # Default hash length
            assert _.default_bucket       == self.test_bucket
            assert _.default_ttl_hours    == 12


    def test_get_or_create_handler(self):                                           # Test handler creation and retrieval
        namespace = Safe_Id("handler-test")

        with self.service as _:
            # First call creates new handler
            handler1 = _.get_or_create_handler(namespace)
            assert type(handler1)        is Cache__Handler
            assert handler1.s3__bucket      == self.test_bucket
            assert handler1.s3__prefix      == str(namespace)
            assert handler1.cache_ttl_hours == 12

            # Second call retrieves same instance
            handler2 = _.get_or_create_handler(namespace)
            assert handler2 is handler1

            # Verify it's in the cache
            assert namespace in _.cache_handlers

    #  Hash generation methods
    def test_hash_from_string(self):                                                # Test hash generation from string
        with self.service as _:
            hash1 = _.hash_from_string(self.test_data_string)
            assert type(hash1) is Safe_Str__Cache_Hash
            assert len(str(hash1)) == 16                                           # Default length from config

            # Same string produces same hash
            hash2 = _.hash_from_string(self.test_data_string)
            assert hash1 == hash2

            # Different string produces different hash
            hash3 = _.hash_from_string("different data")
            assert hash3 != hash1

    def test_hash_from_bytes(self):                                                 # Test hash generation from bytes
        test_bytes = b"test binary data"

        with self.service as _:
            hash_value = _.hash_from_bytes(test_bytes)

            assert type(hash_value) is Safe_Str__Cache_Hash
            assert len(str(hash_value)) == 16

            # Same bytes produce same hash
            hash_value_2 = _.hash_from_bytes(test_bytes)
            assert hash_value == hash_value_2

    def test_hash_from_json(self):                                                  # Test hash generation from JSON
        with self.service as _:
            hash_value = _.hash_from_json(self.test_data_json)

            assert type(hash_value) is Safe_Str__Cache_Hash
            assert len(str(hash_value)) == 16

            # Same JSON produces same hash
            hash_value_2 = _.hash_from_json(self.test_data_json)
            assert hash_value == hash_value_2

            # With field exclusion
            hash_excluded = _.hash_from_json(self.test_data_json, exclude_fields=["number"])
            assert hash_excluded != hash_value                                     # Different hash without 'number' field

    # store_with_strategy
    def test_store_with_strategy(self):                                            # Test storing with specific strategy
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = Random_Guid()

        with self.service as _:
            response = _.store_with_strategy(cache_key_data = self.test_data_string ,
                                             storage_data   = self.test_data_string ,
                                             cache_hash     = cache_hash            ,
                                             cache_id       = cache_id              ,
                                             strategy       = "temporal"            ,
                                             namespace      = self.test_namespace   )
            assert response.obj()               == __( hash     = '1e2ff1555748f789',
                                                       cache_id = cache_id,
                                                       paths    = [ f'data/temporal/{self.path_now}/{cache_id}.json'          ,
                                                                    f'data/temporal/{self.path_now}/{cache_id}.json.config'   ,
                                                                    f'data/temporal/{self.path_now}/{cache_id}.json.metadata'],
                                                       size     = 17)
            assert type(response)               is Schema__Cache__Store__Response
            assert response.cache_id            == cache_id
            assert response.hash                == cache_hash
            assert type(response.paths)         is Type_Safe__List
            assert response.paths.expected_type == Safe_Str__File__Path             # type: ignore
            assert response.size                 > 0

    def test_store_with_strategy__multiple_strategies(self):                        # Test different storage strategies
        cache_hash = self.service.hash_from_string(self.test_data_string)

        with self.service as _:
            # Test each strategy
            for strategy in ["direct", "temporal", "temporal_latest", "temporal_versioned"]:
                cache_id = Random_Guid()
                response = _.store_with_strategy(cache_key_data = self.test_data_string,
                                                 storage_data   = self.test_data_string,
                                                 cache_hash     = cache_hash           ,
                                                 cache_id       = cache_id             ,
                                                 strategy       = strategy             ,
                                                 namespace      = self.test_namespace  )
                assert type(response) is Schema__Cache__Store__Response
                assert response.cache_id == cache_id

    def test__bug__retrieve_by_hash(self):                                               # Test retrieval by hash
        # Store data first
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = Random_Guid()

        with self.service as _:
            result__store  = _.store_with_strategy(cache_key_data = self.test_data_string,
                                                   storage_data   = self.test_data_string,
                                                   cache_hash     = cache_hash           ,
                                                   cache_id       = cache_id             ,
                                                   strategy       = "temporal"           ,
                                                   namespace      = self.test_namespace  )

            # Retrieve by hash
            result__retrieve = _.retrieve_by_hash(cache_hash, self.test_namespace)
            assert result__retrieve         is not None                                       # BUG
            assert "data"         in result__retrieve
            assert result__retrieve["data"] == self.test_data_string

    def test_retrieve_by_hash__not_found(self):                                    # Test retrieval of non-existent hash
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")

        with self.service as _:
            result = _.retrieve_by_hash(non_existent_hash, self.test_namespace)
            assert result is None

    def test_retrieve_by_id(self):                                                 # Test retrieval by cache ID
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = Random_Guid()

        with self.service as _:
            _.store_with_strategy(cache_key_data = self.test_data_string,
                                  storage_data   = self.test_data_string,
                                  cache_hash     = cache_hash           ,
                                  cache_id       = cache_id             ,
                                  strategy       = "direct"             ,
                                  namespace      = self.test_namespace  )

            # Retrieve by ID
            result = _.retrieve_by_id(cache_id, self.test_namespace)

            assert result is not None
            assert "data" in result
            assert result["data"] == self.test_data_string

    def test_retrieve_by_id__not_found(self):                                      # Test retrieval of non-existent ID
        non_existent_id = Random_Guid()

        with self.service as _:
            result = _.retrieve_by_id(non_existent_id, self.test_namespace)
            assert result is None


    def test_retrieve__selective_fields(self):                                     # Test selective field retrieval
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = Random_Guid()

        with self.service as _:
            # Store data
            _.store_with_strategy(cache_key_data = self.test_data_string,
                                  storage_data   = self.test_data_json,  # Store JSON data
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            # Retrieve with different options
            result_full = _.retrieve_by_id(cache_id, self.test_namespace)
            stored_at   = result_full.get('metadata').get(Safe_Id('stored_at'))
            assert result_full is not None
            assert type(result_full ) is dict
            assert type(stored_at   ) is int
            assert result_full == {'data'    : self.test_data_json                                  ,
                                   'metadata': {Safe_Id('cache_hash'      ): '1e2ff1555748f789'     ,
                                                Safe_Id('cache_id'        ): cache_id               ,
                                                Safe_Id('cache_key_data'  ): self.test_data_string  ,
                                                Safe_Id('content_encoding'): None                   ,
                                                Safe_Id('namespace'       ): self.test_namespace    ,
                                                Safe_Id('stored_at'       ): stored_at              ,
                                                Safe_Id('strategy'        ): 'temporal'             }}

    def test_list_namespaces(self):                                                # Test listing active namespaces
        namespaces = [Safe_Id("ns1"), Safe_Id("ns2"), Safe_Id("ns3")]

        with self.service as _:
            # Create handlers for multiple namespaces
            for ns in namespaces:
                _.get_or_create_handler(ns)

            active = _.list_namespaces()

            assert len(active) >= 3
            assert all(ns in active for ns in namespaces)

    def test__multiple_namespaces_isolated(self):                                  # Test namespace isolation
        ns1 = Safe_Id("namespace1")
        ns2 = Safe_Id("namespace2")

        data1_string = "namespace 1 data"
        data2_string = "namespace 2 data"

        with self.service as _:
            # Store in different namespaces
            hash1 = _.hash_from_string(data1_string)
            hash2 = _.hash_from_string(data2_string)
            id1   = Random_Guid()
            id2   = Random_Guid()

            _.store_with_strategy(
                cache_key_data = data1_string,
                storage_data   = data1_string,
                cache_hash     = hash1,
                cache_id       = id1,
                strategy       = "direct",
                namespace      = ns1
            )

            _.store_with_strategy(
                cache_key_data = data2_string,
                storage_data   = data2_string,
                cache_hash     = hash2,
                cache_id       = id2,
                strategy       = "direct",
                namespace      = ns2
            )

            # Retrieve from each namespace
            result1 = _.retrieve_by_id(id1, ns1)
            result2 = _.retrieve_by_id(id2, ns2)

            assert result1["data"] == data1_string
            assert result2["data"] == data2_string

            # Cross-namespace retrieve fails
            result_cross = _.retrieve_by_id(id1, ns2)
            assert result_cross is None

    def test__complex_data_storage(self):                                          # Test storing complex nested data
        complex_data = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ],
            "settings": {
                "theme": "dark",
                "lang": "en-US",
                "nested": {"deep": {"value": 42}}
            },
            "binary": "base64encodeddata=="
        }

        with self.service as _:
            # Generate hash from complex data
            cache_hash = _.hash_from_json(complex_data)
            cache_id   = Random_Guid()

            # Store complex data
            response = _.store_with_strategy(
                cache_key_data = json_to_str(complex_data),
                storage_data   = complex_data,
                cache_hash     = cache_hash,
                cache_id       = cache_id,
                strategy       = "temporal_latest",
                namespace      = self.test_namespace
            )

            assert response.cache_id == cache_id

            # Retrieve and verify
            result = _.retrieve_by_id(cache_id, self.test_namespace)
            assert result is not None
            assert result["data"] == complex_data

    def test__store_with_provided_hash(self):                                      # Test storing with custom hash
        custom_hash = Safe_Str__Cache_Hash("abc1234567890abcd")  # 16 chars to match config
        cache_id    = Random_Guid()

        with self.service as _:
            response = _.store_with_strategy(
                cache_key_data = self.test_data_string,
                storage_data   = self.test_data_string,
                cache_hash     = custom_hash,
                cache_id       = cache_id,
                strategy       = "temporal",
                namespace      = self.test_namespace
            )

            assert response.hash == custom_hash
            assert response.cache_id == cache_id

    def test__hash_consistency_across_types(self):                                 # Test hash consistency
        test_string = "test data"
        test_bytes  = test_string.encode('utf-8')
        test_json   = {"data": test_string}

        with self.service as _:
            # String and bytes of same content should produce same hash
            hash_string = _.hash_from_string(test_string)
            hash_bytes  = _.hash_from_bytes(test_bytes)
            assert hash_string == hash_bytes

            # JSON should produce different hash
            hash_json = _.hash_from_json(test_json)
            assert hash_json != hash_string

    def test__concurrent_operations(self):                                         # Test concurrent operations on same namespace
        with self.service as _:
            cache_ids = []

            # Store multiple items
            for i in range(5):
                data = f"item_{i}"
                hash_val = _.hash_from_string(data)
                cache_id = Random_Guid()
                cache_ids.append(cache_id)

                _.store_with_strategy(
                    cache_key_data = data,
                    storage_data   = data,
                    cache_hash     = hash_val,
                    cache_id       = cache_id,
                    strategy       = "direct",
                    namespace      = self.test_namespace
                )

            # Retrieve all items
            for i, cache_id in enumerate(cache_ids):
                result = _.retrieve_by_id(cache_id, self.test_namespace)
                assert result["data"] == f"item_{i}"

    def test__error_handling__invalid_strategy(self):                              # Test error handling for invalid strategy
        with self.service as _:
            cache_hash = _.hash_from_string(self.test_data_string)
            cache_id   = Random_Guid()

            # Should raise error or handle gracefully
            with pytest.raises(ValueError, match="Unknown strategy"):
                _.store_with_strategy(
                    cache_key_data = self.test_data_string,
                    storage_data   = self.test_data_string,
                    cache_hash     = cache_hash,
                    cache_id       = cache_id,
                    strategy       = "invalid_strategy",
                    namespace      = self.test_namespace
                )