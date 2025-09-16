import gzip
import json
from unittest                                                                       import TestCase
from fastapi                                                                        import Request
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                   import Safe_Id
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_utils.utils.Misc                                                         import random_string_short
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_fast_api.api.routes.Fast_API__Routes                                     import Fast_API__Routes
from mgraph_ai_service_cache.fast_api.routes.Routes__Delete                         import Routes__Delete
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespace                      import Routes__Namespace
from mgraph_ai_service_cache.fast_api.routes.Routes__Retrieve                       import Routes__Retrieve
from mgraph_ai_service_cache.fast_api.routes.Routes__Store                          import Routes__Store, TAG__ROUTES_STORE, Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs

class test_Routes__Store(TestCase):

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-routes-store-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service    = Cache__Service   (default_bucket=cls.test_bucket)
        cls.routes           = Routes__Store    (cache_service=cls.cache_service)
        cls.routes_delete    = Routes__Delete   (cache_service=cls.cache_service)
        cls.routes_retrieve  = Routes__Retrieve (cache_service=cls.cache_service)
        cls.routes_namespace = Routes__Namespace(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace = Safe_Id("test-store-api")
        cls.test_string    = "test store string"
        cls.test_json      = {"api": "test", "value": 123}
        cls.path_now       = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    def setUp(self):
        self.scope = { "type"  : "http",
                       "headers": []}
        self.request = Request(scope=self.scope)                                    # simple Request object that we can use to add data to the state.body object

    @classmethod
    def tearDownClass(cls):                                                         # ONE-TIME cleanup
        for handler in cls.routes.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    def test__init__(self):                                                         # Test initialization
        with Routes__Store() as _:
            assert type(_)               is Routes__Store
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_STORE
            assert type(_.cache_service) is Cache__Service

    def test_store__string(self):                                     # Test string storage endpoint
        target_string           = "a different string"
        response__store         = self.routes.store__string(data      = target_string    ,
                                                                                          strategy  = "temporal"       ,
                                                                                          namespace = self.test_namespace)
        with response__store as _:
            cache_id         = _.cache_id
            cache_id__path_1 = cache_id[0:2]
            cache_id__path_2 = cache_id[2:4]
            cache_hash       = '50c167dc0fbacd1a'                                   # this should always be the same
            files_created    = { 'data'   : [ f'data/temporal/{self.path_now}/{cache_id}.json'                              ,
                                              f'data/temporal/{self.path_now}/{cache_id}.json.config'                       ,
                                              f'data/temporal/{self.path_now}/{cache_id}.json.metadata'                     ],
                                 'by_hash': [ f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json'         ,
                                              f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config'  ,
                                              f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                 'by_id'  : [ f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json'           ,
                                              f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.config'    ,
                                              f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.metadata'  ]}
            assert _.json() == { 'hash'     : cache_hash      ,
                                 'cache_id' : cache_id        ,
                                 'namespace': 'test-store-api',
                                 'paths'    : files_created   ,
                                 'size'     : 20              }

            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash

    def test_store__json(self):                                       # Test JSON storage endpoint
        response__store = self.routes.store__json(data      = self.test_json       ,
                                                                                strategy  = "temporal"           ,
                                                                                namespace = self.test_namespace  )
        with response__store  as _:
            cache_id = _.cache_id
            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash
            assert _.hash           == '96af669d785b90b6'                           # consistent hash for same data

    def test_store__binary(self):                                     # Test binary through route endpoints
        # Create test binary data
        binary_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100                         # Fake PNG header

        with self.routes as _:
            # Store binary
            response_store = _.store__binary(body      = binary_data         ,
                                                                           request   = self.request        ,
                                                                           strategy  = "direct"            ,
                                                                           namespace = self.test_namespace )

            cache_id   = response_store.cache_id
            cache_hash = response_store.hash

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            assert response_store.size           > 100                              # Binary has size

    def test_store__binary__compressed(self):                         # Test compressed binary storage
        original_data   = b"Test data to compress" * 100
        compressed_data = gzip.compress(original_data)
        self.request    = Request(scope={ "type": "http",
                                          "method": "POST",
                                          "headers": [  (b"content-encoding", b"gzip")]})

        with self.routes as _:
            # Store compressed binary
            response_store = _.store__binary(request   = self.request        ,
                                                                           body      = compressed_data     ,
                                                                           strategy  = "temporal"          ,
                                                                           namespace = self.test_namespace )

            cache_id = response_store.cache_id

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            # Size should reflect compressed size
            assert response_store.size < len(original_data)

    def test_store__binary__uncompressed(self):                       # Test uncompressed binary storage
        binary_data = b'Raw binary data \x00\x01\x02'

        with self.routes as _:
            response = _.store__binary(request   = self.request        ,
                                                     body      = binary_data         ,
                                                     strategy  = "temporal_versioned",
                                                     namespace = self.test_namespace )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert type(response.hash)     is Safe_Str__Cache_Hash
            assert response.size           == len(binary_data)

    def test_all_storage_strategies(self):                                          # Test all storage strategies work
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]
        test_data  = "strategy test data"

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                with self.routes as _:
                    # Store with strategy
                    response = _.store__string(data      = test_data                  ,
                                                              strategy  = strategy                   ,
                                                              namespace = Safe_Id(f"strat-{strategy}"))

                    assert type(response.cache_id) is Random_Guid
                    assert response.hash is not None
                    assert 'paths' in response.json()

    def test_namespace_default_handling(self):                                      # Test default namespace handling
        test_data = "default namespace test"

        with self.routes as _:
            # Store without namespace (should use "default")
            response_store = _.store__string(data      = test_data ,
                                                            strategy  = "direct"  ,
                                                            namespace = None      )
            cache_id = response_store.cache_id

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid

    def test_json_that_is_compressed(self):                                         # Test compressed JSON as binary
        json_data       = {"users": [{"id": i, "name": f"User_{i}"} for i in range(10)]}
        json_string     = json.dumps(json_data)
        compressed_data = gzip.compress(json_string.encode())
        self.request    = Request(scope={ "type": "http",
                                          "method": "POST",
                                          "headers": [  (b"content-encoding", b"gzip")]})

        with self.routes as _:
            # Store as compressed binary
            response_store = _.store__binary(request   = self.request        ,
                                                           body      = compressed_data     ,
                                                           strategy  = "temporal_latest"   ,
                                                           namespace = self.test_namespace )
            cache_id = response_store.cache_id

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            # The hash should be of the decompressed data
            assert response_store.hash is not None

    def test_multiple_stores_same_data(self):                                       # Test storing same data multiple times
        test_data = "duplicate data test"

        with self.routes as _:
            # Store same data twice
            response1 = _.store__string(data      = test_data         ,
                                                       strategy  = "temporal"         ,
                                                       namespace = self.test_namespace)

            response2 = _.store__string(data      = test_data         ,
                                                     strategy  = "temporal"         ,
                                                     namespace = self.test_namespace)

            # Should have same hash but different cache_ids
            assert response1.hash     == response2.hash
            assert response1.cache_id != response2.cache_id

    def test_store_empty_string(self):                                              # Test storing empty string
        with self.routes as _:
            response = _.store__string(data      = ""                ,
                                                                     strategy  = "direct"           ,
                                                                     namespace = self.test_namespace)

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid

            assert response.size           == 2                                     # because we are storing as a json object

    def test_store_empty_json(self):                                                # Test storing empty JSON object
        with self.routes as _:
            response = _.store__json(data      = {}                  ,
                                                                   strategy  = "direct"             ,
                                                                   namespace = self.test_namespace  )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid

    def test_store_large_json(self):                                                # Test storing large JSON
        large_json = {
            f"key_{i}": {
                "data": f"value_{i}",
                "nested": {"level": 2, "items": list(range(10))}
            } for i in range(100)
        }

        with self.routes as _:
            response = _.store__json(data      = large_json          ,
                                                  strategy  = "temporal_versioned" ,
                                                  namespace = self.test_namespace  )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert response.size           > 1000  # Should be reasonably large

    def test_store_with_special_characters(self):                                   # Test storing data with special characters
        special_string = "Test with special chars: 你好世界 🚀 \n\t\r"

        with self.routes as _:
            response = _.store__string(data      = special_string    ,
                                                    strategy  = "direct"           ,
                                                    namespace = self.test_namespace)

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid

    def test_store_json_with_null_values(self):                                     # Test JSON with null values
        json_with_nulls = {
            "key1": None,
            "key2": "value",
            "key3": [1, None, 3],
            "key4": {"nested": None}
        }

        with self.routes as _:
            response = _.store__json(data      = json_with_nulls     ,
                                                  strategy  = "temporal"           ,
                                                  namespace = self.test_namespace  )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid

    def test_store_string_in_advanced_path(self):
        from mgraph_ai_service_cache.fast_api.routes.Routes__Delete import Routes__Delete
        an_string = "this is a string"
        cache_key = 'aaa/bbb'
        file_id   = 'page-structure'
        namespace = 'default'
        kwargs    = dict(data      = an_string                                 ,
                         strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                         cache_key = cache_key                                ,
                         file_id   = file_id                                  )

        response__store            = self.routes          .store__string__cache_key   (**kwargs)
        cache_id                   = response__store.cache_id
        cache_hash                 = response__store.hash
        response__details          = self.routes_retrieve  .retrieve__details__all__cache_id(cache_id=cache_id, namespace = namespace)
        response__namespace_ids    = self.routes_namespace.file_ids                         (namespace=namespace)
        response__namespace_hashes = self.routes_namespace.file_hashes                      (namespace=namespace)
        response__delete           = self.routes_delete   .delete__cache_id                 (cache_id=cache_id, namespace = namespace)
        #response__store.print()
        #pprint(response__details)
        #response__store.print()
        assert cache_id     in response__namespace_ids
        assert cache_hash   in response__namespace_hashes
        assert response__delete.get('deleted_count') == 9


