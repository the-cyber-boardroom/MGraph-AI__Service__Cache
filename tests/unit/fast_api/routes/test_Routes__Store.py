import gzip
import json
from unittest                                                                            import TestCase
import pytest
from fastapi                                                                             import Request, HTTPException
from osbot_fast_api_serverless.utils.testing.skip_tests                                  import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                              import __, __SKIP__
from memory_fs.path_handlers.Path__Handler__Temporal                                     import Path__Handler__Temporal
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.utils.Objects                                                           import base_classes
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from mgraph_ai_service_cache.fast_api.routes.Routes__Delete                              import Routes__Delete
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespace                           import Routes__Namespace
from mgraph_ai_service_cache.fast_api.routes.Routes__Retrieve                            import Routes__Retrieve
from mgraph_ai_service_cache.fast_api.routes.Routes__Store                               import Routes__Store, TAG__ROUTES_STORE, Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Invalid_Input          import Schema__Cache__Error__Invalid_Input
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response                import Schema__Cache__Store__Response
from mgraph_ai_service_cache.service.cache.Service__Cache__Retrieve                      import Service__Cache__Retrieve
from mgraph_ai_service_cache.service.cache.Service__Cache__Store                         import Service__Cache__Store
from tests.unit.Service__Fast_API__Test_Objs                                             import setup__service_fast_api_test_objs


class test_Routes__Store(TestCase):

    @classmethod
    def setUpClass(cls):                                                                      # ONE-TIME expensive setup
        cls.test_objs        = setup__service_fast_api_test_objs()
        cls.cache_fixtures   = cls.test_objs.cache_fixtures
        cls.fixtures_bucket  = cls.cache_fixtures.fixtures_bucket
        # cls.cache_service    = Cache__Service(default_bucket=cls.fixtures_bucket)
        #
        # cls.routes           = Routes__Store    (cache_service=cls.cache_service)
        # cls.routes_delete    = Routes__Delete   (cache_service=cls.cache_service)
        # cls.routes_retrieve  = Routes__Retrieve (cache_service=cls.cache_service)
        # cls.routes_namespace = Routes__Namespace(cache_service=cls.cache_service)

        cls.cache_service      = Cache__Service          (default_bucket   = cls.fixtures_bucket )
        cls.retrieve_service   = Service__Cache__Retrieve(cache_service    = cls.cache_service   )
        cls.store_service      = Service__Cache__Store   (cache_service    = cls.cache_service   )

        cls.routes             = Routes__Store           (store_service    = cls.store_service   )
        cls.routes_delete      = Routes__Delete          (cache_service    = cls.cache_service   )
        cls.routes_retrieve    = Routes__Retrieve        (retrieve_service = cls.retrieve_service)
        cls.routes_namespace   = Routes__Namespace       (cache_service    = cls.cache_service   )

        cls.test_namespace   = Safe_Str__Id("test-store-api")                                 # Test data
        cls.test_string      = "test store string"
        cls.test_json        = {"api": "test", "value": 123}
        cls.path_now         = Path__Handler__Temporal().path_now()                           # Current temporal path

    def setUp(self):                                                                          # PER-TEST lightweight setup
        self.scope   = {"type": "http", "headers": []}
        self.request = Request(scope=self.scope)                                              # Simple Request for state.body

    def test__init__(self):                                                                   # Test initialization
        with Routes__Store() as _:
            assert type(_)               is Routes__Store
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_STORE
            assert type(_.store_service) is Service__Cache__Store

    def test_store__string(self):                                                             # Test string storage endpoint
        target_string   = "a different string"
        response__store = self.routes.store__string(data      = target_string                        ,
                                                    strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                    namespace = self.test_namespace                  )

        with response__store as _:
            cache_id         = _.cache_id
            cache_hash       = '50c167dc0fbacd1a'                                             # Consistent hash

            assert _.obj () == __(cache_id   = cache_id           ,
                                   hash      = cache_hash         ,
                                   namespace = self.test_namespace,
                                   paths     = __SKIP__           ,
                                   size      = 20                 )

            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash

    def test_store__json(self):                                                               # Test JSON storage endpoint
        response__store = self.routes.store__json(data      = self.test_json                        ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL ,
                                                  namespace = self.test_namespace                  )

        with response__store as _:
            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash
            assert _.hash           == '96af669d785b90b6'                                     # Consistent hash

    def test_store__binary(self):                                                             # Test binary storage
        binary_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100                                   # Fake PNG header

        with self.routes as _:
            response_store = _.store__binary(body      = binary_data                        ,
                                            request   = self.request                        ,
                                            strategy  = Enum__Cache__Store__Strategy.DIRECT ,
                                            namespace = self.test_namespace                 )

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            assert response_store.size           > 100

    def test_store__binary__compressed(self):                                                 # Test compressed binary storage
        original_data   = b"Test data to compress" * 100
        compressed_data = gzip.compress(original_data)
        self.request    = Request(scope={"type"   : "http"                ,
                                         "method" : "POST"                ,
                                         "headers": [(b"content-encoding", b"gzip")]})

        with self.routes as _:
            response_store = _.store__binary(request   = self.request                        ,
                                            body      = compressed_data                      ,
                                            strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                            namespace = self.test_namespace                  )

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            assert response_store.size           < len(original_data)                         # Compressed size

    def test_store__binary__uncompressed(self):                                               # Test uncompressed binary
        binary_data = b'Raw binary data \x00\x01\x02'

        with self.routes as _:
            response = _.store__binary(request   = self.request                                  ,
                                      body      = binary_data                                    ,
                                      strategy  = Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED,
                                      namespace = self.test_namespace                            )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert type(response.hash)     is Safe_Str__Cache_Hash
            assert response.size           == len(binary_data)

    def test_all_storage_strategies(self):                                                    # Test all strategies work
        skip__if_not__in_github_actions()
        strategies = [Enum__Cache__Store__Strategy.DIRECT           ,
                      Enum__Cache__Store__Strategy.TEMPORAL         ,
                      Enum__Cache__Store__Strategy.TEMPORAL_LATEST  ,
                      Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED]
        test_data  = "strategy test data"

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                with self.routes as _:
                    response = _.store__string(data      = test_data                           ,
                                              strategy  = strategy                             ,
                                              namespace = Safe_Str__Id(f"strat-{strategy}")   )

                    assert type(response.cache_id) is Random_Guid
                    assert response.hash is not None
                    assert 'paths' in response.json()

    def test_namespace_default_handling(self):                                                # Test default namespace
        test_data = "default namespace test"

        with self.routes as _:
            response_store = _.store__string(data      = test_data                          ,
                                            strategy  = Enum__Cache__Store__Strategy.DIRECT ,
                                            namespace = None                                )

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid

    def test_json_that_is_compressed(self):                                                   # Test compressed JSON as binary
        json_data       = {"users": [{"id": i, "name": f"User_{i}"} for i in range(10)]}
        json_string     = json.dumps(json_data)
        compressed_data = gzip.compress(json_string.encode())
        self.request    = Request(scope={"type"   : "http"                ,
                                         "method" : "POST"                ,
                                         "headers": [(b"content-encoding", b"gzip")]})

        with self.routes as _:
            response_store = _.store__binary(request   = self.request                                ,
                                            body      = compressed_data                              ,
                                            strategy  = Enum__Cache__Store__Strategy.TEMPORAL_LATEST ,
                                            namespace = self.test_namespace                          )

            assert type(response_store)          is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            assert response_store.hash is not None

    def test_multiple_stores_same_data(self):                                                 # Test same data multiple times
        test_data = "duplicate data test"

        with self.routes as _:
            response1 = _.store__string(data      = test_data                           ,
                                       strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                       namespace = self.test_namespace                  )

            response2 = _.store__string(data      = test_data                           ,
                                       strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                       namespace = self.test_namespace                  )

            assert response1.hash     == response2.hash                                       # Same hash
            assert response1.cache_id != response2.cache_id                                   # Different IDs

    def test_store_empty_string(self):                                                        # Test empty string
        with self.routes as _:
            with pytest.raises(HTTPException) as exception:
                response = _.store__string(data      = ""                                ,
                                          strategy  = Enum__Cache__Store__Strategy.DIRECT,
                                          namespace = self.test_namespace                )

            error_data = exception.value.detail
            store_response = Schema__Cache__Error__Invalid_Input.from_json(error_data)
            assert store_response.json() == error_data
            assert store_response.obj () == __(field_value   = None                         ,
                                               constraints   = None                         ,
                                               error_type    = 'INVALID_INPUT'              ,
                                               message       = 'String data cannot be empty',
                                               timestamp     = __SKIP__                     ,
                                               request_id    = __SKIP__                     ,
                                               field_name    = 'data'                       ,
                                               expected_type = 'non-empty_string'           )


    def test_store_empty_json(self):                                                          # Test empty JSON
        with self.routes as _:
            response = _.store__json(data      = {}                                ,
                                    strategy  = Enum__Cache__Store__Strategy.DIRECT,
                                    namespace = self.test_namespace                )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert response.hash           == '44136fa355b3678a'
            assert response.namespace      == self.test_namespace

    def test_store_large_json(self):                                                          # Test large JSON
        large_json = {f"key_{i}": {"data"  : f"value_{i}"                ,
                                   "nested": {"level": 2                 ,
                                             "items": list(range(10))    }}
                     for i in range(100)}

        with self.routes as _:
            response = _.store__json(data      = large_json                                    ,
                                    strategy  = Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED,
                                    namespace = self.test_namespace                            )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert response.size           > 1000

    def test_store_with_special_characters(self):                                             # Test special characters
        special_string = "Test with special chars: 你好世界 🚀 \n\t\r"

        with self.routes as _:
            response = _.store__string(data      = special_string                    ,
                                      strategy  = Enum__Cache__Store__Strategy.DIRECT,
                                      namespace = self.test_namespace                )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid

    def test_store_json_with_null_values(self):                                               # Test JSON with nulls
        json_with_nulls = {"key1"  : None              ,
                          "key2"  : "value"            ,
                          "key3"  : [1, None, 3]       ,
                          "key4"  : {"nested": None}   }

        with self.routes as _:
            response = _.store__json(data      = json_with_nulls                     ,
                                    strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                    namespace = self.test_namespace                 )

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid

    def test_store_string_in_advanced_path(self):                                             # Test semantic file path
        an_string = "this is a string"
        cache_key = 'aaa/bbb'
        file_id   = 'page-structure'
        namespace = 'default'

        kwargs = dict(data      = an_string                                  ,
                      namespace = namespace                                  ,
                      strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE ,
                      cache_key = cache_key                                  ,
                      file_id   = file_id                                    )

        response__store            = self.routes.store__string__cache_key(**kwargs)
        cache_id                   = response__store.cache_id
        cache_hash                 = response__store.hash

        response__details          = self.routes_retrieve.retrieve__details__all__cache_id(cache_id=cache_id, namespace=namespace)
        response__namespace_ids    = self.routes_namespace.file_ids                       (namespace=namespace)
        response__namespace_hashes = self.routes_namespace.file_hashes                    (namespace=namespace)
        response__delete           = self.routes_delete.delete__cache_id                  (cache_id=cache_id, namespace=namespace)

        assert response__details                     is None
        assert cache_id                              in response__namespace_ids
        assert cache_hash                            in response__namespace_hashes
        assert response__delete.get('deleted_count') == 9
