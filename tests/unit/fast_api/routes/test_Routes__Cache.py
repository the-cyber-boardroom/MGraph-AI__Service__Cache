from unittest                                                                       import TestCase
from fastapi                                                                        import Request
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_utils.utils.Misc                                                         import random_string_short
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_fast_api.api.routes.Fast_API__Routes                                     import Fast_API__Routes
from mgraph_ai_service_cache.fast_api.routes.Routes__Cache                          import Routes__Cache
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs

class test_Routes__Cache(TestCase):

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-routes-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service  = Cache__Service(default_bucket=cls.test_bucket)
        cls.routes         = Routes__Cache(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace = Safe_Id("test-api")
        cls.test_string    = "test cache string"
        cls.test_json      = {"api": "test", "value": 123}
        cls.path_now       = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    def setUp(self):
        self.request = Request(scope={ "type": "http"})                             # simple Request object that we can use to add data to the state.body object

    @classmethod
    def tearDownClass(cls):                                                         # ONE-TIME cleanup
        for handler in cls.routes.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    def test__init__(self):                                                         # Test initialization
        with Routes__Cache() as _:
            assert type(_)               is Routes__Cache
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == 'cache'
            assert type(_.cache_service) is Cache__Service

    def test_store__string__strategy__namespace(self):                                                    # Test string storage endpoint
        target_string           = "a different string"
        self.request.state.body = target_string.encode()
        response__store         = self.routes.store__string__strategy__namespace(request   = self.request       ,
                                                                                 strategy  = "temporal"         ,
                                                                                 namespace = self.test_namespace)
        with response__store as _:
            cache_id         = _.cache_id
            cache_id__path_1 = cache_id[0:2]
            cache_id__path_2 = cache_id[2:4]
            cache_hash       = '50c167dc0fbacd1a'                                             # this should always be the same
            files_created    = { 'data'   : [ f'data/temporal/{self.path_now}/{cache_id}.json'                              ,
                                              f'data/temporal/{self.path_now}/{cache_id}.json.config'                       ,
                                              f'data/temporal/{self.path_now}/{cache_id}.json.metadata'                     ],
                                 'by_hash': [ f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json'         ,
                                              f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config'  ,
                                              f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                 'by_id'  : [ f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json'           ,
                                              f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.config'    ,
                                              f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.metadata'  ]}
            assert _.json() == { 'hash'    : cache_hash    ,
                                 'cache_id': cache_id      ,
                                 'paths'   : files_created ,
                                 'size'    : 20            }

            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash

        response__retrieve = self.routes.retrieve__by_id__cache_id__namespace(cache_id=cache_id, namespace=self.test_namespace)

        stored_at          = response__retrieve.get('metadata').get(Safe_Id('stored_at'))
        assert type(stored_at)         is int
        assert response__retrieve == {'data': target_string,
                                      'metadata': {Safe_Id('cache_hash'      ): cache_hash,
                                                   Safe_Id('cache_id'        ): cache_id,
                                                   Safe_Id('cache_key_data'  ): target_string,
                                                   Safe_Id('content_encoding'): None,
                                                   Safe_Id('namespace'       ): 'test-api',
                                                   Safe_Id('stored_at'       ): stored_at,
                                                   Safe_Id('strategy'        ): 'temporal'}}
        response__delete = self.routes.delete__by_id__cache_id__namespace(cache_id=cache_id, namespace=self.test_namespace)
        assert response__delete == { 'cache_id'     : cache_id,
                                     'deleted_count': 9,
                                     'deleted_paths': [f'data/temporal/{self.path_now}/{cache_id}.json'                      ,
                                                       f'data/temporal/{self.path_now}/{cache_id}.json.config'               ,
                                                       f'data/temporal/{self.path_now}/{cache_id}.json.metadata'             ,
                                                       f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json'                           ,
                                                       f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config'                    ,
                                                       f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'                  ,
                                                       f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json'         ,
                                                       f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config'  ,
                                                       f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata'],
                                     'failed_count' : 0,
                                     'failed_paths' : [],
                                     'status'       : 'success'}




    def test_store__json__strategy__namespace(self):                                                      # Test JSON storage endpoint
        self.request.state.body = json_to_str(self.test_json).encode()
        response__store = self.routes.store__json__strategy__namespace(request   = self.request       ,
                                                      strategy  = "temporal"         ,
                                                      namespace = self.test_namespace)
        with response__store  as _:
            cache_id = _.cache_id
            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash
        response__retrieve = self.routes.retrieve__by_id__cache_id__namespace(cache_id=cache_id, namespace=self.test_namespace)
        assert response__retrieve == { 'data': {'api': 'test', 'value': 123},
                                       'metadata': { Safe_Id('cache_hash'      ) : '96af669d785b90b6',
                                                     Safe_Id('cache_id'        ) : cache_id,
                                                     Safe_Id('cache_key_data'  ) : "{'api': 'test', 'value': 123}",
                                                     Safe_Id('content_encoding') : None,
                                                     Safe_Id('namespace'       ) : 'test-api',
                                                     Safe_Id('stored_at'       ) : response__retrieve['metadata']['stored_at'],
                                                     Safe_Id('strategy'        ) : 'temporal'}}

    def test_retrieve__by_hash__cache_hash__namespace(self):                                                # Test retrieval by hash
        with self.routes as _:
            self.request.state.body = self.test_string.encode()
            response__store          = _.store__string__strategy__namespace(self.request, namespace=self.test_namespace)      # Store first
            response__retrieve       = _.retrieve__by_hash__cache_hash__namespace(response__store.hash, self.test_namespace)       # Retrieve

            assert response__retrieve is not None
            assert "data" in response__retrieve
            assert response__retrieve["data"] == self.test_string

    def test_retrieve__by_hash__cache_hash__namespace__not_found(self):                                     # Test retrieval of non-existent
        with self.routes as _:
            result = _.retrieve__by_hash__cache_hash__namespace(Safe_Str__Cache_Hash("0000000000000000"), self.test_namespace)

            assert result == {"status": "not_found", "message": "Cache entry not found"}

    def test_retrieve__by_id__cache_id__namespace(self):                                                  # Test retrieval by cache ID
        with self.routes as _:
            self.request.state.body = self.test_string.encode()
            store_response = _.store__string__strategy__namespace(self.request, namespace=self.test_namespace)             # Store first

            # Retrieve
            result = _.retrieve__by_id__cache_id__namespace(store_response.cache_id, self.test_namespace)

            assert result is not None
            assert "data" in result
            assert result["data"] == self.test_string

    def test_hash_calculate(self):                                                  # Test hash calculation endpoint
        with self.routes as _:
            # From string
            result = _.hash_calculate(data="test string")
            assert "hash" in result
            assert len(result["hash"]) == 16

            # From JSON
            result = _.hash_calculate(json_data={"key": "value"})
            assert "hash" in result

            # With exclusions
            result = _.hash_calculate(json_data      = {"key": "value", "id": "123"},
                                    exclude_fields = ["id"])
            assert "hash" in result

            # No data provided
            result = _.hash_calculate()
            assert result == {"error": "No data provided"}

    def test_exists__cache_hash__namespace(self):                                                          # Test existence check
        with self.routes as _:
            self.request.state.body = self.test_string.encode()
            store_response = _.store__string__strategy__namespace(self.request, namespace=self.test_namespace)      # Store data

            result = _.exists__cache_hash__namespace(store_response.hash, self.test_namespace)                                 # Check exists
            assert result == {"exists": True, "hash": str(store_response.hash)}

            # Check non-existent
            result = _.exists__cache_hash__namespace(Safe_Str__Cache_Hash("0000000000000000"), self.test_namespace)
            assert result == {"exists": False, "hash": "0000000000000000"}

    def test_namespaces(self):                                                      # Test namespace listing
        with self.routes as _:
            self.request.state.body = self.test_string.encode()
            _.store__string__strategy__namespace(self.request, namespace=Safe_Id("ns1"))                       # Create some namespaces
            _.store__string__strategy__namespace(self.request, namespace=Safe_Id("ns2"))

            result = _.namespaces()

            assert "namespaces" in result
            assert "count"      in result
            assert result["count"] >= 2

    def test_stats(self):                                                           # Test statistics endpoint
        self.request.state.body = self.test_string.encode()
        with self.routes as _:
            # Store some data
            for i in range(3):
                _.store__string__strategy__namespace(self.request, namespace=self.test_namespace)

            result = _.stats(self.test_namespace)

            assert result["namespace"] == str(self.test_namespace)
            assert result["s3_bucket"] == self.test_bucket
            assert result["ttl_hours"] == 24
            assert "direct_files" in result
            assert "temporal_files" in result