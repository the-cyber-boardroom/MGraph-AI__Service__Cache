import gzip
import json
import base64
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

        stored_at          = response__retrieve.get('metadata').get('stored_at')
        assert type(stored_at)         is int
        assert response__retrieve == {'content_encoding': None,
                                      'data'            : target_string,
                                      'data_type'       : 'string',
                                      'metadata'        : { 'cache_hash'      : cache_hash      ,
                                                            'cache_id'        : cache_id        ,
                                                            'cache_key_data'  : target_string   ,
                                                            'content_encoding': None            ,
                                                            'file_type'       : 'json'          ,
                                                            'namespace'       : 'test-api'      ,
                                                            'stored_at'       : stored_at       ,
                                                            'strategy'        : 'temporal'      }}
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
        assert response__retrieve == { 'content_encoding': None,
                                       'data'            : {'api': 'test', 'value': 123},
                                       'data_type'       : 'json' ,
                                       'metadata'        : { 'cache_hash'       : '96af669d785b90b6',
                                                             'cache_id'         : cache_id          ,
                                                             'cache_key_data'   : '{\n    "api": "test",\n    "value": 123\n}',
                                                             'file_type'        : 'json'            ,
                                                             'content_encoding' : None              ,
                                                             'namespace'        : 'test-api'        ,
                                                             'stored_at'        : response__retrieve['metadata']['stored_at'],
                                                             'strategy'         : 'temporal'        }}

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

    # def test_hash_calculate(self):                                                  # Test hash calculation endpoint
    #     with self.routes as _:
    #         # From string
    #         result = _.hash_calculate(data="test string")
    #         assert "hash" in result
    #         assert len(result["hash"]) == 16
    #
    #         # From JSON
    #         result = _.hash_calculate(json_data={"key": "value"})
    #         assert "hash" in result
    #
    #         # With exclusions
    #         result = _.hash_calculate(json_data      = {"key": "value", "id": "123"},
    #                                 exclude_fields = ["id"])
    #         assert "hash" in result
    #
    #         # No data provided
    #         result = _.hash_calculate()
    #         assert result == {"error": "No data provided"}

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

            result = _.stats__namespaces__namespace(self.test_namespace)

            assert result["namespace"] == str(self.test_namespace)
            assert result["s3_bucket"] == self.test_bucket
            assert result["ttl_hours"] == 24
            assert "direct_files" in result
            assert "temporal_files" in result


    def test_store_and_retrieve_binary_via_routes(self):                                # Test binary through route endpoints
        # Create test binary data
        binary_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100                            # Fake PNG header
        self.request.state.body = binary_data

        with self.routes as _:
            # Store binary
            response_store = _.store__binary__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "direct"            ,
                                                                 namespace = self.test_namespace )

            cache_id   = response_store.cache_id
            cache_hash = response_store.hash

            assert type(response_store)        is Schema__Cache__Store__Response
            assert type(response_store.cache_id) is Random_Guid
            assert response_store.size         > 100                                    # Binary has size

            # Retrieve as binary
            result_binary = _.retrieve__binary__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                          namespace = self.test_namespace)

            assert result_binary.body        == binary_data                             # Exact binary match
            assert result_binary.media_type  == "application/octet-stream"

            # Cannot retrieve data as generic
            result_generic = _.retrieve__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                   namespace = self.test_namespace)
            assert result_generic["data_type"]       == "binary"
            assert result_generic["message"]         == 'Binary data cannot be returned in JSON response'

    def test_retrieve_string_by_id(self):                                               # Test string-specific retrieval
        test_string = "Test string data"
        self.request.state.body = test_string.encode()

        with self.routes as _:
            # Store string
            response_store = _.store__string__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "temporal"          ,
                                                                 namespace = self.test_namespace )
            cache_id = response_store.cache_id

            # Retrieve as string
            result = _.retrieve__string__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                   namespace = self.test_namespace)

            assert result.body       == test_string.encode()
            assert result.media_type == "text/plain"

    def test_retrieve_json_by_id(self):                                                 # Test JSON-specific retrieval
        test_json = {"test": "data", "number": 123, "nested": {"key": "value"}}
        self.request.state.body = json_to_str(test_json).encode()

        with self.routes as _:
            # Store JSON
            response_store = _.store__json__strategy__namespace(request   = self.request       ,
                                                               strategy  = "temporal"          ,
                                                               namespace = self.test_namespace )
            cache_id = response_store.cache_id

            # Retrieve as JSON
            result = _.retrieve__json__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                 namespace = self.test_namespace)

            assert result == test_json                                                  # Direct JSON comparison

    def test_retrieve_by_hash_type_specific(self):                                      # Test type-specific hash retrieval
        test_data = {"key": "value", "list": [1, 2, 3]}
        self.request.state.body = json_to_str(test_data).encode()

        with self.routes as _:
            # Store JSON
            response_store = _.store__json__strategy__namespace(request   = self.request       ,
                                                               strategy  = "direct"             ,
                                                               namespace = self.test_namespace )
            cache_hash = response_store.hash

            # Retrieve as JSON by hash
            result_json = _.retrieve__json__by_hash__cache_hash__namespace(cache_hash = cache_hash      ,
                                                                          namespace  = self.test_namespace)
            assert result_json == test_data

            # Retrieve as string by hash (should stringify JSON)
            result_string = _.retrieve__string__by_hash__cache_hash__namespace(cache_hash = cache_hash      ,
                                                                              namespace  = self.test_namespace)
            assert result_string.body == json.dumps(test_data).encode()

            # Retrieve as binary by hash
            result_binary = _.retrieve__binary__by_hash__cache_hash__namespace(cache_hash = cache_hash      ,
                                                                              namespace  = self.test_namespace)
            assert result_binary.body == json.dumps(test_data).encode('utf-8')

    def test_cross_type_retrieval(self):                                                # Test retrieving data as different types
        # Store string data
        test_string = "Simple test string"
        self.request.state.body = test_string.encode()

        with self.routes as _:
            response_store = _.store__string__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "direct"            ,
                                                                 namespace = self.test_namespace )
            cache_id = response_store.cache_id

            # Retrieve as string (native type)
            result_string = _.retrieve__string__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                          namespace = self.test_namespace)
            assert result_string.body == test_string.encode()

            # Retrieve as JSON (should fail gracefully)
            result_json = _.retrieve__json__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                      namespace = self.test_namespace)
            assert "error" in result_json
            assert result_json["error"] == "Data is not valid JSON"
            assert result_json["data"]  == test_string

            # Retrieve as binary (should convert)
            result_binary = _.retrieve__binary__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                          namespace = self.test_namespace)
            assert result_binary.body == test_string.encode('utf-8')

    def test_retrieve_not_found_responses(self):                                        # Test 404 responses for type-specific endpoints
        non_existent_id   = Random_Guid()
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")

        with self.routes as _:
            # String endpoints return 404 Response
            result_string_id = _.retrieve__string__by_id__cache_id__namespace(cache_id  = non_existent_id  ,
                                                                             namespace = self.test_namespace)
            assert result_string_id.status_code == 404
            assert result_string_id.body        == b"Not found"

            result_string_hash = _.retrieve__string__by_hash__cache_hash__namespace(cache_hash = non_existent_hash,
                                                                                   namespace  = self.test_namespace)
            assert result_string_hash.status_code == 404

            # JSON endpoints return JSON error
            result_json_id = _.retrieve__json__by_id__cache_id__namespace(cache_id  = non_existent_id  ,
                                                                         namespace = self.test_namespace)
            assert result_json_id == {"status": "not_found", "message": "Cache entry not found"}

            # Binary endpoints return 404 Response
            result_binary_id = _.retrieve__binary__by_id__cache_id__namespace(cache_id  = non_existent_id  ,
                                                                             namespace = self.test_namespace)
            assert result_binary_id.status_code == 404

    def test_edge_cases_in_type_conversion(self):                                       # Test edge cases for type conversions
        with self.routes as _:
            # Store binary that looks like text
            text_like_binary = b"This looks like text but is binary"
            self.request.state.body = text_like_binary

            response_store = _.store__binary__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "direct"            ,
                                                                 namespace = self.test_namespace )
            cache_id = response_store.cache_id

            # Should not be able to retrieve it directly
            result = _.retrieve__by_id__cache_id__namespace(cache_id, self.test_namespace)
            assert result["data_type"] == "binary"
            assert result["message"  ] == 'Binary data cannot be returned in JSON response'

            # Retrieve as string should decode
            result_string = _.retrieve__string__by_id__cache_id__namespace(cache_id, self.test_namespace)
            assert result_string.body == text_like_binary

    def test_binary_base64_fallback_for_non_utf8(self):                                 # Test base64 encoding for non-UTF8 binary
        # Binary data that can't be decoded as UTF-8
        non_utf8_binary = b'\xff\xfe\x00\x01\x02\x03'
        self.request.state.body = non_utf8_binary

        with self.routes as _:
            response_store = _.store__binary__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "direct"            ,
                                                                 namespace = self.test_namespace )
            cache_id = response_store.cache_id

            # Retrieve as string should fallback to base64
            result_string = _.retrieve__string__by_id__cache_id__namespace(cache_id, self.test_namespace)
            expected_base64 = base64.b64encode(non_utf8_binary).decode('utf-8')
            assert result_string.body == expected_base64.encode()

            # Retrieve as JSON should wrap in base64
            result_json = _.retrieve__json__by_id__cache_id__namespace(cache_id, self.test_namespace)
            assert result_json == {"data_type": "binary"              ,
                                  "encoding" : "base64"               ,
                                  "data"     : expected_base64        }

    # Updates needed in test_Routes__Cache.py

    def test_store_compressed_binary_via_routes(self):                                  # Test compressed binary storage
        original_data   = b"Test data to compress" * 100
        compressed_data = gzip.compress(original_data)
        self.request    = Request(scope={ "type": "http",
                                          "method": "POST",
                                          "headers": [  (b"content-encoding", b"gzip")]})
        self.request.state.body = compressed_data

        with self.routes as _:
            # Store compressed binary
            response_store = _.store__binary__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "temporal"          ,
                                                                 namespace = self.test_namespace )

            cache_id = response_store.cache_id

            # Retrieve - should auto-decompress
            result = _.retrieve__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                           namespace = self.test_namespace)
            assert result["data_type"]        == "binary"
            assert result["message"]          == 'Binary data cannot be returned in JSON response'                          # Auto-decompressed

            # Test binary retrieval - data should be decompressed, no Content-Encoding header
            result_binary = _.retrieve__binary__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                          namespace = self.test_namespace)
            assert result_binary.body       == original_data                            # Decompressed data
            assert result_binary.media_type == "application/octet-stream"
            assert "content-encoding" not in result_binary.headers.keys()               # No encoding header

    def test_binary_that_is_json_when_decompressed(self):                               # Test compressed JSON as binary
        json_data       = {"users": [{"id": i, "name": f"User_{i}"} for i in range(10)]}
        json_string     = json.dumps(json_data)
        compressed_data = gzip.compress(json_string.encode())
        self.request    = Request(scope={ "type": "http",
                                          "method": "POST",
                                          "headers": [  (b"content-encoding", b"gzip")]})
        self.request.state.body = compressed_data

        with self.routes as _:
            # Store as compressed binary
            response_store = _.store__binary__strategy__namespace(request   = self.request       ,
                                                                 strategy  = "temporal_latest"   ,
                                                                 namespace = self.test_namespace )
            cache_id = response_store.cache_id

            # Generic retrieve should detect JSON after decompression
            result_generic = _.retrieve__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                   namespace = self.test_namespace)

            assert result_generic["data_type"]        == "json"                         # Detected as JSON
            assert result_generic["content_encoding"] == "gzip"
            assert result_generic["data"]             == json_data                      # Fully parsed

            # JSON retrieve should work directly
            result_json = _.retrieve__json__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                      namespace = self.test_namespace)
            assert result_json == json_data

            # Binary retrieve should return decompressed JSON as bytes
            result_binary = _.retrieve__binary__by_id__cache_id__namespace(cache_id  = cache_id        ,
                                                                          namespace = self.test_namespace)
            assert result_binary.body       == json.dumps(json_data).encode('utf-8')    # JSON as bytes
            assert result_binary.media_type == "application/octet-stream"
            assert "content-encoding" not in result_binary.headers.keys()               # No encoding header