import base64
import json
from unittest                                                                       import TestCase
from fastapi                                                                        import Request, Response
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                   import Safe_Id
from osbot_utils.utils.Json                                                         import json_to_str, str_to_json
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_utils.utils.Misc                                                         import random_string_short
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_fast_api.api.routes.Fast_API__Routes                                     import Fast_API__Routes
from mgraph_ai_service_cache.fast_api.routes.Routes__Retrieve                       import Routes__Retrieve
from mgraph_ai_service_cache.fast_api.routes.Routes__Store                          import Routes__Store
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs

class test_Routes__Retrieve(TestCase):

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-routes-retrieve-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service    = Cache__Service  (default_bucket=cls.test_bucket )
        cls.routes__store    = Routes__Store   (cache_service=cls.cache_service)
        cls.routes__retrieve = Routes__Retrieve(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace = Safe_Id("test-retrieve-api")
        cls.test_string    = "test retrieve string"
        cls.test_json      = {"api": "test", "value": 123}
        cls.path_now       = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    def setUp(self):
        self.scope = { "type"  : "http",
                       "headers": []}
        self.request = Request(scope=self.scope)                                    # simple Request object that we can use to add data to the state.body object

    @classmethod
    def tearDownClass(cls):                                                         # ONE-TIME cleanup
        for handler in cls.routes__retrieve.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    def test__init__(self):                                                         # Test initialization
        with Routes__Retrieve() as _:
            assert type(_)               is Routes__Retrieve
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == 'retrieve'
            assert _.prefix              == '/{namespace}'
            assert type(_.cache_service) is Cache__Service

    def _store_test_data(self, data, data_type="string"):                          # Helper to store test data
        """Store test data using cache service directly"""
        if data_type == "string":
            result = self.routes__store.store__string(data=data             , namespace=self.test_namespace)
        elif data_type == "json":
            result = self.routes__store.store__json  (data=json_to_str(data), namespace=self.test_namespace)
        elif data_type == "binary":
            result = self.routes__store.store__binary(body=data             , namespace=self.test_namespace, request=self.request)
        else:
            raise NotImplementedError
        return result.cache_id, result.hash

    def test_retrieve__hash__cache_hash(self):                                      # Test retrieval by hash
        with self.routes__retrieve as _:
            cache_id, cache_hash = self._store_test_data(self.test_string)                             # Store first
            response             = _.retrieve__hash__cache_hash(cache_hash, self.test_namespace)       # Retrieve

            assert response         is not None
            assert "data"           in response
            assert response["data"] == self.test_string

    def test_retrieve__hash__cache_hash__not_found(self):                           # Test retrieval of non-existent
        with self.routes__retrieve as _:
            result = _.retrieve__hash__cache_hash(Safe_Str__Cache_Hash("0000000000000000"), self.test_namespace)

            assert result == {"status": "not_found", "message": "Cache entry not found"}

    def test_retrieve__cache_id(self):                                              # Test retrieval by cache ID
        with self.routes__retrieve as _:
            cache_id, cache_hash = self._store_test_data(self.test_string)         # Store first

            result = _.retrieve__cache_id(cache_id, self.test_namespace)           # Retrieve

            assert result is not None
            assert "data" in result
            assert result["data"] == self.test_string

    def test_retrieve__cache_id__string(self):                                      # Test string-specific retrieval
        test_string = "Test string data"
        cache_id, cache_hash = self._store_test_data(test_string)

        with self.routes__retrieve as _:
            # Retrieve as string
            result = _.retrieve__cache_id__string(cache_id, self.test_namespace)

            assert type(result) is Response
            assert result.body == test_string.encode()
            assert result.media_type == "text/plain"

    def test_retrieve__cache_id__json(self):                                        # Test JSON-specific retrieval
        test_json = {"test": "data", "number": 123, "nested": {"key": "value"}}
        cache_id, cache_hash = self._store_test_data(test_json, "json")

        with self.routes__retrieve as _:
            # Retrieve as JSON
            result = _.retrieve__cache_id__json(cache_id, self.test_namespace)

            assert result == test_json                                              # Direct JSON comparison

    def test_retrieve__cache_id__binary(self):                                      # Test binary-specific retrieval
        test_binary = b'\x01\x02\x03\x04\x05'
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        with self.routes__retrieve as _:
            # Retrieve as binary
            result = _.retrieve__cache_id__binary(cache_id, self.test_namespace)

            assert type(result) is Response
            assert result.body      == test_binary
            assert result.media_type == "application/octet-stream"

    def test_retrieve__hash_type_specific(self):                                    # Test type-specific hash retrieval
        test_data = {"key": "value", "list": [1, 2, 3]}
        cache_id, cache_hash = self._store_test_data(test_data, "json")

        with self.routes__retrieve as _:
            # Retrieve as JSON by hash
            result_json = _.retrieve__hash__cache_hash__json(cache_hash, self.test_namespace)
            assert result_json == test_data

            # Retrieve as string by hash (should stringify JSON)
            result_string = _.retrieve__hash__cache_hash__string(cache_hash, self.test_namespace)
            parsed_result = json.loads(result_string.body.decode())
            assert parsed_result == test_data

            # Retrieve as binary by hash
            result_binary = _.retrieve__hash__cache_hash__binary(cache_hash, self.test_namespace)
            parsed_result = json.loads(result_binary.body.decode())
            assert parsed_result == test_data

    def test_cross_type_retrieval(self):                                            # Test retrieving data as different types
        # Store string data
        test_string = "Simple test string"
        cache_id, cache_hash = self._store_test_data(test_string)

        with self.routes__retrieve as _:
            # Retrieve as string (native type)
            result_string = _.retrieve__cache_id__string(cache_id, self.test_namespace)
            assert result_string.body == test_string.encode()

            # Retrieve as JSON (should fail gracefully)
            result_json = _.retrieve__cache_id__json(cache_id, self.test_namespace)
            assert "error" in result_json
            assert result_json["error"] == "Data is not valid JSON"
            assert result_json["data"]  == test_string

            # Retrieve as binary (should convert)
            result_binary = _.retrieve__cache_id__binary(cache_id, self.test_namespace)
            assert result_binary.body == test_string.encode('utf-8')

    def test_retrieve_not_found_responses(self):                                    # Test 404 responses for type-specific endpoints
        non_existent_id   = Random_Guid()
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")

        with self.routes__retrieve as _:
            # String endpoints return 404 Response
            result_string_id = _.retrieve__cache_id__string(non_existent_id, self.test_namespace)
            assert result_string_id.status_code == 404
            assert result_string_id.body        == b"Not found"

            result_string_hash = _.retrieve__hash__cache_hash__string(non_existent_hash, self.test_namespace)
            assert result_string_hash.status_code == 404

            # JSON endpoints return JSON error
            result_json_id = _.retrieve__cache_id__json(non_existent_id, self.test_namespace)
            assert result_json_id == {"status": "not_found", "message": "Cache entry not found"}

            # Binary endpoints return 404 Response
            result_binary_id = _.retrieve__cache_id__binary(non_existent_id, self.test_namespace)
            assert result_binary_id.status_code == 404

    def test_binary_base64_fallback_for_non_utf8(self):                             # Test base64 encoding for non-UTF8 binary
        # Binary data that can't be decoded as UTF-8
        non_utf8_binary = b'\xff\xfe\x00\x01\x02\x03'
        cache_id, cache_hash = self._store_test_data(non_utf8_binary, "binary")

        with self.routes__retrieve as _:
            # Retrieve as string should fallback to base64
            result_string = _.retrieve__cache_id__string(cache_id, self.test_namespace)
            expected_base64 = base64.b64encode(non_utf8_binary).decode('utf-8')
            assert result_string.body == expected_base64.encode()

            # Retrieve as JSON should wrap in base64
            result_json = _.retrieve__cache_id__json(cache_id, self.test_namespace)
            assert result_json == {"data_type": "binary"              ,
                                  "encoding" : "base64"               ,
                                  "data"     : expected_base64        }

    def test_binary_redirect_message(self):                                         # Test binary data redirect message
        binary_data = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100                         # Fake PNG header
        cache_id, cache_hash = self._store_test_data(binary_data, "binary")

        with self.routes__retrieve as _:
            # Generic retrieve should redirect
            result = _.retrieve__cache_id(cache_id, self.test_namespace)
            assert result["status"]     == "binary_data"
            assert result["message"]    == "Binary data cannot be returned in JSON response"
            assert result["data_type"]  == "binary"
            assert result["size"]       == len(binary_data)
            assert result["binary_url"] == f"/{self.test_namespace}/retrieve/{cache_id}/binary"
            assert "data" not in result  # No actual data

            # Same for hash endpoint
            result_hash = _.retrieve__hash__cache_hash(cache_hash, self.test_namespace)
            assert result_hash["status"]     == "binary_data"
            assert result_hash["binary_url"] == f"/{self.test_namespace}/retrieve/hash/{cache_hash}/binary"

    def test_json_stored_retrieved_as_string(self):                                 # Test JSON data retrieved as string
        test_json = {"key": "value"}
        cache_id, cache_hash = self._store_test_data(test_json, "json")

        with self.routes__retrieve as _:
            # Retrieve as string by hash (should stringify JSON)
            result = _.retrieve__hash__cache_hash__string(cache_hash, self.test_namespace)

            #assert result.body       == json.dumps(test_json).encode()
            assert str_to_json(result.body.decode()) == test_json
            assert result.media_type                 == "text/plain"

    def test_retrieve__hash__cache_hash__json__string_data(self):                   # Test JSON retrieval of string data by hash
        test_string = '{"valid": "json", "as": "string"}'
        cache_id, cache_hash = self._store_test_data(test_string)

        with self.routes__retrieve as _:
            # Retrieve as JSON by hash (should parse string as JSON)
            result = _.retrieve__hash__cache_hash__json(cache_hash, self.test_namespace)

            assert result == {"valid": "json", "as": "string"}

    def test_retrieve__hash__cache_hash__not_found(self):                           # Test JSON retrieval by hash not found
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")

        with self.routes__retrieve as _:
            result = _.retrieve__hash__cache_hash__json(non_existent_hash, self.test_namespace)

            assert result == {"status": "not_found", "message": "Cache entry not found"}

    def test_binary_to_string_conversion(self):                                     # Test binary data retrieved as string
        # UTF-8 decodable binary
        utf8_binary = "Hello World 🌎".encode('utf-8')
        cache_id, cache_hash = self._store_test_data(utf8_binary, "binary")

        with self.routes__retrieve as _:
            # Retrieve as string - should decode UTF-8
            result_string = _.retrieve__cache_id__string(cache_id, self.test_namespace)

            assert result_string.body == utf8_binary  # Returns the raw bytes since it's binary type

    def test_json_to_binary_conversion(self):                                       # Test JSON data retrieved as binary
        test_json = {"test": "json", "number": 42}
        cache_id, cache_hash = self._store_test_data(test_json, "json")

        with self.routes__retrieve as _:
            # Retrieve as binary - should convert JSON to bytes
            result_binary = _.retrieve__cache_id__binary(cache_id, self.test_namespace)

            assert str_to_json(result_binary.body.decode())  == test_json
            assert result_binary.media_type == "application/octet-stream"

    def test_namespace_default_handling(self):                                      # Test default namespace handling
        test_data = "default namespace test"

        # Store in default namespace
        result = self.routes__store.store__string(data=test_data, namespace=None)
        cache_id = result.cache_id
        cache_hash = result.hash

        with self.routes__retrieve as _:
            # Retrieve without namespace (should use "default")
            result = _.retrieve__cache_id(cache_id, namespace=None)

            assert result["data"] == test_data

            # Check hash retrieval without namespace
            hash_result__none    = _.retrieve__hash__cache_hash(cache_hash, namespace=None     )
            hash_result__default = _.retrieve__hash__cache_hash(cache_hash, namespace='default')
            assert hash_result__none["data"] == test_data
            assert hash_result__none == hash_result__default

    def test_retrieve__hash__cache_hash__binary(self):                              # Test binary retrieval by hash
        binary_data = b'\x01\x02\x03\x04\x05'
        cache_id, cache_hash = self._store_test_data(binary_data, "binary")

        with self.routes__retrieve as _:
            # Retrieve as binary by hash
            result = _.retrieve__hash__cache_hash__binary(cache_hash, self.test_namespace)

            assert result.body       == binary_data
            assert result.media_type == "application/octet-stream"

    def test_retrieve__hash__cache_hash__binary__not_found(self):                   # Test binary retrieval by hash not found
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")

        with self.routes__retrieve as _:
            result = _.retrieve__hash__cache_hash__binary(non_existent_hash, self.test_namespace)

            assert result.status_code == 404
            assert result.body        == b"Not found"