import pytest
from unittest                                                                                      import TestCase
from fastapi                                                                                       import HTTPException, Response
from osbot_fast_api.api.routes.Fast_API__Routes                                                    import Fast_API__Routes
from osbot_utils.type_safe.Type_Safe                                                               import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                  import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                              import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                    import Safe_Str__Id
from osbot_utils.utils.Objects                                                                     import base_classes
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Retrieve                           import Routes__Data__Retrieve, TAG__ROUTES_RETRIEVE__DATA, PREFIX__ROUTES_RETRIEVE__DATA, ROUTES_PATHS__RETRIEVE__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store                              import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                              import Routes__File__Store
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Response     import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                     import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                          import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve                     import Cache__Service__Data__Retrieve
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store                        import Cache__Service__Data__Store
from tests.unit.Service__Cache__Test_Objs                                                          import setup__service__cache__test_objs


class test_Routes__Data__Retrieve(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_objs            = setup__service__cache__test_objs()
        cls.cache_fixtures       = cls.test_objs.cache_fixtures
        cls.cache_service        = cls.cache_fixtures.cache_service

        cls.routes_store         = Routes__File__Store   (cache_service = cls.cache_service)    # Parent entry routes
        cls.routes_data_store    = Routes__Data__Store   (cache_service = cls.cache_service)    # For test data creation
        cls.routes_data_retrieve = Routes__Data__Retrieve(cache_service = cls.cache_service)    # Route under test

        cls.service_data_store   = Cache__Service__Data__Store(cache_service = cls.cache_service)

        cls.test_namespace       = Safe_Str__Id("test-retrieve-data")                           # Test namespace
        cls.test_data_key        = Safe_Str__File__Path("configs/app")                          # Hierarchical path

        cls.parent_cache_id      = cls._create_parent_cache_entry(cls)                          # Create parent entry once
        cls._setup_test_data()                                                               # Store test data for retrieval

    @classmethod
    def _create_parent_cache_entry(cls, self):                                                  # Helper to create parent cache entry
        response = cls.routes_store.store__string(data      = "parent for retrieval"                   ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL   ,
                                                  namespace = cls.test_namespace                      )
        return response.cache_id

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup test data files
        # Store string data with ID only
        cls.string_response_1 = cls.routes_data_store.data__store_string__with__id(
            data         = "test string content",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("string-001")
        )

        # Store string data with ID and key
        cls.string_response_2 = cls.routes_data_store.data__store_string__with__id_and_key(
            data         = "nested string content",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("string-002")
        )

        # Store JSON data with ID only
        cls.json_response_1 = cls.routes_data_store.data__store_json__with__id(
            data         = {"status": "active", "count": 42},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("json-001")
        )

        # Store JSON data with ID and key
        cls.json_response_2 = cls.routes_data_store.data__store_json__with__id_and_key(
            data         = {"nested": "data", "items": [1, 2, 3]},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("json-002")
        )

        # Store binary data with ID only
        cls.binary_response_1 = cls.routes_data_store.data__store_binary__with__id(
            body         = b'\x89PNG\r\n\x1a\n' + b'\x00' * 10,
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("binary-001")
        )

        # Store binary data with ID and key
        cls.binary_response_2 = cls.routes_data_store.data__store_binary__with__id_and_key(
            body         = b'binary content with \x00\x01\x02',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("binary-002")
        )

    def test__init__(self):                                                                      # Test initialization and structure
        with Routes__Data__Retrieve() as _:
            assert type(_)                       is Routes__Data__Retrieve
            assert base_classes(_)               == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                         == TAG__ROUTES_RETRIEVE__DATA
            assert _.prefix                      == PREFIX__ROUTES_RETRIEVE__DATA
            assert type(_.cache_service)         is Cache__Service
            assert type(_.retrieve_service())    is Cache__Service__Data__Retrieve

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_RETRIEVE__DATA       == 'data'
        assert PREFIX__ROUTES_RETRIEVE__DATA    == '/{namespace}/cache/{cache_id}'
        assert len(ROUTES_PATHS__RETRIEVE__DATA) == 6
        assert ROUTES_PATHS__RETRIEVE__DATA[0]  == '/{namespace}/cache/{cache_id}/data/json/{data_file_id}'
        assert ROUTES_PATHS__RETRIEVE__DATA[1]  == '/{namespace}/cache/{cache_id}/data/json/{data_key:path}/{data_file_id}'

    def test_data__json__with__id(self):                                                        # Test JSON retrieval with ID only
        with self.routes_data_retrieve as _:
            result = _.data__json__with__id(cache_id     = self.parent_cache_id,
                                           namespace    = self.test_namespace,
                                           data_file_id = Safe_Str__Id("json-001"))

            assert type(result) is dict
            assert result       == {"status": "active", "count": 42}

    def test_data__json__with__id_and_key(self):                                                # Test JSON retrieval with ID and key
        with self.routes_data_retrieve as _:
            result = _.data__json__with__id_and_key(cache_id     = self.parent_cache_id,
                                                   namespace    = self.test_namespace,
                                                   data_key     = self.test_data_key,
                                                   data_file_id = Safe_Str__Id("json-002"))

            assert type(result) is dict
            assert result       == {"nested": "data", "items": [1, 2, 3]}

    def test_data__json__not_found(self):                                                       # Test JSON 404 error
        with self.routes_data_retrieve as _:
            with pytest.raises(HTTPException) as exc_info:
                _.data__json__with__id(cache_id     = self.parent_cache_id,
                                      namespace    = self.test_namespace,
                                      data_file_id = Safe_Str__Id("missing-json"))

            error = exc_info.value
            assert error.status_code              == 404
            assert error.detail["error_type"]     == "NOT_FOUND"
            assert error.detail["message"]        == "Data file not found"
            assert error.detail["data_file_id"]   == "missing-json"

    def test_data__json__wrong_type(self):                                                      # Test JSON 415 error for wrong type
        with self.routes_data_retrieve as _:
            with pytest.raises(HTTPException) as exc_info:                                      # Try to retrieve a string file as JSON
                _.data__json__with__id(cache_id     = self.parent_cache_id,
                                      namespace    = self.test_namespace,
                                      data_file_id = Safe_Str__Id("string-001"))

            error = exc_info.value
            assert error.status_code              == 404
            assert error.detail["error_type"]     == "NOT_FOUND"

    def test_data__string__with__id(self):                                                      # Test string retrieval with ID only
        with self.routes_data_retrieve as _:
            result = _.data__string__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("string-001"))

            assert type(result)         is Response
            assert result.status_code   == 200
            assert result.body          == b"test string content"
            assert result.media_type    == "text/plain"

    def test_data__string__with__id_and_key(self):                                              # Test string retrieval with ID and key
        with self.routes_data_retrieve as _:
            result = _.data__string__with__id_and_key(cache_id     = self.parent_cache_id,
                                                     namespace    = self.test_namespace,
                                                     data_key     = self.test_data_key,
                                                     data_file_id = Safe_Str__Id("string-002"))

            assert type(result)         is Response
            assert result.status_code   == 200
            assert result.body          == b"nested string content"

    def test_data__string__not_found(self):                                                     # Test string 404 response
        with self.routes_data_retrieve as _:
            result = _.data__string__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("missing-string"))

            assert type(result)       is Response
            assert result.status_code == 404
            assert result.body        == b""

    def test_data__string__wrong_type(self):                                                    # Test string 415 response for wrong type
        with self.routes_data_retrieve as _:
            result = _.data__string__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("json-001"))

            assert type(result)       is Response
            assert result.status_code   == 404
            assert result.body          == b""

    def test_data__binary__with__id(self):                                                      # Test binary retrieval with ID only
        with self.routes_data_retrieve as _:
            result = _.data__binary__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("binary-001"))

            assert type(result)         is Response
            assert result.status_code   == 200
            assert result.body          == b'\x89PNG\r\n\x1a\n' + b'\x00' * 10
            assert result.media_type    == "application/octet-stream"

    def test_data__binary__with__id_and_key(self):                                              # Test binary retrieval with ID and key
        with self.routes_data_retrieve as _:
            result = _.data__binary__with__id_and_key(cache_id     = self.parent_cache_id,
                                                     namespace    = self.test_namespace,
                                                     data_key     = self.test_data_key,
                                                     data_file_id = Safe_Str__Id("binary-002"))

            assert type(result)         is Response
            assert result.status_code   == 200
            assert result.body          == b'binary content with \x00\x01\x02'

    def test_data__binary__not_found(self):                                                     # Test binary 404 response
        with self.routes_data_retrieve as _:
            result = _.data__binary__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("missing-binary"))

            assert type(result)       is Response
            assert result.status_code == 404
            assert result.body        == b""

    def test_data__binary__wrong_type(self):                                                    # Test binary 415 response for wrong type
        with self.routes_data_retrieve as _:
            result = _.data__binary__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("string-001"))

            assert type(result)       is Response
            assert result.status_code   == 404
            assert result.body          == b""

    def test__delegation_pattern(self):                                                         # Test methods properly delegate
        with self.routes_data_retrieve as _:
            # Test that with__id delegates to with__id_and_key with empty data_key
            result1 = _.data__json__with__id(cache_id     = self.parent_cache_id,
                                            namespace    = self.test_namespace,
                                            data_file_id = Safe_Str__Id("json-001"))

            # This should be equivalent to calling with empty data_key
            result2 = _.data__json__with__id_and_key(cache_id     = self.parent_cache_id,
                                                    namespace    = self.test_namespace,
                                                    data_key     = Safe_Str__File__Path(""),
                                                    data_file_id = Safe_Str__Id("json-001"))

            assert result1 == result2                                                           # Same result from both methods

    def test_handle_not_found(self):                                                           # Test error handling helper
        with self.routes_data_retrieve as _:
            # Test with None result
            with pytest.raises(HTTPException) as exc_info:
                _.handle_not_found(result       = None,
                                  cache_id      = self.parent_cache_id,
                                  data_file_id  = Safe_Str__Id("test"))

            assert exc_info.value.status_code == 404
            assert exc_info.value.detail["error_type"] == "NOT_FOUND"

            # Test with found=False result
            mock_result = Schema__Cache__Data__Retrieve__Response(found=False)
            with pytest.raises(HTTPException) as exc_info:
                _.handle_not_found(mock_result, self.parent_cache_id, Safe_Str__Id("test"))

            assert exc_info.value.status_code == 404

    def test_handle_json_result(self):                                                         # Test JSON result handler
        with self.routes_data_retrieve as _:
            mock_result = Schema__Cache__Data__Retrieve__Response(found     = True                       ,      # Test successful case
                                                                  data_type = Enum__Cache__Data_Type.JSON,
                                                                  data      = {"test": "data"}           )

            result = _.handle_json_result(mock_result, self.parent_cache_id, Safe_Str__Id("test"))
            assert result == {"test": "data"}

            mock_result.found = False                                                                           # Test not found case
            with pytest.raises(HTTPException) as exc_info:
                _.handle_json_result(mock_result, self.parent_cache_id, Safe_Str__Id("test"))
            assert exc_info.value.status_code == 404

    def test_handle_string_result(self):                                                       # Test string result handler
        with self.routes_data_retrieve as _:
            # Test successful case
            mock_result = Schema__Cache__Data__Retrieve__Response(
                found     = True,
                data_type = Enum__Cache__Data_Type.STRING,
                data      = "test string"
            )

            result = _.handle_string_result(mock_result)
            assert type(result)       is Response
            assert result.status_code == 200
            assert result.body        == b"test string"
            assert result.media_type  == "text/plain"

            # Test not found case
            mock_result.found = False
            result = _.handle_string_result(mock_result)
            assert result.status_code == 404
            assert result.body        == b""

    def test_handle_binary_result(self):                                                       # Test binary result handler
        with self.routes_data_retrieve as _:
            # Test successful case
            mock_result = Schema__Cache__Data__Retrieve__Response(
                found     = True,
                data_type = Enum__Cache__Data_Type.BINARY,
                data      = b"test binary"
            )

            result = _.handle_binary_result(mock_result)
            assert type(result)       is Response
            assert result.status_code == 200
            assert result.body        == b"test binary"
            assert result.media_type  == "application/octet-stream"

            # Test not found case
            mock_result.found = False
            result = _.handle_binary_result(mock_result)
            assert result.status_code == 404
            assert result.body        == b""

    def test_retrieve__with_special_characters(self):                                          # Test handling special characters
        with self.routes_data_retrieve as _:
            # Store data with special characters
            special_data = "Data with 中文 and émojis 🚀"
            response = self.routes_data_store.data__store_string__with__id(
                data         = special_data,
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("special-chars")
            )

            # Retrieve it back
            result = _.data__string__with__id(cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace,
                                             data_file_id = Safe_Str__Id("special-chars"))

            assert result.body.decode('utf-8') == special_data

    def test_retrieve__empty_data_key(self):                                                   # Test empty data_key behavior
        with self.routes_data_retrieve as _:
            # with__id should work same as with__id_and_key with empty key
            result1 = _.data__json__with__id(cache_id     = self.parent_cache_id,
                                            namespace    = self.test_namespace,
                                            data_file_id = Safe_Str__Id("json-001"))

            result2 = _.data__json__with__id_and_key(cache_id     = self.parent_cache_id,
                                                    namespace    = self.test_namespace,
                                                    data_key     = None,  # None converts to empty
                                                    data_file_id = Safe_Str__Id("json-001"))

            assert result1 == result2

    def test_retrieve__parent_not_found(self):                                                 # Test with non-existent parent
        with self.routes_data_retrieve as _:
            non_existent_id = Random_Guid()

            with pytest.raises(HTTPException) as exc_info:
                _.data__json__with__id(cache_id     = non_existent_id,
                                      namespace    = self.test_namespace,
                                      data_file_id = Safe_Str__Id("any"))

            assert exc_info.value.status_code == 404

    def test_retrieve__cross_type_errors(self):                                                # Test retrieving data as wrong types
        with self.routes_data_retrieve as _:
            # Try to get JSON as string (returns 404 Response)
            string_result = _.data__string__with__id(cache_id     = self.parent_cache_id,
                                                    namespace    = self.test_namespace,
                                                    data_file_id = Safe_Str__Id("json-001"))
            assert string_result.status_code == 404

            # Try to get binary as JSON (raises HTTPException)
            with pytest.raises(HTTPException) as exc_info:
                _.data__json__with__id(cache_id     = self.parent_cache_id,
                                      namespace    = self.test_namespace,
                                      data_file_id = Safe_Str__Id("binary-001"))
            assert exc_info.value.status_code == 404

            # Try to get string as binary (returns 404 Response)
            binary_result = _.data__binary__with__id(cache_id     = self.parent_cache_id,
                                                    namespace    = self.test_namespace,
                                                    data_file_id = Safe_Str__Id("string-001"))
            assert binary_result.status_code == 404

    def test__integration__store_and_retrieve_cycle(self):                                     # Full integration test
        with self.routes_data_retrieve as _:
            # Store multiple data types
            test_string = "integration test string"
            test_json   = {"integration": "test", "values": [1, 2, 3]}
            test_binary = b"integration\x00test\x01binary"
            test_key    = Safe_Str__File__Path("integration/test")

            # Store them
            store_string = self.routes_data_store.data__store_string__with__id_and_key(
                data         = test_string,
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-str")
            )

            store_json = self.routes_data_store.data__store_json__with__id_and_key(
                data         = test_json,
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-json")
            )

            store_binary = self.routes_data_store.data__store_binary__with__id_and_key(
                body         = test_binary,
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-bin")
            )

            # Retrieve them back
            retrieved_string = _.data__string__with__id_and_key(
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-str")
            )

            retrieved_json = _.data__json__with__id_and_key(
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-json")
            )

            retrieved_binary = _.data__binary__with__id_and_key(
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-bin")
            )

            # Verify data integrity
            assert retrieved_string.body           == test_string.encode('utf-8')
            assert retrieved_json                  == test_json
            assert retrieved_binary.body           == test_binary