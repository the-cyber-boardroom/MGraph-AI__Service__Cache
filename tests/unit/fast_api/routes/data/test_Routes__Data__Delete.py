import pytest
from unittest                                                                               import TestCase
from fastapi                                                                                import HTTPException
from osbot_fast_api.api.routes.Fast_API__Routes                                             import Fast_API__Routes
from osbot_utils.testing.__                                                                 import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.utils.Objects                                                              import base_classes
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Delete                      import Routes__Data__Delete, TAG__ROUTES_DELETE__DATA, PREFIX__ROUTES_DELETE__DATA, ROUTES_PATHS__DELETE__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Retrieve                    import Routes__Data__Retrieve
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store                       import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                       import Routes__File__Store
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                     import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Delete                import Cache__Service__Data__Delete
from tests.unit.Service__Cache__Test_Objs                                                   import setup__service__cache__test_objs


class test_Routes__Data__Delete(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs            = setup__service__cache__test_objs()
        cls.cache_fixtures       = cls.test_objs.cache_fixtures
        cls.cache_service        = cls.cache_fixtures.cache_service

        cls.routes_store         = Routes__File__Store   (cache_service = cls.cache_service)    # Parent entry routes
        cls.routes_data_store    = Routes__Data__Store   (cache_service = cls.cache_service)    # For test data creation
        cls.routes_data_retrieve = Routes__Data__Retrieve(cache_service = cls.cache_service)    # For verification
        cls.routes_data_delete   = Routes__Data__Delete  (cache_service = cls.cache_service)    # Route under test

        cls.test_namespace       = Safe_Str__Id("test-delete-data")                             # Test namespace
        cls.test_data_key        = Safe_Str__File__Path("configs/app")                          # Hierarchical path

        cls.parent_cache_id      = cls._create_parent_cache_entry(cls)                          # Create parent entry once
        cls._setup_test_data()                                                               # Store test data for deletion

    @classmethod
    def _create_parent_cache_entry(cls, self):                                                  # Helper to create parent cache entry
        response = cls.routes_store.store__string(data      = "parent for deletion"                    ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL   ,
                                                  namespace = cls.test_namespace                      )
        return response.cache_id

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup test data files for deletion
        # Store string data with ID only
        cls.string_response_1 = cls.routes_data_store.data__store_string__with__id(
            data         = "delete test string 1",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("string-001")
        )

        # Store string data with ID and key
        cls.string_response_2 = cls.routes_data_store.data__store_string__with__id_and_key(
            data         = "delete test string 2",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("string-002")
        )

        # Store JSON data with ID only
        cls.json_response_1 = cls.routes_data_store.data__store_json__with__id(
            data         = {"delete": "me", "value": 42},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("json-001")
        )

        # Store JSON data with ID and key
        cls.json_response_2 = cls.routes_data_store.data__store_json__with__id_and_key(
            data         = {"nested": "delete", "items": [1, 2, 3]},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("json-002")
        )

        # Store binary data with ID only
        cls.binary_response_1 = cls.routes_data_store.data__store_binary__with__id(
            body         = b'DELETE\x00ME\x01BINARY',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("binary-001")
        )

        # Store binary data with ID and key
        cls.binary_response_2 = cls.routes_data_store.data__store_binary__with__id_and_key(
            body         = b'delete binary with key \x00\x01\x02',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("binary-002")
        )

    def test__init__(self):                                                                      # Test initialization and structure
        with Routes__Data__Delete() as _:
            assert type(_)                     is Routes__Data__Delete
            assert base_classes(_)             == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                       == TAG__ROUTES_DELETE__DATA
            assert _.prefix                    == PREFIX__ROUTES_DELETE__DATA
            assert type(_.cache_service)       is Cache__Service
            assert type(_.delete_service())    is Cache__Service__Data__Delete

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_DELETE__DATA       == 'data'
        assert PREFIX__ROUTES_DELETE__DATA    == '/{namespace}/cache/{cache_id}'
        assert len(ROUTES_PATHS__DELETE__DATA) == 4
        assert ROUTES_PATHS__DELETE__DATA[0]  == '/{namespace}/cache/{cache_id}/data/delete/{data_type}/{data_file_id}'
        assert ROUTES_PATHS__DELETE__DATA[1]  == '/{namespace}/cache/{cache_id}/data/delete/{data_type}/{data_key:path}/{data_file_id}'
        assert ROUTES_PATHS__DELETE__DATA[2]  == '/{namespace}/cache/{cache_id}/data/delete/all'
        assert ROUTES_PATHS__DELETE__DATA[3]  == '/{namespace}/cache/{cache_id}/data/delete/all/{data_key:path}'

    def test_delete__data__file__with__id(self):                                                # Test deletion with ID only
        with self.routes_data_delete as _:
            # Create fresh data to delete
            parent = self.routes_store.store__string(data      = "delete test parent"         ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                     namespace = self.test_namespace          )

            self.routes_data_store.data__store_string__with__id(data         = "to be deleted"    ,
                                                                cache_id     = parent.cache_id    ,
                                                                namespace    = self.test_namespace,
                                                                data_file_id = Safe_Str__Id("del-001"))

            # Delete it
            result = _.delete__data__file__with__id(cache_id     = parent.cache_id              ,
                                                   namespace    = self.test_namespace          ,
                                                   data_type    = Enum__Cache__Data_Type.STRING,
                                                   data_file_id = Safe_Str__Id("del-001")      )

            assert result == { "status"        : "success"                                      ,
                              "message"       : "Data file deleted successfully"               ,
                              "cache_id"      : str(parent.cache_id)                          ,
                              "data_file_id"  : "del-001"                                     ,
                              "data_type"     : "string"                                      ,
                              "data_key"      : None                                          ,
                              "namespace"     : "test-delete-data"                            }

            # Verify deletion
            retrieve_result = self.routes_data_retrieve.data__string__with__id(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("del-001")
            )
            assert retrieve_result.status_code == 404

    def test_delete__data__file__with__id_and_key(self):                                        # Test deletion with ID and key
        with self.routes_data_delete as _:
            # Create fresh data with key
            parent = self.routes_store.store__string(data      = "delete with key parent"     ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                     namespace = self.test_namespace          )

            self.routes_data_store.data__store_json__with__id_and_key(
                data         = {"delete": "this"},
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_key     = self.test_data_key,
                data_file_id = Safe_Str__Id("del-json")
            )

            # Delete it
            result = _.delete__data__file__with__id_and_key(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_type    = Enum__Cache__Data_Type.JSON,
                data_key     = self.test_data_key,
                data_file_id = Safe_Str__Id("del-json")
            )

            assert result["status"]       == "success"
            assert result["data_file_id"] == "del-json"
            assert result["data_type"]    == "json"
            assert result["data_key"]     == str(self.test_data_key)

            # Verify deletion
            with pytest.raises(HTTPException) as exc_info:
                self.routes_data_retrieve.data__json__with__id_and_key(
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_key     = self.test_data_key,
                    data_file_id = Safe_Str__Id("del-json")
                )
            assert exc_info.value.status_code == 404

    def test_delete__data__file__not_found(self):                                               # Test 404 when file doesn't exist
        with self.routes_data_delete as _:
            with pytest.raises(HTTPException) as exc_info:
                _.delete__data__file__with__id(cache_id     = self.parent_cache_id              ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.STRING     ,
                                              data_file_id = Safe_Str__Id("non-existent")      )

            error = exc_info.value
            assert error.status_code              == 404
            assert error.detail["error_type"]     == "NOT_FOUND"
            assert error.detail["message"]        == "Data file non-existent not found"
            assert error.detail["data_file_id"]   == "non-existent"

    def test_delete__data__file__missing_data_file_id(self):                                    # Test 400 for missing data_file_id
        with self.routes_data_delete as _:
            with pytest.raises(HTTPException) as exc_info:
                _.delete__data__file__with__id(cache_id  = self.parent_cache_id              ,
                                              namespace = self.test_namespace                ,
                                              data_type = Enum__Cache__Data_Type.STRING     ,
                                              data_file_id = None                           )

            error = exc_info.value
            assert error.status_code          == 400
            assert error.detail["error_type"] == "INVALID_INPUT"
            assert "data_file_id is required" in error.detail["message"]

    def test_delete__data__file__missing_data_type(self):                                       # Test 400 for missing data_type
        with self.routes_data_delete as _:
            with pytest.raises(HTTPException) as exc_info:
                _.delete__data__file__with__id(cache_id     = self.parent_cache_id         ,
                                              namespace    = self.test_namespace           ,
                                              data_type    = None                         ,
                                              data_file_id = Safe_Str__Id("test")        )

            error = exc_info.value
            assert error.status_code          == 400
            assert error.detail["error_type"] == "INVALID_INPUT"
            assert "data_type is required" in error.detail["message"]

    def test_delete__data__file__wrong_type(self):                                              # Test type mismatch prevents deletion
        with self.routes_data_delete as _:
            # Try to delete JSON file with STRING type
            with pytest.raises(HTTPException) as exc_info:
                _.delete__data__file__with__id(cache_id     = self.parent_cache_id              ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.STRING     ,  # Wrong type!
                                              data_file_id = Safe_Str__Id("json-001")          )

            assert exc_info.value.status_code == 404                                            # Not found with wrong type

            # Verify JSON file still exists with correct type
            result = self.routes_data_retrieve.data__json__with__id(
                cache_id     = self.parent_cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("json-001")
            )
            assert result == {"delete": "me", "value": 42}

    def test_delete__all__data__files(self):                                                    # Test deleting all files
        with self.routes_data_delete as _:
            # Create parent with multiple files
            parent = self.routes_store.store__string(data      = "parent for bulk delete"     ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                     namespace = self.test_namespace          )

            # Store multiple files
            for i in range(3):
                self.routes_data_store.data__store_string__with__id(
                    data         = f"bulk delete {i}",
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_file_id = Safe_Str__Id(f"bulk-{i}")
                )

            # Delete all
            result = _.delete__all__data__files(cache_id  = parent.cache_id   ,
                                               namespace = self.test_namespace)

            assert result["status"]        == "success"
            assert result["message"]       == "Deleted 3 data files"
            assert result["deleted_count"] == 3
            assert len(result["deleted_files"]) == 3
            assert result["data_key"]      is None

            # Verify all deleted
            for i in range(3):
                retrieve_result = self.routes_data_retrieve.data__string__with__id(
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_file_id = Safe_Str__Id(f"bulk-{i}")
                )
                assert retrieve_result.status_code == 404

    def test_delete__all__data__files__with__key(self):                                         # Test deleting files under specific key
        with self.routes_data_delete as _:
            # Create parent with categorized files
            parent = self.routes_store.store__string(data      = "categorized parent"         ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                     namespace = self.test_namespace          )

            # Store files in "logs" key
            for i in range(2):
                self.routes_data_store.data__store_string__with__id_and_key(
                    data         = f"log {i}",
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_key     = Safe_Str__File__Path("logs"),
                    data_file_id = Safe_Str__Id(f"log-{i}")
                )

            # Store files in "configs" key
            for i in range(2):
                self.routes_data_store.data__store_json__with__id_and_key(
                    data         = {"config": i},
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_key     = Safe_Str__File__Path("configs"),
                    data_file_id = Safe_Str__Id(f"config-{i}")
                )

            # Delete only "logs" files
            result = _.delete__all__data__files__with__key(
                cache_id  = parent.cache_id,
                namespace = self.test_namespace,
                data_key  = Safe_Str__File__Path("logs")
            )

            assert result["status"]        == "success"
            assert result["deleted_count"] == 2
            assert result["data_key"]      == "logs"

            # Verify logs deleted
            for i in range(2):
                retrieve_result = self.routes_data_retrieve.data__string__with__id_and_key(
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_key     = Safe_Str__File__Path("logs"),
                    data_file_id = Safe_Str__Id(f"log-{i}")
                )
                assert retrieve_result.status_code == 404

            # Verify configs still exist
            for i in range(2):
                config_data = self.routes_data_retrieve.data__json__with__id_and_key(
                    cache_id     = parent.cache_id,
                    namespace    = self.test_namespace,
                    data_key     = Safe_Str__File__Path("configs"),
                    data_file_id = Safe_Str__Id(f"config-{i}")
                )
                assert config_data == {"config": i}

    def test_delete__all__data__files__no_files(self):                                          # Test deleting when no files exist
        with self.routes_data_delete as _:
            # Create empty parent
            parent = self.routes_store.store__string(data      = "empty parent"              ,
                                                     strategy  = Enum__Cache__Store__Strategy.DIRECT ,
                                                     namespace = self.test_namespace          )

            result = _.delete__all__data__files(cache_id  = parent.cache_id   ,
                                               namespace = self.test_namespace)

            assert result["status"]        == "success"
            assert result["message"]       == "No data files to delete"
            assert result["deleted_count"] == 0
            assert "deleted_files"    not in result                                             # No files list when empty

    def test_delete__all__data__files__invalid_parent(self):                                    # Test with non-existent parent
        with self.routes_data_delete as _:
            non_existent_id = Random_Guid()

            result = _.delete__all__data__files(cache_id  = non_existent_id   ,
                                               namespace = self.test_namespace)

            assert result["status"]        == "success"
            assert result["message"]       == "No data files to delete"
            assert result["deleted_count"] == 0

    def test__delegation_pattern(self):                                                         # Test methods properly delegate
        with self.routes_data_delete as _:
            # Create test data
            parent = self.routes_store.store__string(data      = "delegation test"           ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                     namespace = self.test_namespace          )

            self.routes_data_store.data__store_string__with__id(
                data         = "test delegation",
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("deleg-001")
            )

            # Test that with__id delegates to with__id_and_key with empty key
            result = _.delete__data__file__with__id(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_file_id = Safe_Str__Id("deleg-001")
            )

            assert result["status"]   == "success"
            assert result["data_key"] is None                                                   # Empty key in delegation

    def test_delete__different_data_types(self):                                                # Test deleting each data type
        with self.routes_data_delete as _:
            parent = self.routes_store.store__string(data      = "multi-type parent"         ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                                     namespace = self.test_namespace          )

            # Store different types
            self.routes_data_store.data__store_string__with__id(
                data         = "string data",
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("type-str")
            )

            self.routes_data_store.data__store_json__with__id(
                data         = {"json": "data"},
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("type-json")
            )

            self.routes_data_store.data__store_binary__with__id(
                body         = b"binary data",
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("type-bin")
            )

            # Delete each type
            string_result = _.delete__data__file__with__id(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_file_id = Safe_Str__Id("type-str")
            )
            assert string_result["data_type"] == "string"

            json_result = _.delete__data__file__with__id(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_type    = Enum__Cache__Data_Type.JSON,
                data_file_id = Safe_Str__Id("type-json")
            )
            assert json_result["data_type"] == "json"

            binary_result = _.delete__data__file__with__id(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_type    = Enum__Cache__Data_Type.BINARY,
                data_file_id = Safe_Str__Id("type-bin")
            )
            assert binary_result["data_type"] == "binary"

    def test__integration__store_retrieve_delete_cycle(self):                                   # Full integration test
        with self.routes_data_delete as _:
            # Create parent
            parent = self.routes_store.store__string(
                data      = "integration test parent",
                namespace = self.test_namespace,
                strategy  = Enum__Cache__Store__Strategy.TEMPORAL
            )

            # Store multiple data types with hierarchy
            test_key = Safe_Str__File__Path("integration/test")

            # Store string
            self.routes_data_store.data__store_string__with__id_and_key(
                data         = "integration string",
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-str")
            )

            # Store JSON
            self.routes_data_store.data__store_json__with__id_and_key(
                data         = {"integration": "json", "test": True},
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-json")
            )

            # Store binary
            self.routes_data_store.data__store_binary__with__id_and_key(
                body         = b"integration\x00binary",
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-bin")
            )

            # Verify all exist
            string_data = self.routes_data_retrieve.data__string__with__id_and_key(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-str")
            )
            assert string_data.body == b"integration string"

            json_data = self.routes_data_retrieve.data__json__with__id_and_key(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-json")
            )
            assert json_data == {"integration": "json", "test": True}

            # Delete individually
            _.delete__data__file__with__id_and_key(
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = test_key,
                data_file_id = Safe_Str__Id("integ-str")
            )

            # Delete remaining with delete all
            result = _.delete__all__data__files__with__key(
                cache_id  = parent.cache_id,
                namespace = self.test_namespace,
                data_key  = test_key
            )

            assert result["deleted_count"] == 2                                                 # JSON and binary deleted

            # Verify all deleted
            for file_id in ["integ-str", "integ-json", "integ-bin"]:
                with pytest.raises(HTTPException):
                    self.routes_data_retrieve.data__json__with__id_and_key(
                        cache_id     = parent.cache_id,
                        namespace    = self.test_namespace,
                        data_key     = test_key,
                        data_file_id = Safe_Str__Id(file_id)
                    )

    def test_setup_routes(self):                                                                # Test route setup
        with self.routes_data_delete as _:
            result = _.setup_routes()
            assert result is _                                                                  # Returns self for chaining

            # Verify routes are registered (would need app instance to fully test)
            # This at least ensures the method executes without error