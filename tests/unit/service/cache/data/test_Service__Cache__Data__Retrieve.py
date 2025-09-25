import pytest
from unittest                                                                           import TestCase
from osbot_utils.testing.__                                                             import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                   import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id         import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path       import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.core.Safe_UInt                                    import Safe_UInt
from osbot_utils.utils.Objects                                                          import base_classes
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                 import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy           import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Store__Request     import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request  import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Response import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store             import Cache__Service__Data__Store
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve          import Cache__Service__Data__Retrieve
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve            import Cache__Service__Retrieve
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                  import Cache__Service__Store
from tests.unit.Service__Cache__Test_Objs                                               import setup__service__cache__test_objs


class test_Cache__Service__Data__Retrieve(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs             = setup__service__cache__test_objs()
        cls.cache_fixtures        = cls.test_objs.cache_fixtures
        cls.service__cache        = cls.cache_fixtures.cache_service
        cls.service__store        = Cache__Service__Store        (cache_service = cls.service__cache)
        cls.service__retrieve     = Cache__Service__Retrieve     (cache_service = cls.service__cache)
        cls.service__store_data   = Cache__Service__Data__Store  (cache_service = cls.service__cache)
        cls.service__retrieve_data = Cache__Service__Data__Retrieve(cache_service = cls.service__cache)

        cls.test_namespace = Safe_Str__Id("test-data-retrieve")                                 # Test data setup
        cls.test_cache_key = Safe_Str__File__Path("app/data")

        # Create parent cache entry
        cls.parent_response = cls.service__store.store_string(data      = "parent for retrieval"                    ,
                                                              namespace = cls.test_namespace                        ,
                                                              strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                              cache_key = cls.test_cache_key                        ,
                                                              file_id   = Safe_Str__Id("parent-retrieve")           )
        cls.parent_cache_id = cls.parent_response.cache_id
        cls.test_data       = {}                                                                # Store test data files for retrieval

        # Store string data
        string_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id          ,
                                                             data         = "test string content"        ,
                                                             data_type    = Enum__Cache__Data_Type.STRING,
                                                             data_key     = Safe_Str__File__Path("logs") ,
                                                             data_file_id = Safe_Str__Id("log-001")      ,
                                                             namespace    = cls.test_namespace           )
        cls.test_data['string'] = cls.service__store_data.store_data(string_request)

        # Store JSON data
        json_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id,
                                                           data         = {"status": "active", "count": 42},
                                                           data_type    = Enum__Cache__Data_Type.JSON,
                                                           data_key     = Safe_Str__File__Path("configs"),
                                                           data_file_id = Safe_Str__Id("config-001"),
                                                           namespace    = cls.test_namespace)
        cls.test_data['json'] = cls.service__store_data.store_data(json_request)

        # Store binary data
        binary_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id,
                                                             data         = b"binary content \x00\x01\x02",
                                                             data_type    = Enum__Cache__Data_Type.BINARY,
                                                             data_key     = Safe_Str__File__Path("attachments"),
                                                             data_file_id = Safe_Str__Id("binary-001"),
                                                             namespace    = cls.test_namespace)
        cls.test_data['binary'] = cls.service__store_data.store_data(binary_request)

    def test__init__(self):                                                                     # Test auto-initialization
        with Cache__Service__Data__Retrieve() as _:
            assert type(_)               is Cache__Service__Data__Retrieve
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

    def test_retrieve_service(self):                                                            # Test retrieve service caching
        with self.service__retrieve_data as _:
            retrieve1 = _.retrieve_service()
            retrieve2 = _.retrieve_service()

            assert retrieve1 is retrieve2                                                       # Same instance (cached)
            assert type(retrieve1) is Cache__Service__Retrieve

    def test_retrieve_data__string(self):                                                       # Test retrieving string data
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id,
                                                             data_type    = Enum__Cache__Data_Type.STRING,
                                                             data_key     = Safe_Str__File__Path("logs"),
                                                             data_file_id = Safe_Str__Id("log-001"),
                                                             namespace    = self.test_namespace)

            result = _.retrieve_data(request)

            assert type(result)      is Schema__Cache__Data__Retrieve__Response
            assert result.data       == "test string content"
            assert result.data_type  == Enum__Cache__Data_Type.STRING
            assert result.data_file_id == Safe_Str__Id("log-001")
            assert result.data_key   == Safe_Str__File__Path("logs")
            assert result.size       == Safe_UInt(len("test string content"))
            assert result.found      is True
            assert result.obj()      == __(data         = 'test string content' ,
                                           data_type    = 'string'              ,
                                           data_file_id = 'log-001'             ,
                                           data_key     = 'logs'                ,
                                           full_path    = 'test-data-retrieve/data/semantic-file/app/data/parent-retrieve/data/logs/log-001.txt',
                                           size         = 19                    ,
                                           found        = True                  )

    def test_retrieve_data__json(self):                                                         # Test retrieving JSON data
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id            ,
                                                             data_type    = Enum__Cache__Data_Type.JSON     ,
                                                             data_key     = Safe_Str__File__Path("configs") ,
                                                             data_file_id = Safe_Str__Id("config-001")      ,
                                                             namespace    = self.test_namespace             )

            result = _.retrieve_data(request)

            assert type(result)      is Schema__Cache__Data__Retrieve__Response
            assert result.data       == {"status": "active", "count": 42}
            assert result.data_type  == Enum__Cache__Data_Type.JSON
            assert result.found      is True

    def test_retrieve_data__binary(self):                                                       # Test retrieving binary data
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.BINARY,
                data_key     = Safe_Str__File__Path("attachments"),
                data_file_id = Safe_Str__Id("binary-001"),
                namespace    = self.test_namespace
            )

            result = _.retrieve_data(request)

            assert type(result)      is Schema__Cache__Data__Retrieve__Response
            assert result.data       == b"binary content \x00\x01\x02"
            assert result.data_type  == Enum__Cache__Data_Type.BINARY
            assert result.size       == Safe_UInt(len(b"binary content \x00\x01\x02"))
            assert result.found      is True

    def test_retrieve_data__not_found(self):                                                    # Test retrieving non-existent data
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("nonexistent"),
                data_file_id = Safe_Str__Id("missing-001"),
                namespace    = self.test_namespace
            )

            result = _.retrieve_data(request)

            assert type(result)  is Schema__Cache__Data__Retrieve__Response
            assert result.found  is False
            assert result.data   is None

    def test_retrieve_data__invalid_cache_id(self):                                             # Test with non-existent cache_id
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = Random_Guid(),                                                   # Non-existent
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("logs"),
                data_file_id = Safe_Str__Id("log-001"),
                namespace    = self.test_namespace
            )

            result = _.retrieve_data(request)

            assert type(result)  is Schema__Cache__Data__Retrieve__Response
            assert result.found  is False

    def test__read_direct__string(self):                                                        # Test direct read method for string
        with self.service__retrieve_data as _:
            handler = self.service__cache.get_or_create_handler(self.test_namespace)
            storage_backend = handler.storage_backend

            # Simulate a string file path
            test_path = Safe_Str__File__Path(f"{self.test_namespace}/test.txt")
            storage_backend.file__save(test_path, b"direct string")

            result = _.read_direct(storage_backend, test_path, Enum__Cache__Data_Type.STRING)

            assert result == "direct string"

    def test__read_direct__json(self):                                                          # Test direct read method for JSON
        with self.service__retrieve_data as _:
            handler = self.service__cache.get_or_create_handler(self.test_namespace)
            storage_backend = handler.storage_backend

            # Simulate a JSON file path
            test_path = Safe_Str__File__Path(f"{self.test_namespace}/test.json")
            storage_backend.file__save(test_path, b'{"key": "value"}')

            result = _.read_direct(storage_backend, test_path, Enum__Cache__Data_Type.JSON)

            assert result == {"key": "value"}

    def test__read_direct__binary(self):                                                        # Test direct read method for binary
        with self.service__retrieve_data as _:
            handler = self.service__cache.get_or_create_handler(self.test_namespace)
            storage_backend = handler.storage_backend

            # Simulate a binary file path
            test_path = Safe_Str__File__Path(f"{self.test_namespace}/test.bin")
            test_bytes = b"\x00\x01\x02\x03"
            storage_backend.file__save(test_path, test_bytes)

            result = _.read_direct(storage_backend, test_path, Enum__Cache__Data_Type.BINARY)

            assert result == test_bytes

    def test_get_extension_for_type(self):                                                      # Test file extension mapping
        with self.service__retrieve_data as _:
            assert _.get_extension_for_type(Enum__Cache__Data_Type.STRING) == 'txt'
            assert _.get_extension_for_type(Enum__Cache__Data_Type.JSON)   == 'json'
            assert _.get_extension_for_type(Enum__Cache__Data_Type.BINARY) == 'bin'

            # Test default case
            result = _.get_extension_for_type(None)
            assert result == "data"

    def test_delete_data__string(self):                                                             # Test deleting string data
        with self.service__retrieve_data as _:
            store_request = Schema__Cache__Data__Store__Request(cache_id     = self.parent_cache_id              ,          # First store a file to delete
                                                                data         = "to be deleted"                   ,
                                                                data_type    = Enum__Cache__Data_Type.STRING     ,
                                                                data_key     = Safe_Str__File__Path("temp"     ) ,
                                                                data_file_id = Safe_Str__Id        ("delete-me") ,
                                                                namespace    = self.test_namespace               )
            self.service__store_data.store_data(store_request)


            delete_request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id         ,           # Now delete it
                                                                    data_type    = Enum__Cache__Data_Type.STRING,
                                                                    data_key     = Safe_Str__File__Path("temp") ,
                                                                    data_file_id = Safe_Str__Id("delete-me")    ,
                                                                    namespace    = self.test_namespace          )

            deleted = _.delete_data(delete_request)
            assert deleted is True

            result = _.retrieve_data(delete_request)                                                                        # Verify it's gone
            assert result.found is False

    def test_delete_data__not_found(self):                                                      # Test deleting non-existent data
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id,
                                                             data_type    = Enum__Cache__Data_Type.STRING,
                                                             data_key     = Safe_Str__File__Path("nonexistent"),
                                                             data_file_id = Safe_Str__Id("not-there"),
                                                             namespace    = self.test_namespace)

            deleted = _.delete_data(request)
            assert deleted is False

    def test_retrieve_data__without_data_key(self):                                             # Test retrieval without data_key
        with self.service__retrieve_data as _:
            # Store without data_key
            store_request = Schema__Cache__Data__Store__Request(cache_id     = self.parent_cache_id,
                                                                data         = "no key data",
                                                                data_type    = Enum__Cache__Data_Type.STRING,
                                                                data_key     = None,                                                            # No data_key
                                                                data_file_id = Safe_Str__Id("nokey-001"),
                                                                namespace    = self.test_namespace)
            self.service__store_data.store_data(store_request)

            # Retrieve without data_key
            retrieve_request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id,
                                                                      data_type    = Enum__Cache__Data_Type.STRING,
                                                                      data_key     = None,                                                            # No data_key
                                                                      data_file_id = Safe_Str__Id("nokey-001"),
                                                                      namespace    = self.test_namespace)

            result = self.service__retrieve_data.retrieve_data(retrieve_request)

            assert type(result) is Schema__Cache__Data__Retrieve__Response
            assert result.data  == "no key data"
            assert result.found is True

    def test_retrieve_data__nested_path(self):                                                  # Test with nested data_key path
        with self.service__retrieve_data as _:
            # Store with nested path
            store_request = Schema__Cache__Data__Store__Request(
                cache_id     = self.parent_cache_id,
                data         = {"nested": "data"},
                data_type    = Enum__Cache__Data_Type.JSON,
                data_key     = Safe_Str__File__Path("2024/12/reports"),
                data_file_id = Safe_Str__Id("report-001"),
                namespace    = self.test_namespace
            )
            self.service__store_data.store_data(store_request)

            # Retrieve with nested path
            retrieve_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.JSON,
                data_key     = Safe_Str__File__Path("2024/12/reports"),
                data_file_id = Safe_Str__Id("report-001"),
                namespace    = self.test_namespace
            )

            result = self.service__retrieve_data.retrieve_data(retrieve_request)

            assert result.data == {"nested": "data"}
            assert result.found is True

    def test_retrieve_data__comprehensive_response(self):                                       # Test all response fields populated correctly
        with self.service__retrieve_data as _:
            request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("logs"),
                data_file_id = Safe_Str__Id("log-001"),
                namespace    = self.test_namespace
            )

            result = _.retrieve_data(request)

            # Verify complete response using .obj()
            assert result.obj() == __(data         = "test string content"                      ,
                                      data_type    = 'string'                                   ,
                                      data_file_id = 'log-001'                                  ,
                                      data_key     = 'logs'                                     ,
                                      full_path    = __SKIP__                                   ,  # Path varies by storage
                                      size         = len("test string content")                 ,
                                      found        = True                                       )