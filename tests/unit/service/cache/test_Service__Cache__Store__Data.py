import pytest
from unittest                                                                         import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                               import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                           import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                  import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                 import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id       import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path     import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text          import Safe_Str__Text
from osbot_utils.utils.Misc                                                           import is_guid
from osbot_utils.utils.Objects                                                        import base_classes
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type               import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy         import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Store__Request   import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Store__Response  import Schema__Cache__Data__Store__Response
from mgraph_ai_service_cache.service.cache.Cache__Service                             import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve          import Cache__Service__Retrieve
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                import Cache__Service__Store
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store__Data          import Cache__Service__Store__Data
from tests.unit.Service__Cache__Test_Objs                                             import setup__service__cache__test_objs

class test_Cache__Service__Store__Data(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_objs           = setup__service__cache__test_objs()                            # Reuse shared test objects
        cls.cache_fixtures      = cls.test_objs.cache_fixtures
        cls.service__cache      = cls.cache_fixtures.cache_service
        cls.service__store      = Cache__Service__Store      (cache_service = cls.service__cache)
        cls.service__retrieve   = Cache__Service__Retrieve   (cache_service = cls.service__cache)
        cls.service__store_data = Cache__Service__Store__Data(cache_service = cls.service__cache)

        cls.test_namespace   = Safe_Str__Id("test-data-service")                                # Test data setup
        cls.test_cache_key   = Safe_Str__File__Path("logs/application")
        cls.test_string      = "test data string"
        cls.test_json        = {"data": "value", "count": 42}
        cls.test_binary      = b"data binary content \x00\x01\x02"

        # Create parent cache entry for testing
        cls.parent_response  = cls.service__store.store_string(data      = "parent data"                          ,
                                                               namespace = cls.test_namespace                      ,
                                                               strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                               cache_key = cls.test_cache_key                     ,
                                                               file_id   = Safe_Str__Id("parent-001")             )
        cls.parent_cache_id  = cls.parent_response.cache_id

    def setUp(self):
        self.request = Schema__Cache__Data__Store__Request(cache_id     = self.parent_cache_id,
                                                           data         = self.test_string,
                                                           data_type    = Enum__Cache__Data_Type.STRING,
                                                           data_key     = None,
                                                           data_file_id = None,
                                                           namespace    = self.test_namespace)

    def test__init__(self):                                                                      # Test auto-initialization
        with Cache__Service__Store__Data() as _:
            assert type(_)               is Cache__Service__Store__Data
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service                                      # Fixed attribute name

            assert _.obj() == __(cache_service = __(cache_config     = __(storage_mode     = 'memory'            ,
                                                                          default_bucket    = None                ,
                                                                          default_ttl_hours = 24                  ,
                                                                          local_disk_path   = None                ,
                                                                          sqlite_path       = None                ,
                                                                          zip_path          = None                ),
                                                    cache_handlers   = __()                                               ,
                                                    hash_config      = __(algorithm = 'sha256', length = 16)             ,
                                                    hash_generator   = __(config = __(algorithm = 'sha256', length = 16))))

    def test_store_data__string(self):                                                          # Test string data storage
        with self.service__store_data as _:
            with self.request as request:
                request.data_key     = Safe_Str__File__Path('an/data/folder')
                request.data_file_id = Safe_Str__Id('an-file')

                result = _.store_data(request)

                expected_data_file = f'{self.test_namespace}/data/semantic-file/logs/application/parent-001/data/{request.data_key}/{request.data_file_id}.txt'

                assert type(result) is Schema__Cache__Data__Store__Response
                assert result.obj() == __(cache_id           = self.parent_cache_id                  ,
                                          data_files_created = [expected_data_file]                  ,
                                          data_key           = 'an/data/folder'                      ,
                                          data_type          = 'string'                              ,
                                          extension          = 'txt'                                 ,
                                          file_id            = 'an-file'                             ,
                                          file_size          = len(self.test_string)                 ,
                                          namespace          = self.test_namespace                   ,
                                          timestamp          = __SKIP__                              )

    def test_store_data__json(self):                                                            # Test JSON data storage
        with self.service__store_data as _:
            with self.request as request:
                request.data         = self.test_json
                request.data_type    = Enum__Cache__Data_Type.JSON
                request.data_key     = Safe_Str__File__Path('configs')
                request.data_file_id = Safe_Str__Id('config-001')

                result = _.store_data(request)

                expected_data_file = f'{self.test_namespace}/data/semantic-file/logs/application/parent-001/data/{request.data_key}/{request.data_file_id}.json'

                assert type(result) is Schema__Cache__Data__Store__Response
                assert result.cache_id       == self.parent_cache_id
                assert result.file_size      > 0
                assert expected_data_file in result.data_files_created

    def test_store_data__binary(self):                                                          # Test binary data storage
        with self.service__store_data as _:
            with self.request as request:
                request.data         = self.test_binary
                request.data_type    = Enum__Cache__Data_Type.BINARY
                request.data_key     = Safe_Str__File__Path('attachments')
                request.data_file_id = Safe_Str__Id('binary-001')

                result = _.store_data(request)

                expected_data_file = f'{self.test_namespace}/data/semantic-file/logs/application/parent-001/data/{request.data_key}/{request.data_file_id}.bin'

                assert type(result) is Schema__Cache__Data__Store__Response
                assert result.file_size == len(self.test_binary)
                assert expected_data_file in result.data_files_created

    def test_store_data__auto_generated_id(self):                                               # Test auto-generation when data_file_id not provided
        with self.service__store_data as _:
            with self.request as request:
                request.data_key = Safe_Str__File__Path('auto-gen')

                result = _.store_data(request)

                assert type(result) is Schema__Cache__Data__Store__Response
                assert len(result.data_files_created) > 0

                created_file = result.data_files_created[0]
                assert result.data_key == request.data_key                                      # Data key is preserved
                assert f'{request.data_key}/'  in created_file                                  # and used in the path
                assert '.txt'                   in created_file                                 # String extension
                assert result.file_id           in created_file                                 # File should have auto-generated GUID in path
                assert is_guid(result.file_id) is True

    def test_store_data__non_existent_cache_id(self):                                           # Test behavior with non-existent cache_id
        with self.service__store_data as _:
            with self.request as request:
                request.cache_id = Random_Guid()

                result = _.store_data(request)

                assert result is None                                                           # Returns None when cache_id doesn't exist

    def test_store_data__multiple_path_handlers(self):                                          # Test with multiple data folders
        with self.service__store_data as _:
            with self.request as request:
                request.data         = "multi handler test"
                request.data_key     = Safe_Str__File__Path('multi-handler')
                request.data_file_id = Safe_Str__Id('multi-001')

                result = _.store_data(request)

                assert type(result) is Schema__Cache__Data__Store__Response
                assert len(result.data_files_created) >= 1                                      # If multiple handlers exist, would have multiple files created

    def test_get_extension_for_type(self):                                                      # Test file extension mapping
        with self.service__store_data as _:
            assert _.get_extension_for_type(Enum__Cache__Data_Type.STRING) == 'txt'
            assert _.get_extension_for_type(Enum__Cache__Data_Type.JSON)   == 'json'
            assert _.get_extension_for_type(Enum__Cache__Data_Type.BINARY) == 'bin'

    def test_serialize_data(self):                                                              # Test data serialization
        with self.service__store_data as _:
            # String serialization
            string_bytes = _.serialize_data("test string", Safe_Str__Text('string'))
            assert string_bytes == b"test string"

            # JSON serialization
            json_data  = {"key": "value"}
            json_bytes = _.serialize_data(json_data, Safe_Str__Text('json'))
            assert type(json_bytes) is bytes
            assert b'"key"' in json_bytes

            # Binary serialization
            binary_data  = b"raw bytes"
            binary_bytes = _.serialize_data(binary_data, Safe_Str__Text('binary'))
            assert binary_bytes == binary_data

            # Invalid binary data
            with pytest.raises(ValueError) as exc_info:
                _.serialize_data("not bytes", Safe_Str__Text('binary'))
            assert "Binary data must be bytes" in str(exc_info.value)

    def test_store_data__nested_data_key(self):                                                 # Test nested path structure with data_key
        with self.service__store_data as _:
            with self.request as request:
                request.data         = "nested path test"
                request.data_key     = Safe_Str__File__Path('2024/12/logs')                    # Nested path
                request.data_file_id = Safe_Str__Id('log-001')

                result = _.store_data(request)

                assert type(result) is Schema__Cache__Data__Store__Response
                created_file = result.data_files_created[0]
                assert '2024/12/logs/log-001.txt' in created_file

    def test_store_data__without_data_key(self):                                                # Test without data_key (direct in data folder)
        with self.service__store_data as _:
            with self.request as request:
                request.data         = "no data key"
                request.data_key     = None                                                     # No data_key
                request.data_file_id = Safe_Str__Id('direct-file')

                result = _.store_data(request)

                assert type(result) is Schema__Cache__Data__Store__Response
                created_file = result.data_files_created[0]
                assert f'/data/{request.data_file_id}.txt' in created_file

    def test_store_data__multiple_to_same_parent(self):                                         # Test multiple data files for same parent
        skip__if_not__in_github_actions()
        with self.service__store_data as _:
            results = []

            for i in range(3):
                with self.request as request:
                    request.data         = f"data file {i}"
                    request.data_key     = Safe_Str__File__Path('batch')
                    request.data_file_id = Safe_Str__Id(f'multi-data-{i:03d}')

                    result = _.store_data(request)
                    results.append(result)

            assert len(results) == 3
            for result in results:
                assert type(result) is Schema__Cache__Data__Store__Response
                assert result.cache_id == self.parent_cache_id

    def test_retrieve_service(self):                                                            # Test retrieve service caching
        with self.service__store_data as _:
            retrieve1 = _.retrieve_service()
            retrieve2 = _.retrieve_service()

            assert retrieve1 is retrieve2                                                       # Same instance (cached)
            assert type(retrieve1) is Cache__Service__Retrieve

    def test_store_data__path_traversal_safety(self):                                           # Test that url_join_safe prevents path traversal
        with self.service__store_data as _:
            with self.request as request:
                request.data         = "path traversal attempt"
                request.data_key     = Safe_Str__File__Path('../../../etc')                    # Dangerous path
                request.data_file_id = Safe_Str__Id('passwd')

                result = _.store_data(request)

                # url_join_safe should sanitize the path
                assert type(result) is Schema__Cache__Data__Store__Response
                created_file = result.data_files_created[0]
                # Path should be sanitized (no ../ sequences that could escape)
                assert created_file == 'test-data-service/data/semantic-file/logs/application/parent-001/data/-/-/-/etc/passwd.txt'