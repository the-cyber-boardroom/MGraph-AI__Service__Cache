import pytest
from unittest                                                                               import TestCase
from fastapi                                                                                import HTTPException
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Cache__File__File_Id   import Safe_Str__Cache__File__File_Id
from osbot_utils.utils.Misc                                                                 import is_guid
from memory_fs.path_handlers.Path__Handler__Temporal                                        import Path__Handler__Temporal
from osbot_fast_api.api.routes.Fast_API__Routes                                             import Fast_API__Routes
from osbot_utils.testing.__                                                                 import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.utils.Objects                                                              import base_classes
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store                       import Routes__Data__Store, TAG__ROUTES_STORE__DATA, PREFIX__ROUTES_STORE__DATA, ROUTES_PATHS__STORE__DATA
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Retrieve                    import Routes__File__Retrieve
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                       import Routes__File__Store
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Store__Response import Schema__Cache__Data__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type              import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy        import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store                 import Cache__Service__Data__Store
from tests.unit.Service__Cache__Test_Objs                                                   import setup__service__cache__test_objs


class test_Routes__Data__Store(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_objs          = setup__service__cache__test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.cache_service      = cls.cache_fixtures.cache_service

        cls.routes_store       = Routes__File__Store   (cache_service = cls.cache_service)      # Parent entry routes
        cls.routes_retrieve    = Routes__File__Retrieve(cache_service = cls.cache_service)      # For verification
        cls.routes_data_store  = Routes__Data__Store   (cache_service = cls.cache_service)      # Route under test

        cls.test_namespace     = "test-store-data"                                              # Test namespace
        cls.test_data_key      = "configs/app"                                                  # Hierarchical path

        cls.parent_cache_id    = cls._create_parent_cache_entry(cls)                            # Create parent entry once
        cls.path_now           = Path__Handler__Temporal().path_now()

    @classmethod
    def _create_parent_cache_entry(cls, self):                                                  # Helper to create parent cache entry
        response = cls.routes_store.store__string(data      = "parent content"                          ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL    ,
                                                  namespace = cls.test_namespace                       )
        return response.cache_id

    def test__init__(self):                                                                      # Test initialization and structure
        with Routes__Data__Store() as _:
            assert type(_)                    is Routes__Data__Store
            assert base_classes(_)            == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                      == TAG__ROUTES_STORE__DATA
            assert _.prefix                   == PREFIX__ROUTES_STORE__DATA
            assert type(_.cache_service)      is Cache__Service
            assert type(_.store_service())    is Cache__Service__Data__Store

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_STORE__DATA        == 'data/store'
        assert PREFIX__ROUTES_STORE__DATA     == '/{namespace}/cache/{cache_id}'
        assert len(ROUTES_PATHS__STORE__DATA) == 9
        assert ROUTES_PATHS__STORE__DATA[0]   == '/{namespace}/cache/{cache_id}/data/store/string'

    def test_data__store_string(self):                                                          # Test base string endpoint (auto-generates ID)
        with self.routes_data_store as _:
            test_string = "auto generated id test"

            response = _.data__store_string(data         = test_string         ,
                                           cache_id     = self.parent_cache_id,
                                           namespace    = self.test_namespace  )
            file_id = response.file_id
            assert is_guid(file_id)                 is True
            assert type(file_id)                    is Safe_Str__Cache__File__File_Id
            assert type(response)                   is Schema__Cache__Data__Store__Response
            assert response.cache_id                == self.parent_cache_id
            assert response.data_type               == Enum__Cache__Data_Type.STRING
            assert response.data_key                == ''                                       # No key provided
            assert response.file_id                 != ''                                       # Auto-generated
            assert response.namespace               == self.test_namespace
            assert len(response.data_files_created) == 1
            assert response.file_size               == len(test_string.encode('utf-8'))
            assert response.obj()                   == __( cache_id           = self.parent_cache_id,
                                                           data_files_created = [f'{self.test_namespace}/data/temporal/{self.path_now}/{self.parent_cache_id}/data/{file_id}.txt'],
                                                           data_key           = ''               ,
                                                           data_type          = 'string'         ,
                                                           extension          = 'txt'            ,
                                                           file_id            = file_id          ,
                                                           file_size          = 22               ,
                                                           namespace          = 'test-store-data',
                                                           timestamp          = __SKIP__         )

    def test_data__store_string__with__id(self):                                                # Test with specific file ID
        with self.routes_data_store as _:
            test_string = "specific id test"
            file_id     = "config-v1"

            response = _.data__store_string__with__id(data         = test_string         ,
                                                      cache_id     = self.parent_cache_id,
                                                      namespace    = self.test_namespace ,
                                                      data_file_id = file_id             )

            assert response.cache_id  == self.parent_cache_id
            assert response.data_type == Enum__Cache__Data_Type.STRING
            assert response.data_key  == ''                                                     # Still no key
            assert response.file_id   == file_id                                                # Specific ID used

    def test_data__store_string__with__id_and_key(self):                                        # Test full control path
        with self.routes_data_store as _:
            test_string = "configuration data"
            file_id     = "config-v1"

            response = _.data__store_string__with__id_and_key(data         = test_string         ,
                                                              cache_id     = self.parent_cache_id,
                                                              namespace    = self.test_namespace ,
                                                              data_key     = self.test_data_key  ,
                                                              data_file_id = file_id             )

            assert response.obj() == __(cache_id           = self.parent_cache_id              ,
                                       data_files_created = __SKIP__                          ,  # Dynamic paths
                                       data_key           = self.test_data_key                ,
                                       data_type          = Enum__Cache__Data_Type.STRING     ,
                                       extension          = 'txt'                             ,
                                       file_id            = file_id                           ,
                                       file_size          = 18                                ,
                                       namespace          = self.test_namespace               ,
                                       timestamp          = __SKIP__                          )  # Auto-generated

    def test_data__store_json(self):                                                            # Test base JSON endpoint
        with self.routes_data_store as _:
            test_json = {"setting": "value", "count": 42}

            response = _.data__store_json(data         = test_json            ,
                                         cache_id     = self.parent_cache_id ,
                                         namespace    = self.test_namespace  )

            assert response.data_type == Enum__Cache__Data_Type.JSON
            assert response.extension == 'json'
            assert response.file_id   != ''                                                     # Auto-generated

    def test_data__store_json__with__id(self):                                                  # Test JSON with specific ID
        with self.routes_data_store as _:
            test_json = {"setting": "value", "count": 42}
            file_id   = "settings"

            response = _.data__store_json__with__id(data         = test_json            ,
                                                    cache_id     = self.parent_cache_id,
                                                    namespace    = self.test_namespace ,
                                                    data_file_id = file_id             )

            assert response.data_type == Enum__Cache__Data_Type.JSON
            assert response.file_id   == file_id

    def test_data__store_json__with__id_and_key(self):                                          # Test JSON with full path
        with self.routes_data_store as _:
            test_json = {"setting": "value", "count": 42}
            file_id   = "settings"

            response = _.data__store_json__with__id_and_key(data         = test_json            ,
                                                            cache_id     = self.parent_cache_id,
                                                            namespace    = self.test_namespace ,
                                                            data_key     = self.test_data_key  ,
                                                            data_file_id = file_id             )

            assert response.data_key == self.test_data_key
            assert response.file_id  == file_id

    def test_data__store_binary(self):                                                          # Test base binary endpoint
        with self.routes_data_store as _:
            test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50                                  # Fake PNG header

            response = _.data__store_binary(body         = test_binary         ,
                                           cache_id     = self.parent_cache_id,
                                           namespace    = self.test_namespace  )

            assert response.data_type == Enum__Cache__Data_Type.BINARY
            assert response.extension == 'bin'
            assert response.file_size == len(test_binary)
            assert response.file_id  != ''                                                      # Auto-generated

    def test_data__store_binary__with__id(self):                                                # Test binary with specific ID
        with self.routes_data_store as _:
            test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50
            file_id     = "image-thumbnail"

            response = _.data__store_binary__with__id(body         = test_binary         ,
                                                      cache_id     = self.parent_cache_id,
                                                      namespace    = self.test_namespace ,
                                                      data_file_id = file_id             )

            assert response.file_id == file_id

    def test_data__store_binary__with__id_and_key(self):                                        # Test binary with full path
        with self.routes_data_store as _:
            test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 50
            file_id     = "image-thumbnail"

            response = _.data__store_binary__with__id_and_key(body         = test_binary         ,
                                                              cache_id     = self.parent_cache_id,
                                                              namespace    = self.test_namespace ,
                                                              data_key     = self.test_data_key  ,
                                                              data_file_id = file_id             )

            assert response.data_key == self.test_data_key
            assert response.file_id  == file_id

    def test__delegation_pattern(self):                                                         # Test methods properly delegate
        with self.routes_data_store as _:                                                       # Test that base methods delegate to full methods with empty defaults
            test_data = "delegation test"

            response1 = _.data__store_string(data         = test_data          ,                # Base delegates to with__id with empty string
                                             cache_id     = self.parent_cache_id,
                                             namespace    = self.test_namespace )
            assert response1.data_key == ''                                                     # Verify delegation worked (ID was auto-generated)
            assert response1.file_id  != ''

    def test_store__data__with_hierarchy(self):                                                 # Test hierarchical data organization
        with self.routes_data_store as _:
            data_key_1 = "users/profiles"                                                       # Store multiple files in hierarchy
            data_key_2 = "users/settings"

            response_1 = _.data__store_string__with__id_and_key(data         = "profile data"       ,
                                                                cache_id     = self.parent_cache_id ,
                                                                namespace    = self.test_namespace  ,
                                                                data_key     = data_key_1           ,
                                                                data_file_id = "user1"              )

            response_2 = _.data__store_json__with__id_and_key  (data         = {"theme": "dark"}    ,
                                                                cache_id     = self.parent_cache_id ,
                                                                namespace    = self.test_namespace  ,
                                                                data_key     = data_key_2           ,
                                                                data_file_id = "user1"              )

            assert response_1.data_key == data_key_1
            assert response_2.data_key == data_key_2
            assert response_1.cache_id == response_2.cache_id                                   # Same parent

    def test_store__data__parent_not_found(self):                                               # Test error when parent doesn't exist
        with self.routes_data_store as _:
            non_existent_id = Random_Guid()

            with pytest.raises(HTTPException) as exc_info:
                _.data__store_string(data         = "test"              ,
                                    cache_id     = non_existent_id     ,
                                    namespace    = self.test_namespace)

            error = exc_info.value
            assert error.status_code          == 404
            assert error.detail["error_type"] == "NOT_FOUND"
            assert str(non_existent_id)      in error.detail["message"]

    def test_store__multiple_data_files(self):                                                  # Test storing multiple data files
        with self.routes_data_store as _:
            files_created = []

            for i in range(3):                                                                  # Store 3 files
                file_id  = f"file-{i}"
                response = _.data__store_string__with__id_and_key(data         = f"content {i}"        ,
                                                                  cache_id     = self.parent_cache_id ,
                                                                  namespace    = self.test_namespace  ,
                                                                  data_key     = self.test_data_key   ,
                                                                  data_file_id = file_id              )
                files_created.append(response.file_id)

            assert len(files_created)      == 3
            assert len(set(files_created)) == 3                                                 # All unique

    def test_store__data__special_characters(self):                                             # Test handling of special characters
        with self.routes_data_store as _:
            special_string = "Data with 中文 and émojis 🚀"

            response = _.data__store_string__with__id_and_key(data         = special_string       ,
                                                              cache_id     = self.parent_cache_id,
                                                              namespace    = self.test_namespace ,
                                                              data_key     = self.test_data_key  )

            assert response.file_size > len(special_string)                                     # UTF-8 encoding increases size

    def test_store__data__empty_json(self):                                                     # Test empty JSON storage
        with self.routes_data_store as _:
            response = _.data__store_json(data         = {}                   ,
                                         cache_id     = self.parent_cache_id ,
                                         namespace    = self.test_namespace  )

            assert response.data_type == Enum__Cache__Data_Type.JSON
            assert response.file_size == 2                                                      # "{}" is 2 bytes

    def test_store__data__large_json(self):                                                     # Test large JSON storage
        with self.routes_data_store as _:
            large_json = {f"key_{i}": {"value": i, "data": list(range(10))} for i in range(100)}

            response = _.data__store_json(data         = large_json           ,
                                         cache_id     = self.parent_cache_id ,
                                         namespace    = self.test_namespace  )

            assert response.file_size > 1000                                                    # Large file

    def test__integration_with_retrieve(self):                                                  # Integration test with retrieve
        with self.routes_data_store as _:
            # Store data
            test_data = "integration test data"
            file_id   = "integration-test"

            store_response = _.data__store_string__with__id_and_key(data         = test_data          ,
                                                                    cache_id     = self.parent_cache_id,
                                                                    namespace    = self.test_namespace ,
                                                                    data_key     = self.test_data_key  ,
                                                                    data_file_id = file_id             )
            cache_id      = store_response.cache_id
            expected_file = f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data/{self.test_data_key}/{file_id}.txt'

            assert store_response.obj() == __(cache_id           = cache_id          ,
                                              data_files_created = [expected_file]   ,
                                              data_key           = 'configs/app'     ,
                                              data_type          = 'string'          ,
                                              extension          = 'txt'             ,
                                              file_id            = 'integration-test',
                                              file_size          = 21                ,
                                              namespace          = 'test-store-data' ,
                                              timestamp          = __SKIP__          )

            # Verify parent entry still exists
            parent_entry = self.routes_retrieve.retrieve__cache_id(cache_id  = self.parent_cache_id,
                                                                   namespace = self.test_namespace )

            # Data files are stored in data folders referenced in parent
            file_refs = self.routes_retrieve.retrieve__cache_id__refs(cache_id  = self.parent_cache_id,
                                                                      namespace = self.test_namespace )

            assert parent_entry.metadata.cache_id       == self.parent_cache_id
            assert len(file_refs.file_paths.data_folders) > 0                                   # Has data folders

            assert file_refs.obj() == __(all_paths   = __(data    = [ f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.json',
                                                                       f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.json.config',
                                                                       f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.json.metadata'],
                                                          by_hash = [ f'{self.test_namespace}/refs/by-hash/2d/45/2d456ae65174bccb.json'],
                                                          by_id   = [ f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json']),
                                         cache_id    = cache_id         ,
                                         cache_hash  = '2d456ae65174bccb',
                                         file_type   = 'json'            ,
                                         namespace   = 'test-store-data' ,
                                         file_paths  = __(content_files = [f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.json'],
                                                         data_folders  = [f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data']),
                                         strategy    = 'temporal'        ,
                                         timestamp   = __SKIP__         )