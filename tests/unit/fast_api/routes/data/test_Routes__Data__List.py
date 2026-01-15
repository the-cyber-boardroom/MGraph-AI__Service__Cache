import pytest
from unittest                                                                                   import TestCase
from fastapi                                                                                    import HTTPException
from memory_fs.path_handlers.Path__Handler__Temporal import Path__Handler__Temporal
from osbot_fast_api.api.routes.Fast_API__Routes                                                 import Fast_API__Routes
from osbot_utils.testing.__                                                                     import __
from osbot_utils.type_safe.Type_Safe                                                            import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path               import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                           import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                 import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__List import Type_Safe__List
from osbot_utils.utils.Objects                                                                  import base_classes
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__List                            import Routes__Data__List, TAG__ROUTES_LIST__DATA, PREFIX__ROUTES_LIST__DATA, ROUTES_PATHS__LIST__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store                           import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                           import Routes__File__Store
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__List__Response      import Schema__Cache__Data__List__Response
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__File__Info          import Schema__Cache__Data__File__Info
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                  import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__List                      import Cache__Service__Data__List
from tests.unit.Service__Cache__Test_Objs                                                       import setup__service__cache__test_objs


class test_Routes__Data__List(TestCase):

    @classmethod
    def setUpClass(cls):                                                                                                # ONE-TIME expensive setup
        cls.test_objs                = setup__service__cache__test_objs()
        cls.cache_fixtures           = cls.test_objs.cache_fixtures
        cls.cache_service            = cls.cache_fixtures.cache_service
    
        cls.routes_store             = Routes__File__Store(cache_service = cls.cache_service)                           # Parent entry routes
        cls.routes_data_store        = Routes__Data__Store(cache_service = cls.cache_service)                           # For test data creation
        cls.routes_data_list         = Routes__Data__List (cache_service = cls.cache_service)                           # Route under test
                        
        cls.test_namespace           = Safe_Str__Id("test-list-data")                                                   # Test namespace
        cls.test_data_key            = Safe_Str__File__Path("configs/app")                                              # Hierarchical path
        cls.test_data_key_2          = Safe_Str__File__Path("configs/other")                                            # Second hierarchical path
                    
        cls.parent_cache_id          = cls._create_parent_cache_entry(cls)                                              # Create parent entry once
        cls.parent_cache_id__sharded = f'{cls.parent_cache_id[0:2]}/{cls.parent_cache_id[2:4]}/{cls.parent_cache_id}'
        cls.path_now                 = Path__Handler__Temporal().path_now()                           # Current temporal path
        cls.data_path_temporal       = f'test-list-data/data/temporal/{cls.path_now}/{cls.parent_cache_id}/data'
        cls._setup_test_data()                                                                                          # Store test data for listing

    @classmethod
    def _create_parent_cache_entry(cls, self):                                                  # Helper to create parent cache entry
        response = cls.routes_store.store__string(data      = "parent for listing"                    ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL   ,
                                                  namespace = cls.test_namespace                      )
        return response.cache_id

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup test data files for listing
        # Store files at root level (no data_key)
        cls.routes_data_store.data__store_string__with__id(
            data         = "root string 1",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("root-string-001")
        )

        cls.routes_data_store.data__store_json__with__id(
            data         = {"root": "json"},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("root-json-001")
        )

        cls.routes_data_store.data__store_binary__with__id(
            body         = b'root binary data',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("root-binary-001")
        )

        # Store files under test_data_key
        cls.routes_data_store.data__store_string__with__id_and_key(
            data         = "keyed string 1",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("keyed-string-001")
        )

        cls.routes_data_store.data__store_json__with__id_and_key(
            data         = {"keyed": "json"},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("keyed-json-001")
        )

        cls.routes_data_store.data__store_binary__with__id_and_key(
            body         = b'keyed binary data',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("keyed-binary-001")
        )

        # Store files under test_data_key_2 (different path)
        cls.routes_data_store.data__store_json__with__id_and_key(
            data         = {"other": "json"},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key_2,
            data_file_id = Safe_Str__Id("other-json-001")
        )

    def test__init__(self):                                                                     # Test initialization and structure
        with Routes__Data__List() as _:
            assert type(_)                   is Routes__Data__List
            assert base_classes(_)           == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                     == TAG__ROUTES_LIST__DATA
            assert _.prefix                  == PREFIX__ROUTES_LIST__DATA
            assert type(_.cache_service)     is Cache__Service
            assert type(_.list_service())    is Cache__Service__Data__List

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_LIST__DATA       == 'data'
        assert PREFIX__ROUTES_LIST__DATA    == '/{namespace}/cache/{cache_id}'
        assert len(ROUTES_PATHS__LIST__DATA) == 2
        assert ROUTES_PATHS__LIST__DATA[0]  == '/{namespace}/cache/{cache_id}/data/list'
        assert ROUTES_PATHS__LIST__DATA[1]  == '/{namespace}/cache/{cache_id}/data/list/{data_key:path}'

    def test_data__list__all_files(self):                                                       # Test listing all data files
        with self.routes_data_list as _:

            result = _.data__list(cache_id  = self.parent_cache_id  ,
                                  namespace = self.test_namespace   ,
                                  recursive = True                  )

            assert type(result)      is Schema__Cache__Data__List__Response
            assert result.cache_id   == self.parent_cache_id
            assert result.namespace  == self.test_namespace
            assert result.file_count >= 7                                                        # At least 7 files we stored
            assert result.total_size > 0
            assert len(result.files) == result.file_count

            # Verify file info structure
            for file_info in result.files:
                assert type(file_info)       is Schema__Cache__Data__File__Info
                assert file_info.data_file_id is not None
                assert file_info.data_type    is not None
                assert file_info.file_path    is not None
                assert file_info.extension    is not None
            assert result.obj()               == __(cache_id=self.parent_cache_id ,
                                                    namespace='test-list-data',
                                                    data_key='',
                                                    file_count=7,
                                                    files=[__(data_file_id='root-string-001',
                                                              data_key='',
                                                              data_type='string',
                                                              file_path=f'{self.data_path_temporal}/root-string-001.txt',
                                                              file_size=13,
                                                              extension='txt'),
                                                           __(data_file_id='root-json-001',
                                                              data_key='',
                                                              data_type='json',
                                                              file_path=f'{self.data_path_temporal}/root-json-001.json',
                                                              file_size=22,
                                                              extension='json'),
                                                           __(data_file_id='root-binary-001',
                                                              data_key='',
                                                              data_type='binary',
                                                              file_path=f'{self.data_path_temporal}/root-binary-001.bin',
                                                              file_size=16,
                                                              extension='bin'),
                                                           __(data_file_id='keyed-string-001',
                                                              data_key='configs/app',
                                                              data_type='string',
                                                              file_path=f'{self.data_path_temporal}/configs/app/keyed-string-001.txt',
                                                              file_size=14,
                                                              extension='txt'),
                                                           __(data_file_id='keyed-json-001',
                                                              data_key='configs/app',
                                                              data_type='json',
                                                              file_path=f'{self.data_path_temporal}/configs/app/keyed-json-001.json',
                                                              file_size=23,
                                                              extension='json'),
                                                           __(data_file_id='keyed-binary-001',
                                                              data_key='configs/app',
                                                              data_type='binary',
                                                              file_path=f'{self.data_path_temporal}/configs/app/keyed-binary-001.bin',
                                                              file_size=17,
                                                              extension='bin'),
                                                           __(data_file_id='other-json-001',
                                                              data_key='configs/other',
                                                              data_type='json',
                                                              file_path=f'{self.data_path_temporal}/configs/other/other-json-001.json',
                                                              file_size=23,
                                                              extension='json')],
                                                    total_size=128)

    def test_data__list__with__key(self):                                                       # Test listing files under specific key
        with self.routes_data_list as _:
            result = _.data__list__with__key(cache_id  = self.parent_cache_id  ,
                                             namespace = self.test_namespace   ,
                                             data_key  = self.test_data_key    ,
                                             recursive = True                  )

            assert type(result)      is Schema__Cache__Data__List__Response
            assert result.cache_id   == self.parent_cache_id
            assert result.namespace  == self.test_namespace
            assert result.data_key   == self.test_data_key
            assert result.file_count == 3                                                        # 3 files under test_data_key

            # Verify all files are from the correct key
            file_ids = [f.data_file_id for f in result.files]
            assert "keyed-string-001" in file_ids
            assert "keyed-json-001"   in file_ids
            assert "keyed-binary-001" in file_ids
            assert result.obj()       == __(cache_id=self.parent_cache_id ,
                                           namespace='test-list-data',
                                           data_key='configs/app',
                                           file_count=3,
                                           files=[__(data_file_id='keyed-string-001',
                                                     data_key='configs/app',
                                                     data_type='string',
                                                     file_path=f'{self.data_path_temporal}/configs/app/keyed-string-001.txt',
                                                     file_size=14,
                                                     extension='txt'),
                                                  __(data_file_id='keyed-json-001',
                                                     data_key='configs/app',
                                                     data_type='json',
                                                     file_path=f'{self.data_path_temporal}/configs/app/keyed-json-001.json',
                                                     file_size=23,
                                                     extension='json'),
                                                  __(data_file_id='keyed-binary-001',
                                                     data_key='configs/app',
                                                     data_type='binary',
                                                     file_path=f'{self.data_path_temporal}/configs/app/keyed-binary-001.bin',
                                                     file_size=17,
                                                     extension='bin')],
                                           total_size=54) 

    def test_data__list__different_key(self):                                                   # Test listing files under different key
        with self.routes_data_list as _:
            result = _.data__list__with__key(cache_id  = self.parent_cache_id  ,
                                             namespace = self.test_namespace   ,
                                             data_key  = self.test_data_key_2  ,
                                             recursive = True                  )

            assert result.file_count == 1                                                        # Only 1 file under test_data_key_2

            file_ids = [f.data_file_id for f in result.files]
            assert "other-json-001" in file_ids

    def test_data__list__empty_result(self):                                                    # Test listing when no files exist
        with self.routes_data_list as _:
            # Create parent with no data files
            parent = self.routes_store.store__string(data      = "empty parent"                         ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            result = _.data__list(cache_id  = parent.cache_id     ,
                                  namespace = self.test_namespace ,
                                  recursive = True                )

            assert type(result)      is Schema__Cache__Data__List__Response
            assert result.file_count == 0
            assert result.files      == []
            assert result.total_size == 0

    def test_data__list__non_existent_key(self):                                                # Test listing under non-existent key path
        with self.routes_data_list as _:
            result = _.data__list__with__key(cache_id  = self.parent_cache_id            ,
                                             namespace = self.test_namespace             ,
                                             data_key  = Safe_Str__File__Path("non/existent/path"),
                                             recursive = True                            )

            assert result.file_count == 0
            assert result.files      == []

    def test_data__list__non_existent_parent(self):                                             # Test listing with non-existent parent
        with self.routes_data_list as _:
            non_existent_id = Random_Guid()

            with pytest.raises(HTTPException) as exc_info:
                _.data__list(cache_id  = non_existent_id    ,
                             namespace = self.test_namespace,
                             recursive = True               )

            error = exc_info.value
            assert error.status_code          == 404
            assert error.detail["error_type"] == "NOT_FOUND"
            assert str(non_existent_id)      in error.detail["message"]

    def test_data__list__file_type_detection(self):                                             # Test that file types are correctly detected
        with self.routes_data_list as _:
            result = _.data__list(cache_id  = self.parent_cache_id  ,
                                  namespace = self.test_namespace   ,
                                  recursive = True                  )

            # Find files by type
            string_files = Type_Safe__List(expected_type=Schema__Cache__Data__File__Info, initial_data=[f for f in result.files if f.data_type == Enum__Cache__Data_Type.STRING])
            json_files   = Type_Safe__List(expected_type=Schema__Cache__Data__File__Info, initial_data=[f for f in result.files if f.data_type == Enum__Cache__Data_Type.JSON  ])
            binary_files = Type_Safe__List(expected_type=Schema__Cache__Data__File__Info, initial_data=[f for f in result.files if f.data_type == Enum__Cache__Data_Type.BINARY])

            assert len(string_files) >= 2                                                        # At least 2 string files
            assert len(json_files)   >= 3                                                        # At least 3 json files
            assert len(binary_files) >= 2                                                        # At least 2 binary files

            assert string_files.obj() == [__(data_file_id='root-string-001',
                                             data_key='',
                                             data_type='string',
                                             file_path=f'{self.data_path_temporal}/root-string-001.txt',
                                             file_size=13,
                                             extension='txt'),
                                          __(data_file_id='keyed-string-001',
                                             data_key='configs/app',
                                             data_type='string',
                                             file_path=f'{self.data_path_temporal}/configs/app/keyed-string-001.txt',
                                             file_size=14,
                                             extension='txt')]

            assert json_files.obj()   == [__(data_file_id='root-json-001',
                                             data_key='',
                                             data_type='json',
                                             file_path=f'{self.data_path_temporal}/root-json-001.json',
                                             file_size=22,
                                             extension='json'),
                                          __(data_file_id='keyed-json-001',
                                             data_key='configs/app',
                                             data_type='json',
                                             file_path=f'{self.data_path_temporal}/configs/app/keyed-json-001.json',
                                             file_size=23,
                                             extension='json'),
                                          __(data_file_id='other-json-001',
                                             data_key='configs/other',
                                             data_type='json',
                                             file_path=f'{self.data_path_temporal}/configs/other/other-json-001.json',
                                             file_size=23,
                                             extension='json')]

            assert binary_files.obj() == [__(data_file_id='root-binary-001',
                                             data_key='',
                                             data_type='binary',
                                             file_path=f'{self.data_path_temporal}/root-binary-001.bin',
                                             file_size=16,
                                             extension='bin'),
                                          __(data_file_id='keyed-binary-001',
                                             data_key='configs/app',
                                             data_type='binary',
                                             file_path=f'{self.data_path_temporal}/configs/app/keyed-binary-001.bin',
                                             file_size=17,
                                             extension='bin')]


            # Verify extensions match types
            for f in string_files:
                assert f.extension == 'txt'

            for f in json_files:
                assert f.extension == 'json'

            for f in binary_files:
                assert f.extension == 'bin'

    def test_data__list__delegates_to_with_key(self):                                           # Test data__list delegates to data__list__with__key
        with self.routes_data_list as _:
            result1 = _.data__list(cache_id  = self.parent_cache_id  ,                          # Both should return same result when no key specified
                                   namespace = self.test_namespace   ,
                                   recursive = True                  )

            result2 = _.data__list__with__key(cache_id  = self.parent_cache_id  ,
                                              namespace = self.test_namespace   ,
                                              data_key  = ''                    ,
                                              recursive = True                  )

            assert result1.file_count == result2.file_count

    def test_data__list__recursive_false(self):                                                 # Test non-recursive listing
        with self.routes_data_list as _:
            # Non-recursive should only get root-level files
            result = _.data__list(cache_id  = self.parent_cache_id  ,
                                  namespace = self.test_namespace   ,
                                  recursive = False                 )

            file_ids = [f.data_file_id for f in result.files]                                   # Should only contain root files, not those under data_key paths

            assert file_ids == [Safe_Str__Id('root-string-001'),
                                Safe_Str__Id('root-json-001'  ),
                                Safe_Str__Id('root-binary-001')]

    def test_data__list__total_size_calculation(self):                                          # Test that total_size is calculated correctly
        with self.routes_data_list as _:
            result = _.data__list(cache_id  = self.parent_cache_id  ,
                                  namespace = self.test_namespace   ,
                                  recursive = True                  )

            # Calculate expected total from individual files
            calculated_total = sum(f.file_size for f in result.files if f.file_size)

            assert result.total_size == calculated_total

    def test_data__list__namespace_isolation(self):                                             # Test namespace isolation
        with self.routes_data_list as _:
            different_namespace = Safe_Str__Id("different-namespace")

            # Files should not exist in different namespace
            with pytest.raises(HTTPException) as exc_info:
                _.data__list(cache_id  = self.parent_cache_id   ,
                             namespace = different_namespace    ,
                             recursive = True                   )

            # Should fail because parent doesn't exist in different namespace
            assert exc_info.value.status_code == 404

    def test__integration__store_list_verify(self):                                             # Full integration test
        with self.routes_data_list as _:
            # Create fresh parent
            parent = self.routes_store.store__string(data      = "integration list parent"              ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            # Store multiple files with hierarchy
            test_key = Safe_Str__File__Path("integration/test")

            self.routes_data_store.data__store_string__with__id_and_key(data         = "integ string"           ,
                                                                        cache_id     = parent.cache_id          ,
                                                                        namespace    = self.test_namespace      ,
                                                                        data_key     = test_key                 ,
                                                                        data_file_id = Safe_Str__Id("integ-str"))

            self.routes_data_store.data__store_json__with__id_and_key(data         = {"integ": "json"}          ,
                                                                      cache_id     = parent.cache_id            ,
                                                                      namespace    = self.test_namespace        ,
                                                                      data_key     = test_key                   ,
                                                                      data_file_id = Safe_Str__Id("integ-json") )

            self.routes_data_store.data__store_binary__with__id_and_key(body         = b'integ binary'          ,
                                                                        cache_id     = parent.cache_id          ,
                                                                        namespace    = self.test_namespace      ,
                                                                        data_key     = test_key                 ,
                                                                        data_file_id = Safe_Str__Id("integ-bin"))

            # List all files
            all_result = _.data__list(cache_id  = parent.cache_id   ,
                                      namespace = self.test_namespace,
                                      recursive = True              )

            assert all_result.file_count == 3

            # List files under key only
            key_result = _.data__list__with__key(cache_id  = parent.cache_id    ,
                                                 namespace = self.test_namespace,
                                                 data_key  = test_key           ,
                                                 recursive = True               )

            assert key_result.file_count == 3
            assert key_result.data_key   == test_key

            # Verify file info
            file_ids = [f.data_file_id for f in key_result.files]
            assert file_ids == ["integ-str", "integ-json", "integ-bin"]

            # Verify types
            type_map = {f.data_file_id: f.data_type for f in key_result.files}
            assert type_map[Safe_Str__Id("integ-str" )]  == Enum__Cache__Data_Type.STRING
            assert type_map[Safe_Str__Id("integ-json")] == Enum__Cache__Data_Type.JSON
            assert type_map[Safe_Str__Id("integ-bin" )]  == Enum__Cache__Data_Type.BINARY