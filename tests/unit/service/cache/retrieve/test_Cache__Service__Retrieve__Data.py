import pytest
from unittest                                                                           import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                                 import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                             import __
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                   import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id         import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path       import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text            import Safe_Str__Text
from osbot_utils.type_safe.primitives.core.Safe_UInt                                    import Safe_UInt
from osbot_utils.utils.Objects                                                          import base_classes
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                 import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy           import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Store__Request     import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve            import Cache__Service__Retrieve
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve__Data      import Cache__Service__Retrieve__Data, Schema__Cache__Data__File__Info, Schema__Cache__Data__File__Content
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                  import Cache__Service__Store
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store__Data            import Cache__Service__Store__Data
from tests.unit.Service__Cache__Test_Objs                                               import setup__service__cache__test_objs


class test_Cache__Service__Retrieve__Data(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs           = setup__service__cache__test_objs()
        cls.cache_fixtures      = cls.test_objs.cache_fixtures
        cls.cache_service       = cls.cache_fixtures.cache_service
        cls.service__store      = Cache__Service__Store        (cache_service = cls.cache_service)
        cls.service__store_data = Cache__Service__Store__Data  (cache_service = cls.cache_service)
        cls.service__retrieve   = Cache__Service__Retrieve__Data(cache_service = cls.cache_service)

        cls.test_namespace      = Safe_Str__Id("test-retrieve-data")                            # Test data
        cls.test_cache_key      = Safe_Str__File__Path("test/retrieve")

        # Create parent cache entry
        parent_response         = cls.service__store.store_string(data      = "parent for data retrieval"         ,
                                                                  namespace = cls.test_namespace                  ,
                                                                  strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                                  cache_key = cls.test_cache_key                  ,
                                                                  file_id   = Safe_Str__Id("retrieve-parent")     )
        cls.parent_cache_id     = parent_response.cache_id

        # Create test data files
        cls.test_data_files = {}
        for data_type, data in [(Enum__Cache__Data_Type.STRING , "test data string"          ),
                                (Enum__Cache__Data_Type.JSON   , {"data": "json", "id": 456} ),
                                (Enum__Cache__Data_Type.BINARY , b"test data bytes\x00\x01"  )]:
            data_key     = Safe_Str__File__Path(f"test/{data_type.value}")
            data_file_id = Safe_Str__Id(f"test-{data_type.value}-data")
            data_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id,
                                                               data         = data,
                                                               data_type    = data_type.value,
                                                               data_key     = data_key,
                                                               data_file_id = data_file_id,
                                                               namespace    = cls.test_namespace)
            data_response = cls.service__store_data.store_data(data_request)

            cls.test_data_files[data_type] = { "data"         : data          ,
                                               "data_key"     : data_key      ,
                                               "data_file_id" : data_file_id  ,
                                               "result"       : data_response }

    def test__init__(self):                                                                     # Test auto-initialization
        with Cache__Service__Retrieve__Data() as _:
            assert type(_)               is Cache__Service__Retrieve__Data
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

    def test_retrieve_data__string(self):                                                       # Test retrieving string data
        with self.service__retrieve as _:
            data_info = self.test_data_files["string"]

            result = _.retrieve_data(cache_id     = self.parent_cache_id                        ,
                                    data_key     = data_info["data_key"]                        ,
                                    data_file_id = data_info["data_file_id"]                    ,
                                    namespace    = self.test_namespace                          )

            assert type(result) is Schema__Cache__Data__File__Content
            assert result.data          == data_info["data"]
            assert result.data_type     == Enum__Cache__Data_Type.STRING
            assert result.data_file_id  == data_info["data_file_id"]
            assert result.data_key      == data_info["data_key"]
            assert type(result.full_path) is Safe_Str__File__Path

            assert result.obj().contains(__(data        = data_info["data"]             ,               # Use .obj() for comprehensive comparison
                                           data_type    = Enum__Cache__Data_Type.STRING ,
                                           data_file_id = data_info["data_file_id"]     ))
            assert result.obj()      == __(data         = 'test data string',
                                           data_type    = 'string'          ,
                                           data_file_id = 'test-string-data',
                                           data_key     = 'test/string'     ,
                                           full_path    = 'test-retrieve-data/data/semantic-file/test/retrieve/retrieve-parent/data/test/string/test-string-data.txt',
                                           size         = 16                )

    def test_retrieve_data__json(self):                                                         # Test retrieving JSON data
        with self.service__retrieve as _:
            data_info = self.test_data_files["json"]

            result = _.retrieve_data(cache_id     = self.parent_cache_id      ,
                                     data_key     = data_info["data_key"]     ,
                                     data_file_id = data_info["data_file_id"] ,
                                     namespace    = self.test_namespace       )

            assert type(result) is Schema__Cache__Data__File__Content
            assert result.data          == data_info["data"]
            assert result.data_type     == Enum__Cache__Data_Type.JSON
            assert result.data_file_id  == data_info["data_file_id"]
            assert result.obj()         == __(data         = __(data        = 'json'  ,
                                                                id          = 456     ),
                                              data_type    = 'json'    ,
                                              data_file_id = 'test-json-data'   ,
                                              data_key     ='test/json'         ,
                                              full_path    ='test-retrieve-data/data/semantic-file/test/retrieve/retrieve-parent/data/test/json/test-json-data.json',
                                              size         = 27             )

    def test_retrieve_data__binary(self):                                                       # Test retrieving binary data
        with self.service__retrieve as _:
            data_info = self.test_data_files["binary"]

            result = _.retrieve_data(cache_id     = self.parent_cache_id                        ,
                                    data_key     = data_info["data_key"]                        ,
                                    data_file_id = data_info["data_file_id"]                    ,
                                    namespace    = self.test_namespace                          )

            assert type(result) is Schema__Cache__Data__File__Content
            assert result.data       == data_info["data"]
            assert result.data_type  == Enum__Cache__Data_Type.BINARY
            assert result.data_file_id == data_info["data_file_id"]

    def test_retrieve_data__not_found(self):                                                    # Test retrieving non-existent data
        with self.service__retrieve as _:
            result = _.retrieve_data(cache_id     = self.parent_cache_id                        ,
                                    data_key     = Safe_Str__File__Path("non/existent")         ,
                                    data_file_id = Safe_Str__Id("not-found")                    ,
                                    namespace    = self.test_namespace                          )

            assert result is None                                                               # Not found returns None

    def test_retrieve_data__missing_cache_id(self):                                             # Test parameter validation
        with self.service__retrieve as _:
            with pytest.raises(ValueError) as exc_info:
                _.retrieve_data(cache_id     = None                                             ,  # Missing
                               data_key     = Safe_Str__File__Path("test")                     ,
                               data_file_id = Safe_Str__Id("test")                              ,
                               namespace    = self.test_namespace                               )

            assert "cache_id is required" in str(exc_info.value)

    def test_retrieve_data__non_existent_cache_id(self):                                        # Test with non-existent cache
        with self.service__retrieve as _:
            result = _.retrieve_data(cache_id     = Random_Guid()                               ,  # Doesn't exist
                                    data_key     = Safe_Str__File__Path("test")                 ,
                                    data_file_id = Safe_Str__Id("test")                          ,
                                    namespace    = self.test_namespace                          )

            assert result is None                                                               # Returns None

    def test_list_data_files(self):                                                             # Test listing all data files
        with self.service__retrieve as _:
            data_files = _.list_data_files(cache_id  = self.parent_cache_id                     ,
                                          namespace = self.test_namespace                        )

            assert type(data_files) is list
            assert len(data_files) >= 3                                                         # At least our 3 test files

            for file_info in data_files:
                assert type(file_info) is Schema__Cache__Data__File__Info
                assert type(file_info.data_file_id) is Safe_Str__Id
                assert type(file_info.data_key)     is Safe_Str__File__Path
                assert type(file_info.full_path)    is Safe_Str__File__Path
                assert type(file_info.data_type)    is Enum__Cache__Data_Type
                assert type(file_info.extension)    is Safe_Str__Text
                assert type(file_info.size)         is Safe_UInt

    def test_list_data_files__with_filter(self):                                                # Test listing with data_key filter
        with self.service__retrieve as _:
            # List only files under test/string path
            data_files = _.list_data_files(cache_id  = self.parent_cache_id                     ,
                                          data_key  = Safe_Str__File__Path("test/string")       ,
                                          namespace = self.test_namespace                        )

            # Should find the string test file
            assert len(data_files) >= 1
            string_files = [f for f in data_files if f.data_file_id == Safe_Str__Id("test-string-data")]
            assert len(string_files) == 1

    def test_list_data_files__empty_cache(self):                                                # Test listing when no data files exist
        skip__if_not__in_github_actions()
        with self.service__retrieve as _:
            # Create cache entry with no data files
            empty_parent = self.service__store.store_string(data      = "empty parent"                           ,
                                                           namespace = self.test_namespace                       ,
                                                           strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                           cache_key = Safe_Str__File__Path("empty/parent")      ,
                                                           file_id   = Safe_Str__Id("empty-001")                 )

            data_files = _.list_data_files(cache_id  = empty_parent.cache_id                    ,
                                          namespace = self.test_namespace                        )

            assert type(data_files) is list
            assert len(data_files) == 0                                                         # No data files

    def test_list_data_files__missing_cache_id(self):                                           # Test parameter validation for list
        with self.service__retrieve as _:
            with pytest.raises(ValueError) as exc_info:
                _.list_data_files(cache_id  = None                                              ,  # Missing
                                 namespace = self.test_namespace                                )

            assert "cache_id is required" in str(exc_info.value)

    def test_count_data_files(self):                                                            # Test counting data files
        with self.service__retrieve as _:
            count = _.count_data_files(cache_id  = self.parent_cache_id                         ,
                                      namespace = self.test_namespace                           )

            assert type(count) is Safe_UInt
            assert count >= 3                                                                   # At least our 3 test files

    def test_delete_data_file(self):                                                            # Test deleting a data file
        skip__if_not__in_github_actions()
        with self.service__retrieve as _:
            # Create a data file to delete
            delete_data_key     = Safe_Str__File__Path("delete/test")
            delete_data_file_id = Safe_Str__Id("file-to-delete")

            self.service__store_data.store_data(cache_id     = self.parent_cache_id             ,
                                               data         = "data to delete"                  ,
                                               data_type    = Enum__Cache__Data_Type.STRING     ,
                                               data_key     = delete_data_key                   ,
                                               data_file_id = delete_data_file_id               ,
                                               namespace    = self.test_namespace               )

            # Verify it exists
            exists = _.retrieve_data(cache_id     = self.parent_cache_id                        ,
                                    data_key     = delete_data_key                              ,
                                    data_file_id = delete_data_file_id                          ,
                                    namespace    = self.test_namespace                          )
            assert exists is not None

            # Delete it
            deleted = _.delete_data_file(cache_id     = self.parent_cache_id                    ,
                                        data_key     = delete_data_key                          ,
                                        data_file_id = delete_data_file_id                      ,
                                        namespace    = self.test_namespace                      )

            assert deleted is True                                                              # Successfully deleted

            # Verify it's gone
            gone = _.retrieve_data(cache_id     = self.parent_cache_id                          ,
                                  data_key     = delete_data_key                                ,
                                  data_file_id = delete_data_file_id                            ,
                                  namespace    = self.test_namespace                            )
            assert gone is None

    def test_delete_data_file__not_found(self):                                                 # Test deleting non-existent file
        with self.service__retrieve as _:
            deleted = _.delete_data_file(cache_id     = self.parent_cache_id                    ,
                                        data_key     = Safe_Str__File__Path("never/existed")    ,
                                        data_file_id = Safe_Str__Id("never-existed")            ,
                                        namespace    = self.test_namespace                      )

            assert deleted is False                                                             # Not found

    def test_delete_data_file__missing_parameters(self):                                        # Test parameter validation
        with self.service__retrieve as _:
            # Missing cache_id
            with pytest.raises(ValueError) as exc_info:
                _.delete_data_file(cache_id     = None                                          ,
                                  data_file_id = Safe_Str__Id("test")                           ,
                                  namespace    = self.test_namespace                            )
            assert "cache_id is required" in str(exc_info.value)

            # Missing data_file_id
            with pytest.raises(ValueError) as exc_info:
                _.delete_data_file(cache_id     = self.parent_cache_id                          ,
                                  data_file_id = None                                           ,
                                  namespace    = self.test_namespace                            )
            assert "data_file_id is required" in str(exc_info.value)

    def test_delete_all_data_files(self):                                                       # Test deleting all data files
        skip__if_not__in_github_actions()
        with self.service__retrieve as _:
            # Create cache with data files to delete
            delete_parent = self.service__store.store_string(data      = "parent for delete all"                ,
                                                            namespace = self.test_namespace                      ,
                                                            strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                            cache_key = Safe_Str__File__Path("delete/all")       ,
                                                            file_id   = Safe_Str__Id("delete-parent")            )

            # Add multiple data files
            for i in range(5):
                self.service__store_data.store_data(cache_id     = delete_parent.cache_id       ,
                                                   data         = f"delete data {i}"            ,
                                                   data_type    = Enum__Cache__Data_Type.STRING ,
                                                   data_key     = Safe_Str__File__Path("batch")  ,
                                                   data_file_id = Safe_Str__Id(f"del-data-{i}")  ,
                                                   namespace    = self.test_namespace           )

            # Verify they exist
            count_before = _.count_data_files(cache_id  = delete_parent.cache_id                ,
                                             namespace = self.test_namespace                     )
            assert count_before == 5

            # Delete all
            deleted_count = _.delete_all_data_files(cache_id  = delete_parent.cache_id          ,
                                                   namespace = self.test_namespace               )

            assert type(deleted_count) is Safe_UInt
            assert deleted_count == 5

            # Verify they're gone
            count_after = _.count_data_files(cache_id  = delete_parent.cache_id                 ,
                                            namespace = self.test_namespace                      )
            assert count_after == 0

    def test_get_data_folder_size(self):                                                        # Test calculating total size
        with self.service__retrieve as _:
            total_size = _.get_data_folder_size(cache_id  = self.parent_cache_id                ,
                                               namespace = self.test_namespace                   )

            assert type(total_size) is Safe_UInt
            assert total_size > 0                                                               # Should have some size

    def test_retrieve_service(self):                                                            # Test retrieve service caching
        with self.service__retrieve as _:
            retrieve1 = _.retrieve_service()
            retrieve2 = _.retrieve_service()

            assert retrieve1 is retrieve2                                                       # Same instance (cached)
            assert type(retrieve1) is Cache__Service__Retrieve

    def test_schema__data__file__info(self):                                                    # Test Schema__Data__File__Info
        with Schema__Cache__Data__File__Info() as _:
            assert type(_) is Schema__Cache__Data__File__Info
            assert base_classes(_)      == [Type_Safe, object]
            assert type(_.data_file_id) is Safe_Str__Id
            assert type(_.data_key)     is Safe_Str__File__Path
            assert type(_.full_path)    is Safe_Str__File__Path
            assert type(_.data_type)    is Enum__Cache__Data_Type
            assert type(_.extension)    is Safe_Str__Text
            assert type(_.size)         is Safe_UInt

            # Test with data
            _.data_file_id = Safe_Str__Id("info-data")
            _.data_key     = Safe_Str__File__Path("path/to")
            _.full_path    = Safe_Str__File__Path("full/path/to/data.txt")
            _.data_type    = Enum__Cache__Data_Type.STRING
            _.extension    = Safe_Str__Text("txt")
            _.size         = Safe_UInt(1024)

            assert _.obj() == __(data_file_id = "info-data"                                     ,
                                data_key     = "path/to"                                        ,
                                full_path    = "full/path/to/data.txt"                          ,
                                data_type    = Enum__Cache__Data_Type.STRING                    ,
                                extension    = "txt"                                            ,
                                size         = 1024                                             )

    def test_schema__data__file__content(self):                                                 # Test Schema__Data__File__Content
        with Schema__Cache__Data__File__Content() as _:
            assert type(_) is Schema__Cache__Data__File__Content
            assert base_classes(_)      == [Type_Safe, object]
            # Note: 'data' is Any type, so no type checking
            assert type(_.data_type)    is Enum__Cache__Data_Type
            assert type(_.data_file_id) is Safe_Str__Id
            assert type(_.data_key)     is Safe_Str__File__Path
            assert type(_.full_path)    is Safe_Str__File__Path
            assert type(_.size)         is Safe_UInt

            # Test with string data
            _.data         = "test content data"
            _.data_type    = Enum__Cache__Data_Type.STRING
            _.data_file_id = Safe_Str__Id("content-data")
            _.data_key     = Safe_Str__File__Path("content/path")
            _.full_path    = Safe_Str__File__Path("full/content/path/data.txt")
            _.size         = Safe_UInt(len("test content data"))

            assert _.obj() == __(data         = "test content data"                             ,
                                data_type    = Enum__Cache__Data_Type.STRING                    ,
                                data_file_id = "content-data"                                   ,
                                data_key     = "content/path"                                   ,
                                full_path    = "full/content/path/data.txt"                     ,
                                size         = 17                                               )

    def test_retrieve_data__without_data_key(self):                                             # Test retrieval without data_key
        skip__if_not__in_github_actions()
        with self.service__retrieve as _:
            # Store data without data_key
            direct_file_id = Safe_Str__Id("direct-file")
            self.service__store_data.store_data(cache_id     = self.parent_cache_id             ,
                                               data         = "direct data"                     ,
                                               data_type    = Enum__Cache__Data_Type.STRING     ,
                                               data_key     = None                              ,  # No key
                                               data_file_id = direct_file_id                    ,
                                               namespace    = self.test_namespace               )

            # Retrieve it
            result = _.retrieve_data(cache_id     = self.parent_cache_id                        ,
                                    data_key     = None                                         ,
                                    data_file_id = direct_file_id                               ,
                                    namespace    = self.test_namespace                          )

            assert result is not None
            assert result.data == "direct data"
            assert result.data_file_id == direct_file_id