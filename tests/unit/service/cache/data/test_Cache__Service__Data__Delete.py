import pytest
from unittest                                                                                    import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                             import Path__Handler__Temporal
from osbot_utils.testing.__                                                                      import __
from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                            import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                  import Safe_Str__Id
from osbot_utils.utils.Objects                                                                   import base_classes
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Delete__All_Files__Response import Schema__Cache__Data__Delete__All_Files__Response
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request           import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Store__Request              import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                          import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy                    import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                        import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Delete                     import Cache__Service__Data__Delete
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store                      import Cache__Service__Data__Store
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve                   import Cache__Service__Data__Retrieve
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve                     import Cache__Service__Retrieve
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                           import Cache__Service__Store
from tests.unit.Service__Cache__Test_Objs                                                        import setup__service__cache__test_objs


class test_Cache__Service__Data__Delete(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs              = setup__service__cache__test_objs()
        cls.cache_fixtures         = cls.test_objs.cache_fixtures
        cls.service__cache         = cls.cache_fixtures.cache_service
        cls.service__store         = Cache__Service__Store        (cache_service = cls.service__cache)
        cls.service__retrieve      = Cache__Service__Retrieve     (cache_service = cls.service__cache)
        cls.service__store_data    = Cache__Service__Data__Store  (cache_service = cls.service__cache)
        cls.service__retrieve_data = Cache__Service__Data__Retrieve(cache_service = cls.service__cache)
        cls.service__delete_data   = Cache__Service__Data__Delete (cache_service = cls.service__cache)

        cls.test_namespace = Safe_Str__Id("test-data-delete")                                   # Test namespace
        cls.test_cache_key = Safe_Str__File__Path("app/data")                                   # Test cache key

        cls.parent_response = cls.service__store.store_string(data      = "parent for deletion"                 ,
                                                              namespace = cls.test_namespace                    ,
                                                              strategy  = Enum__Cache__Store__Strategy.KEY_BASED,
                                                              cache_key = cls.test_cache_key                    ,
                                                              file_id   = Safe_Str__Id("parent-delete")         )
        cls.parent_cache_id = cls.parent_response.cache_id
        cls.path_now        = Path__Handler__Temporal().path_now()                           # Current temporal path
        cls._setup_test_data()                                                               # Create test data to delete

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup data files for deletion testing
        # Store string data without key
        cls.string_store_1 = cls.service__store_data.store_data(
            Schema__Cache__Data__Store__Request(
                cache_id     = cls.parent_cache_id,
                data         = "delete me string 1",
                data_type    = Enum__Cache__Data_Type.STRING,
                data_file_id = Safe_Str__Id("string-001"),
                namespace    = cls.test_namespace
            )
        )

        # Store string data with key
        cls.string_store_2 = cls.service__store_data.store_data(
            Schema__Cache__Data__Store__Request(
                cache_id     = cls.parent_cache_id,
                data         = "delete me string 2",
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("logs"),
                data_file_id = Safe_Str__Id("string-002"),
                namespace    = cls.test_namespace
            )
        )

        # Store JSON data
        cls.json_store = cls.service__store_data.store_data(
            Schema__Cache__Data__Store__Request(
                cache_id     = cls.parent_cache_id,
                data         = {"delete": "this", "count": 99},
                data_type    = Enum__Cache__Data_Type.JSON,
                data_key     = Safe_Str__File__Path("configs"),
                data_file_id = Safe_Str__Id("json-001"),
                namespace    = cls.test_namespace
            )
        )

        # Store binary data
        cls.binary_store = cls.service__store_data.store_data(
            Schema__Cache__Data__Store__Request(
                cache_id     = cls.parent_cache_id,
                data         = b"delete me binary \x00\x01",
                data_type    = Enum__Cache__Data_Type.BINARY,
                data_key     = Safe_Str__File__Path("uploads"),
                data_file_id = Safe_Str__Id("binary-001"),
                namespace    = cls.test_namespace
            )
        )

    def test__init__(self):                                                                     # Test auto-initialization
        with Cache__Service__Data__Delete() as _:
            assert type(_)                   is Cache__Service__Data__Delete
            assert base_classes(_)           == [Type_Safe, object]
            assert type(_.cache_service)     is Cache__Service
            assert type(_.retrieve_service()) is Cache__Service__Retrieve

    def test_build_data_file_path(self):                                                        # Test centralized path construction
        with self.service__delete_data as _:
            data_folder = Safe_Str__File__Path("namespace/data/temporal/cache-123")

            # Test with all parameters
            path = _.build_data_file_path(data_folder   = data_folder,
                                          data_key      = Safe_Str__File__Path("logs/2024"),
                                          data_file_id  = Safe_Str__Id("log-001"),
                                          data_type     = Enum__Cache__Data_Type.STRING)
            assert path == "namespace/data/temporal/cache-123/logs/2024/log-001.txt"

            # Test without data_key
            path = _.build_data_file_path(data_folder   = data_folder,
                                          data_key      = None,
                                          data_file_id  = Safe_Str__Id("log-002"),
                                          data_type     = Enum__Cache__Data_Type.JSON)
            assert path == "namespace/data/temporal/cache-123/log-002.json"

            # Test required data_file_id
            with pytest.raises(ValueError, match="data_file_id is required"):
                _.build_data_file_path(data_folder = data_folder,
                                      data_type    = Enum__Cache__Data_Type.STRING)

    def test_get_extension_for_type(self):                                                      # Test file extension mapping
        with self.service__delete_data as _:
            assert _.get_extension_for_type(Enum__Cache__Data_Type.STRING) == 'txt'
            assert _.get_extension_for_type(Enum__Cache__Data_Type.JSON)   == 'json'
            assert _.get_extension_for_type(Enum__Cache__Data_Type.BINARY) == 'bin'
            assert _.get_extension_for_type(None)                         == 'data'             # Default

    def test_delete_data_file__string(self):                                                    # Test deleting string file
        with self.service__delete_data as _:
            # Verify file exists before deletion
            retrieve_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_file_id = Safe_Str__Id("string-001"),
                namespace    = self.test_namespace
            )
            result = self.service__retrieve_data.retrieve_data(retrieve_request)
            assert result.found is True
            assert result.data  == "delete me string 1"

            # Delete the file
            deleted = _.delete_data_file(retrieve_request)
            assert deleted is True

            # Verify file is gone
            result = self.service__retrieve_data.retrieve_data(retrieve_request)
            assert result.found is False

    def test_delete_data_file__with_key(self):                                                  # Test deleting file with data_key
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("logs"),
                data_file_id = Safe_Str__Id("string-002"),
                namespace    = self.test_namespace
            )

            # Delete the file
            deleted = _.delete_data_file(delete_request)
            assert deleted is True

            # Verify deletion
            result = self.service__retrieve_data.retrieve_data(delete_request)
            assert result.found is False

    def test_delete_data_file__json(self):                                                      # Test deleting JSON file
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.JSON,
                data_key     = Safe_Str__File__Path("configs"),
                data_file_id = Safe_Str__Id("json-001"),
                namespace    = self.test_namespace
            )

            deleted = _.delete_data_file(delete_request)
            assert deleted is True

    def test_delete_data_file__binary(self):                                                    # Test deleting binary file
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.BINARY,
                data_key     = Safe_Str__File__Path("uploads"),
                data_file_id = Safe_Str__Id("binary-001"),
                namespace    = self.test_namespace
            )

            deleted = _.delete_data_file(delete_request)
            assert deleted is True

    def test_delete_data_file__not_found(self):                                                 # Test deleting non-existent file
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_file_id = Safe_Str__Id("non-existent"),
                namespace    = self.test_namespace
            )

            deleted = _.delete_data_file(delete_request)
            assert deleted is False                                                             # Returns False when file doesn't exist

    def test_delete_data_file__missing_data_file_id(self):                                     # Test required data_file_id
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id  = self.parent_cache_id,
                data_type = Enum__Cache__Data_Type.STRING,
                namespace = self.test_namespace
                # Missing data_file_id
            )

            with pytest.raises(ValueError, match="data_file_id is required"):
                _.delete_data_file(delete_request)

    def test_delete_data_file__missing_data_type(self):                                         # Test required data_type
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = self.parent_cache_id,
                data_file_id = Safe_Str__Id("test"),
                namespace    = self.test_namespace
                # Missing data_type
            )

            with pytest.raises(ValueError, match="data_type is required"):
                _.delete_data_file(delete_request)

    def test_delete_data_file__invalid_parent(self):                                            # Test with non-existent parent
        with self.service__delete_data as _:
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = Random_Guid(),                                                   # Non-existent parent
                data_type    = Enum__Cache__Data_Type.STRING,
                data_file_id = Safe_Str__Id("test"),
                namespace    = self.test_namespace
            )

            deleted = _.delete_data_file(delete_request)
            assert deleted is False                                                             # Returns False when parent doesn't exist

    def test_delete_all_data_files__no_key(self):                                               # Test deleting all files
        with self.service__delete_data as _:                                                    # Create parent with multiple files
            parent     = self.service__store.store_string(data      = "parent with many files"              ,
                                                          namespace = self.test_namespace                   ,
                                                          strategy  = Enum__Cache__Store__Strategy.TEMPORAL )
            cache_id   = parent.cache_id
            data_files = [ f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data/file-0.txt',
                           f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data/file-1.txt',
                           f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data/file-2.txt',
                           f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data/file-3.txt',
                           f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}/data/file-4.txt']
            for i in range(5):                                                              # Store multiple files
                store_request__kwargs = dict(cache_id     = cache_id                  ,
                                             data         = f"file {i}"                     ,
                                             data_type    = Enum__Cache__Data_Type.STRING   ,
                                             data_file_id = Safe_Str__Id(f"file-{i}")       ,
                                             namespace    = self.test_namespace             )
                self.service__store_data.store_data(Schema__Cache__Data__Store__Request(**store_request__kwargs))

            files_list = self.service__retrieve_data.list_data_files(cache_id=cache_id, namespace=self.test_namespace)
            assert files_list.obj() == __(cache_id   = cache_id          ,
                                          namespace  = 'test-data-delete',
                                          data_key   = None              ,
                                          data_files =data_files         )


            # Delete all files
            result = _.delete_all_data_files(cache_id  = parent.cache_id,
                                            namespace = self.test_namespace)
            assert type(result) is Schema__Cache__Data__Delete__All_Files__Response
            assert result.obj() == __(deleted_count  = 5,
                                       deleted_files = data_files)
            # Verify all deleted
            for i in range(5):
                retrieve_request = Schema__Cache__Data__Retrieve__Request(
                    cache_id     = parent.cache_id,
                    data_type    = Enum__Cache__Data_Type.STRING,
                    data_file_id = Safe_Str__Id(f"file-{i}"),
                    namespace    = self.test_namespace
                )
                assert self.service__retrieve_data.retrieve_data(retrieve_request).found is False

    def test_delete_all_data_files__with_key(self):                                             # Test deleting files under specific key
        with self.service__delete_data as _:                                                    # Create parent with files in different keys
            parent = self.service__store.store_string(data      = "parent with categorized files",
                                                      namespace = self.test_namespace,
                                                      strategy  = Enum__Cache__Store__Strategy.TEMPORAL
            )

            for i in range(3):                                                                          # Store files in "logs" key
                self.service__store_data.store_data(Schema__Cache__Data__Store__Request(cache_id     = parent.cache_id,
                                                                                        data         = f"log {i}",
                                                                                        data_type    = Enum__Cache__Data_Type.STRING,
                                                                                        data_key     = Safe_Str__File__Path("logs"),
                                                                                        data_file_id = Safe_Str__Id(f"log-{i}"),
                                                                                        namespace    = self.test_namespace))

            for i in range(2):                                                                          # Store files in "configs" key
                self.service__store_data.store_data(Schema__Cache__Data__Store__Request(cache_id     = parent.cache_id,
                                                                                        data         = {"config": i},
                                                                                        data_type    = Enum__Cache__Data_Type.JSON,
                                                                                        data_key     = Safe_Str__File__Path("configs"),
                                                                                        data_file_id = Safe_Str__Id(f"config-{i}"),
                                                                                        namespace    = self.test_namespace))

            result = _.delete_all_data_files(cache_id  = parent.cache_id,                       # Delete only "logs" files
                                             namespace = self.test_namespace,
                                             data_key  = Safe_Str__File__Path("logs"))

            assert result.obj() == __(deleted_count=3,
                                      deleted_files=[ f'test-data-delete/data/temporal/{self.path_now}/{parent.cache_id}/data/logs/log-0.txt',
                                                      f'test-data-delete/data/temporal/{self.path_now}/{parent.cache_id}/data/logs/log-1.txt',
                                                      f'test-data-delete/data/temporal/{self.path_now}/{parent.cache_id}/data/logs/log-2.txt'])



            # Verify logs deleted
            for i in range(3):
                retrieve_request = Schema__Cache__Data__Retrieve__Request(cache_id     = parent.cache_id                ,
                                                                          data_type    = Enum__Cache__Data_Type.STRING  ,
                                                                          data_key     = Safe_Str__File__Path("logs")   ,
                                                                          data_file_id = Safe_Str__Id(f"log-{i}")       ,
                                                                          namespace    = self.test_namespace            )
                assert self.service__retrieve_data.retrieve_data(retrieve_request).found is False

            # Verify configs still exist
            for i in range(2):
                retrieve_request = Schema__Cache__Data__Retrieve__Request(cache_id     = parent.cache_id                ,
                                                                          data_type    = Enum__Cache__Data_Type.JSON    ,
                                                                          data_key     = Safe_Str__File__Path("configs"),
                                                                          data_file_id = Safe_Str__Id(f"config-{i}")    ,
                                                                          namespace    = self.test_namespace            )
                assert self.service__retrieve_data.retrieve_data(retrieve_request).found is True

    def test_delete_all_data_files__no_files(self):                                             # Test deleting when no files exist
        with self.service__delete_data as _:
            # Create parent with no data files
            parent = self.service__store.store_string(data      = "empty parent"                     ,
                                                      namespace = self.test_namespace                ,
                                                      strategy  = Enum__Cache__Store__Strategy.DIRECT)

            result = _.delete_all_data_files(cache_id  = parent.cache_id,
                                            namespace = self.test_namespace)

            assert result.obj() == __(deleted_count=0, deleted_files=[])

    def test_delete_all_data_files__invalid_parent(self):                                       # Test with non-existent parent
        with self.service__delete_data as _:
            result = _.delete_all_data_files(cache_id  = Random_Guid(),
                                            namespace = self.test_namespace)

            assert result.obj() == __(deleted_count=0, deleted_files=[])

    def test__integration__store_retrieve_delete_cycle(self):                                   # Full integration test
        with self.service__delete_data as _:
            parent = self.service__store.store_string(data      = "integration test parent"            ,        # Create new parent
                                                      namespace = self.test_namespace                  ,
                                                      strategy  = Enum__Cache__Store__Strategy.TEMPORAL)

            # Store data
            test_data = "integration test data"
            store_request = Schema__Cache__Data__Store__Request(
                cache_id     = parent.cache_id,
                data         = test_data,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("integration"),
                data_file_id = Safe_Str__Id("test-001"),
                namespace    = self.test_namespace
            )
            self.service__store_data.store_data(store_request)

            # Retrieve to verify it exists
            retrieve_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = parent.cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,
                data_key     = Safe_Str__File__Path("integration"),
                data_file_id = Safe_Str__Id("test-001"),
                namespace    = self.test_namespace
            )
            result = self.service__retrieve_data.retrieve_data(retrieve_request)
            assert result.found is True
            assert result.data  == test_data

            # Delete it
            deleted = _.delete_data_file(retrieve_request)
            assert deleted is True

            # Verify deletion
            result = self.service__retrieve_data.retrieve_data(retrieve_request)
            assert result.found is False

    def test__wrong_type_doesnt_delete(self):                                                   # Test type safety prevents wrong deletions
        with self.service__delete_data as _:
            # Store a JSON file
            parent = self.service__store.store_string(data      = "type safety test"                   ,
                                                      namespace = self.test_namespace                  ,
                                                      strategy  = Enum__Cache__Store__Strategy.TEMPORAL)

            store_request = Schema__Cache__Data__Store__Request(cache_id     = parent.cache_id,
                                                                data         = {"safe": "data"},
                                                                data_type    = Enum__Cache__Data_Type.JSON,
                                                                data_file_id = Safe_Str__Id("safe-001"),
                                                                namespace    = self.test_namespace)
            self.service__store_data.store_data(store_request)

            # Try to delete with wrong type
            delete_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = parent.cache_id,
                data_type    = Enum__Cache__Data_Type.STRING,                                   # Wrong type!
                data_file_id = Safe_Str__Id("safe-001"),
                namespace    = self.test_namespace
            )
            deleted = _.delete_data_file(delete_request)
            assert deleted is False                                                             # Doesn't delete wrong type

            # Verify JSON file still exists
            retrieve_request = Schema__Cache__Data__Retrieve__Request(
                cache_id     = parent.cache_id,
                data_type    = Enum__Cache__Data_Type.JSON,                                     # Correct type
                data_file_id = Safe_Str__Id("safe-001"),
                namespace    = self.test_namespace
            )
            result = self.service__retrieve_data.retrieve_data(retrieve_request)
            assert result.found is True
            assert result.data  == {"safe": "data"}