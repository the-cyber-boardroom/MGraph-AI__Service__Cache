import pytest
from unittest                                                                           import TestCase
from osbot_utils.testing.__                                                             import __
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                   import Random_Guid
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__List                   import Type_Safe__List
from osbot_utils.utils.Objects                                                          import base_classes
from osbot_utils.utils.Zip                                                              import zip_bytes_empty, zip_bytes__add_file, zip_bytes__file_list
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from mgraph_ai_service_cache_client.schemas.cache.zip.enums.Enum__Cache__Zip__Operation        import Enum__Cache__Zip__Operation
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store               import Cache__Service__Zip__Store
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Operations          import Cache__Service__Zip__Operations
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Store__Request       import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Request   import Schema__Cache__Zip__Operation__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Response  import Schema__Cache__Zip__Operation__Response


class test_Cache__Service__Zip__Operations(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_service = Cache__Service()                                                    # Create cache service once
        cls.store_service = Cache__Service__Zip__Store     (cache_service=cls.cache_service)    # For storing test zips
        cls.ops_service   = Cache__Service__Zip__Operations(cache_service=cls.cache_service)

        # Create test zip files once
        cls.test_zip = zip_bytes_empty()
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "file1.txt"    , b"content 1"     )
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "file2.txt"    , b"content 2"     )
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "dir/file3.txt", b"nested content")

        cls.test_namespace = "test-ops"

    def setUp(self):                                                                            # Per-test lightweight setup
        store_request = Schema__Cache__Zip__Store__Request(zip_bytes = self.test_zip       ,    # Store a fresh zip for each test
                                                           namespace = self.test_namespace )
        store_result = self.store_service.store_zip(store_request)
        self.test_cache_id = store_result.cache_id

    def test__init__(self):                                                               # Test service initialization
        with Cache__Service__Zip__Operations() as _:
            assert type(_)               is Cache__Service__Zip__Operations
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

    def test_perform_operation__list(self):                                               # Test listing files in zip
        with self.ops_service as _:
            request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
                                                             operation = "list"              ,
                                                             namespace = self.test_namespace )

            result = _.perform_operation(request)

            assert type(result)          is Schema__Cache__Zip__Operation__Response

            assert result.success        == True
            assert result.operation      == "list"
            assert result.cache_id       == self.test_cache_id                            # List doesn't change ID
            assert len(result.file_list) == 3
            assert "file1.txt"           in result.file_list
            assert "file2.txt"           in result.file_list
            assert "dir/file3.txt"       in result.file_list
            assert result.message        == "Found 3 files in zip"

            assert result.obj()          == __(message        = 'Found 3 files in zip',
                                               file_list      = ['dir/file3.txt', 'file1.txt', 'file2.txt'],
                                               file_content   = b''               ,
                                               file_size      = 0                 ,
                                               files_affected = []                ,
                                               error_details  = ''                ,
                                               success        = True              ,
                                               operation      = 'list'            ,
                                               cache_id       = self.test_cache_id)


    def test_perform_operation__get(self):                                                # Test extracting file from zip
        with self.ops_service as _:
            request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
                                                             operation = "get"               ,
                                                             file_path = "file1.txt"         ,
                                                             namespace = self.test_namespace )

            result = _.perform_operation(request)

            assert type(result)         is Schema__Cache__Zip__Operation__Response
            assert result.success       == True
            assert result.operation     == "get"
            assert result.cache_id      == self.test_cache_id                             # Get doesn't change ID
            assert result.file_content  == b"content 1"
            assert result.file_size     == 9
            assert result.message       == "Retrieved 'file1.txt'"
            assert result.obj()        == __(message        = "Retrieved 'file1.txt'",
                                             file_list      = []                     ,
                                             file_content   = b'content 1'           ,
                                             file_size      = 9                      ,
                                             files_affected = []                   ,
                                             error_details  = ''                     ,
                                             success        = True                   ,
                                             operation      = 'get'                  ,
                                             cache_id       = self.test_cache_id     )

    def test_perform_operation__add(self):                                                # Test adding file creates new cache entry
        with self.ops_service as _:
            request = Schema__Cache__Zip__Operation__Request(cache_id     = self.test_cache_id              ,
                                                             operation    = Enum__Cache__Zip__Operation.ADD ,
                                                             file_path    = "new_file.txt"                  ,
                                                             file_content = b"new content"                  ,
                                                             namespace    = self.test_namespace             )

            result = _.perform_operation(request)

            assert result.success           == True
            assert result.operation         == "add"
            assert result.cache_id          != self.test_cache_id                         # NEW cache ID!

            assert result.original_cache_id == self.test_cache_id                         # Original preserved
            assert result.files_affected    == ["new_file.txt"]
            assert "new cache_id:"          in result.message

            # Verify new cache entry has the added file
            list_request = Schema__Cache__Zip__Operation__Request(cache_id  = result.cache_id     ,                                         # Use NEW ID
                                                                  operation = "list"              ,
                                                                  namespace = self.test_namespace )
            list_result = _.perform_operation(list_request)
            assert "new_file.txt" in list_result.file_list
            assert len(list_result.file_list) == 4                                        # 3 original + 1 new
            assert list_result.obj()          == __(cache_id          = result.cache_id,
                                                    original_cache_id = None           ,
                                                    message           = 'Found 4 files in zip',
                                                    file_list         = ['dir/file3.txt', 'file1.txt', 'file2.txt', 'new_file.txt'],
                                                    file_content      = b''      ,
                                                    file_size         = 0        ,
                                                    files_affected    = []       ,
                                                    error_details     = ''       ,
                                                    success           = True     ,
                                                    operation         = 'list'   )
            # Verify original is unchanged
            original_list_request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,                                         # Original ID
                                                                           operation = "list"              ,
                                                                           namespace = self.test_namespace)
            original_result = _.perform_operation(original_list_request)
            assert "new_file.txt"                 not in original_result.file_list                        # Original unchanged
            assert len(original_result.file_list)     == 3
            assert original_result.obj()              == __(cache_id          = self.test_cache_id     ,
                                                            original_cache_id = None                   ,
                                                            message           = 'Found 3 files in zip' ,
                                                            file_list         = ['dir/file3.txt', 'file1.txt', 'file2.txt'],
                                                            file_content      = b''    ,
                                                            file_size         = 0      ,
                                                            files_affected    = []     ,
                                                            error_details     = ''     ,
                                                            success           = True    ,
                                                            operation         = 'list'  )

    def test_perform_operation__remove(self):                                             # Test removing file creates new cache entry
        with self.ops_service as _:
            request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
                                                             operation = "remove"            ,
                                                             file_path = "file2.txt"         ,
                                                             namespace = self.test_namespace )

            result = _.perform_operation(request)

            assert result.success           == True
            assert result.operation         == "remove"
            assert result.cache_id          != self.test_cache_id                         # NEW cache ID!
            assert result.original_cache_id == self.test_cache_id                         # Original preserved
            assert result.files_affected    == ["file2.txt"]
            assert "new cache_id:"          in result.message

            # Verify new cache entry has file removed
            list_request = Schema__Cache__Zip__Operation__Request(cache_id  = result.cache_id     ,        # Use NEW ID
                                                                  operation = "list"              ,
                                                                  namespace = self.test_namespace)
            list_result = _.perform_operation(list_request)
            assert "file2.txt" not in list_result.file_list
            assert len(list_result.file_list) == 2                                        # 3 original - 1 removed
            assert list_result.obj()          == __(cache_id          = result.cache_id         ,
                                                    original_cache_id = None                    ,
                                                    message           = 'Found 2 files in zip'  ,
                                                    file_list         = ['dir/file3.txt', 'file1.txt'],
                                                    file_content      = b''     ,
                                                    file_size         = 0       ,
                                                    files_affected    = []      ,
                                                    error_details     = ''      ,
                                                    success           = True    ,
                                                    operation         = 'list'  )

            # Verify original is unchanged
            original_result = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id ,
                                                                                         operation = "list"             ,
                                                                                         namespace = self.test_namespace))
            assert "file2.txt"           in original_result.file_list                               # Still in original
            assert original_result.obj() == __(cache_id          = self.test_cache_id     ,
                                               original_cache_id = None                   ,
                                               message           = 'Found 3 files in zip' ,
                                               file_list         = ['dir/file3.txt', 'file1.txt', 'file2.txt'],
                                               file_content      = b''    ,
                                               file_size         = 0      ,
                                               files_affected    = []     ,
                                               error_details     = ''      ,
                                               success           = True    ,
                                               operation         = 'list'  )

    def test_perform_operation__replace(self):                                            # Test replacing file creates new cache entry
        with self.ops_service as _:
            request = Schema__Cache__Zip__Operation__Request(cache_id     = self.test_cache_id    ,
                                                             operation    = "replace"             ,
                                                             file_path    = "file1.txt"           ,
                                                             file_content = b"replaced content"   ,
                                                             namespace    = self.test_namespace   )

            result = _.perform_operation(request)

            assert result.success           == True
            assert result.operation         == "replace"
            assert result.cache_id          != self.test_cache_id                         # NEW cache ID!
            assert result.original_cache_id == self.test_cache_id                         # Original preserved
            assert result.files_affected    == ["file1.txt"]
            assert "new cache_id:"          in result.message

            # Verify new cache entry has replaced content
            get_request = Schema__Cache__Zip__Operation__Request(cache_id  = result.cache_id     ,                          # Use NEW ID
                                                                 operation = "get"               ,
                                                                 file_path = "file1.txt"         ,
                                                                 namespace = self.test_namespace )
            get_result = _.perform_operation(get_request)
            assert get_result.file_content == b"replaced content"

            # Verify original is unchanged
            original_get = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id           ,
                                                                                      operation = "get", file_path="file1.txt" ,
                                                                                      namespace = self.test_namespace          ))
            assert original_get.file_content == b"content 1"                              # Original unchanged

    def test_immutability_chain(self):                                                    # Test chain of operations creates chain of immutable entries
        with self.ops_service as _:

            add_result = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id     = self.test_cache_id  ,    # Operation 1: Add file
                                                                                    operation    = "add"               ,
                                                                                    file_path    = "new.txt"           ,
                                                                                    file_content = b"new"              ,
                                                                                    namespace    = self.test_namespace))
            cache_id_v2 = add_result.cache_id

            remove_result = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = cache_id_v2        ,     # Operation 2: Remove file from v2
                                                                                       operation = "remove"           ,
                                                                                       file_path = "file2.txt"        ,
                                                                                       namespace = self.test_namespace))
            cache_id_v3 = remove_result.cache_id

            # Verify all three versions exist independently
            v1_files = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
                                                                                  operation = "list"              ,
                                                                                  namespace = self.test_namespace)).file_list
            v2_files = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = cache_id_v2,
                                                                                  operation = "list",
                                                                                  namespace = self.test_namespace)).file_list
            v3_files = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = cache_id_v3,
                                                                                  operation = "list",
                                                                                  namespace = self.test_namespace)).file_list
            assert type(v1_files) == type(v2_files) == type(v3_files) == Type_Safe__List

            # Each version has its expected state
            assert len(v1_files) == 3                                                     # Original
            assert "new.txt" not in v1_files

            assert len(v2_files) == 4                                                     # Added new.txt
            assert "new.txt" in v2_files
            assert "file2.txt" in v2_files

            assert len(v3_files) == 3                                                     # Removed file2.txt
            assert "new.txt" in v3_files
            assert "file2.txt" not in v3_files

    def test_create_modified_zip(self):                                                   # Test internal method for creating new cache entries
        with self.ops_service as _:
            modified_zip = zip_bytes__add_file(self.test_zip, "test.txt", b"test")

            new_cache_id = _.create_modified_zip(original_id = self.test_cache_id    ,
                                                 namespace   = self.test_namespace    ,
                                                 zip_bytes   = modified_zip           ,
                                                 operation   = "test_operation"       ,
                                                 details     = {"test": "details"}    )

            assert new_cache_id is not None
            assert new_cache_id != self.test_cache_id                                     # Always creates new ID

            # Verify new entry exists
            retrieved = _.retrieve_zip_bytes(new_cache_id, self.test_namespace)
            assert retrieved is not None
            files = zip_bytes__file_list(retrieved)
            assert "test.txt" in files

    def test_error_handling(self):                                                        # Test error conditions still work
        with self.ops_service as _:
            # Test missing file path for operations that need it
            for operation in ["get", "remove", "replace"]:
                result = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
                                                                                    operation = operation           ,
                                                                                    namespace = self.test_namespace))
                assert result.success       == False
                assert result.error_details == f"file_path required for {operation} operation"

            error_message = "Invalid value 'invalid' for enum Enum__Cache__Zip__Operation"
            with pytest.raises(ValueError, match=error_message):
                _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id ,
                                                                           operation = "invalid"          ,
                                                                           namespace = self.test_namespace))


            # Test non-existent cache ID
            result = _.perform_operation(Schema__Cache__Zip__Operation__Request(cache_id  = Random_Guid()      ,
                                                                                operation = "list"             ,
                                                                                namespace = self.test_namespace))
            assert result.success == False
            assert "not found"    in result.error_details