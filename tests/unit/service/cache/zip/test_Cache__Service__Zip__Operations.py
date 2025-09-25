# from unittest                                                                              import TestCase
# from osbot_utils.testing.__                                                                import __, __SKIP__
# from osbot_utils.utils.Objects                                                             import base_classes
# from osbot_utils.utils.Zip                                                                 import zip_bytes_empty, zip_bytes__add_file, zip_bytes__file_list
# from osbot_utils.type_safe.Type_Safe                                                       import Type_Safe
# from mgraph_ai_service_cache.service.cache.Cache__Service                                  import Cache__Service
# from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store                  import Cache__Service__Zip__Store
# from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Operations             import Cache__Service__Zip__Operations
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Request          import Schema__Cache__Zip__Store__Request
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Request      import Schema__Cache__Zip__Operation__Request
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Response     import Schema__Cache__Zip__Operation__Response
# import pytest
#
#
# class test_Cache__Service__Zip__Operations(TestCase):
#
#     @classmethod
#     def setUpClass(cls):
#         cls.cache_service = Cache__Service()                                                    # Create cache service once
#         cls.store_service = Cache__Service__Zip__Store     (cache_service=cls.cache_service)    # For storing test zips
#         cls.ops_service   = Cache__Service__Zip__Operations(cache_service=cls.cache_service)
#
#         # Create test zip files once
#         cls.test_zip = zip_bytes_empty()
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "file1.txt"    , b"content 1"     )
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "file2.txt"    , b"content 2"     )
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "dir/file3.txt", b"nested content")
#
#         cls.test_namespace = "test-ops"
#
#     def setUp(self):                                                                       # Per-test lightweight setup
#         # Store a fresh zip for each test
#         store_request = Schema__Cache__Zip__Store__Request(
#             zip_bytes = self.test_zip       ,
#             namespace = self.test_namespace
#         )
#         store_result = self.store_service.store_zip(store_request)
#         self.test_cache_id = store_result.cache_id
#
#     def test__init__(self):                                                               # Test service initialization
#         with Cache__Service__Zip__Operations() as _:
#             assert type(_)         is Cache__Service__Zip__Operations
#             assert base_classes(_) == [Type_Safe, object]
#             assert type(_.cache_service) is Cache__Service
#
#     def test_perform_operation__list(self):                                               # Test listing files in zip
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
#                                                              operation = "list"              ,
#                                                              namespace = self.test_namespace )
#
#             result = _.perform_operation(request)
#
#             assert type(result)          is Schema__Cache__Zip__Operation__Response
#
#             assert result.success        == True
#             assert result.operation      == "list"
#             assert result.cache_id       == self.test_cache_id
#             assert len(result.file_list) == 3                                             # Three files in test zip
#             assert "file1.txt"           in result.file_list
#             assert "file2.txt"           in result.file_list
#             assert "dir/file3.txt"       in result.file_list
#             assert result.message        == "Found 3 files in zip"
#             assert result.obj()          == __(message        = 'Found 3 files in zip',
#                                                file_list      = ['dir/file3.txt', 'file1.txt', 'file2.txt'],
#                                                file_content   = None              ,
#                                                file_size      = None              ,
#                                                files_affected = None              ,
#                                                error_details  = None              ,
#                                                success        = True              ,
#                                                operation      = 'list'            ,
#                                                cache_id       = self.test_cache_id)
#
#     def test_perform_operation__get(self):                                                # Test extracting file from zip
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
#                                                              operation = "get"               ,
#                                                              file_path = "file1.txt"         ,
#                                                              namespace = self.test_namespace )
#
#             result = _.perform_operation(request)
#             assert type(result)         is Schema__Cache__Zip__Operation__Response
#             assert result.success       == True
#             assert result.operation     == "get"
#             assert result.file_content  == b"content 1"
#             assert result.file_size     == 9                                                  # Length of "content 1"
#             assert result.message       == "Retrieved 'file1.txt'"
#             assert request.obj()        == __(file_path    = 'file1.txt',
#                                               file_content = __SKIP__          ,        # it is binary (not supported by .obj())
#                                               namespace    = 'test-ops'        ,
#                                               cache_id     = self.test_cache_id,
#                                               operation    = 'get'             )
#
#     def test_perform_operation__get__nested_file(self):                                   # Test getting nested file
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
#                                                              operation = "get"               ,
#                                                              file_path = "dir/file3.txt"     ,
#                                                              namespace = self.test_namespace )
#
#             result = _.perform_operation(request)
#
#             assert result.success      == True
#             assert result.file_content == b"nested content"
#             assert result.file_size    == 14                                              # Length of "nested content"
#
#     def test_perform_operation__get__missing_file(self):                                  # Test getting non-existent file
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(cache_id  = self.test_cache_id  ,
#                                                              operation = "get"               ,
#                                                              file_path = "nonexistent.txt"   ,
#                                                              namespace = self.test_namespace )
#
#             result = _.perform_operation(request)
#
#             assert result.success       == False
#             assert result.operation     == "get"
#             assert result.error_details == "File 'nonexistent.txt' not found in zip"
#             assert result.file_content is None
#
#     def test_perform_operation__get__no_path(self):                                       # Test get without file path
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "get"               ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == False
#             assert result.error_details == "file_path required for get operation"
#
#     def test_perform_operation__add(self):                                                # Test adding file to zip
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(cache_id     = self.test_cache_id  ,
#                                                              operation    = "add"               ,
#                                                              file_path    = "new_file.txt"      ,
#                                                              file_content = b"new content"      ,
#                                                              namespace    = self.test_namespace )
#
#             result = _.perform_operation(request)
#
#             assert result.success == True
#             assert result.operation == "add"
#             assert result.files_affected == ["new_file.txt"]
#             assert result.message == "Added 'new_file.txt' to zip"
#
#             # Verify file was actually added
#             list_request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "list"              ,
#                 namespace = self.test_namespace
#             )
#             list_result = _.perform_operation(list_request)
#             assert "new_file.txt" in list_result.file_list
#             assert len(list_result.file_list) == 4                                        # 3 original + 1 new
#
#     def test_perform_operation__add__missing_content(self):                               # Test add without content
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "add"               ,
#                 file_path = "new_file.txt"      ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == False
#             assert result.error_details == "file_path and file_content required for add"
#
#     def test_perform_operation__remove(self):                                             # Test removing file from zip
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "remove"            ,
#                 file_path = "file2.txt"         ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == True
#             assert result.operation == "remove"
#             assert result.files_affected == ["file2.txt"]
#             assert result.message == "Removed 'file2.txt' from zip"
#
#             # Verify file was removed
#             list_request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "list"              ,
#                 namespace = self.test_namespace
#             )
#             list_result = _.perform_operation(list_request)
#             assert "file2.txt" not in list_result.file_list
#             assert len(list_result.file_list) == 2                                        # 3 original - 1 removed
#
#     def test_perform_operation__remove__no_path(self):                                    # Test remove without path
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "remove"            ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == False
#             assert result.error_details == "file_path required for remove operation"
#
#     def test_perform_operation__replace(self):                                            # Test replacing file in zip
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id     = self.test_cache_id    ,
#                 operation    = "replace"             ,
#                 file_path    = "file1.txt"           ,
#                 file_content = b"replaced content"   ,
#                 namespace    = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == True
#             assert result.operation == "replace"
#             assert result.files_affected == ["file1.txt"]
#             assert result.message == "Replaced 'file1.txt' in zip"
#
#             # Verify content was replaced
#             get_request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "get"               ,
#                 file_path = "file1.txt"         ,
#                 namespace = self.test_namespace
#             )
#             get_result = _.perform_operation(get_request)
#             assert get_result.file_content == b"replaced content"
#
#     def test_perform_operation__replace__missing_content(self):                           # Test replace without content
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "replace"           ,
#                 file_path = "file1.txt"         ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == False
#             assert result.error_details == "file_path and file_content required for replace"
#
#     def test_perform_operation__invalid_operation(self):                                  # Test unknown operation
#         with self.ops_service as _:
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = self.test_cache_id  ,
#                 operation = "invalid"           ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == False
#             assert result.operation == "invalid"
#             assert "Unknown operation: invalid" in result.error_details
#
#     def test_perform_operation__nonexistent_zip(self):                                    # Test operation on missing zip
#         with self.ops_service as _:
#             from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid import Random_Guid
#
#             request = Schema__Cache__Zip__Operation__Request(
#                 cache_id  = Random_Guid()       ,                                         # Non-existent ID
#                 operation = "list"              ,
#                 namespace = self.test_namespace
#             )
#
#             result = _.perform_operation(request)
#
#             assert result.success == False
#             assert result.error_details == "Zip file not found in cache"
#
#     def test__retrieve_zip_bytes(self):                                                   # Test internal zip retrieval
#         with self.ops_service as _:
#             # Test successful retrieval
#             zip_bytes = _._retrieve_zip_bytes(self.test_cache_id, self.test_namespace)
#             assert zip_bytes is not None
#             assert type(zip_bytes) is bytes
#
#             # Verify it's the correct zip
#             files = zip_bytes__file_list(zip_bytes)
#             assert len(files) == 3
#             assert "file1.txt" in files
#
#             # Test retrieval of non-existent
#             from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid import Random_Guid
#             missing = _._retrieve_zip_bytes(Random_Guid(), self.test_namespace)
#             assert missing is None
#
#     def test__update_zip(self):                                                           # Test internal zip update
#         with self.ops_service as _:
#             # Create modified zip
#             modified_zip = zip_bytes__add_file(self.test_zip, "added.txt", b"added content")
#
#             # Update the stored zip
#             success = _.update_zip(self.test_cache_id, self.test_namespace, modified_zip)
#             assert success == True
#
#             # Verify update worked
#             retrieved = _._retrieve_zip_bytes(self.test_cache_id, self.test_namespace)
#             files = zip_bytes__file_list(retrieved)
#             assert "added.txt" in files
#             assert len(files) == 4                                                        # 3 original + 1 added
#
#     def test_perform_operation__comprehensive_workflow(self):                             # Test full workflow
#         with self.ops_service as _:
#             # List initial files
#             list_result = _.perform_operation(Schema__Cache__Zip__Operation__Request(
#                 cache_id=self.test_cache_id, operation="list", namespace=self.test_namespace
#             ))
#             initial_count = len(list_result.file_list)
#
#             # Add a file
#             _.perform_operation(Schema__Cache__Zip__Operation__Request(
#                 cache_id=self.test_cache_id, operation="add", file_path="test.txt",
#                 file_content=b"test", namespace=self.test_namespace
#             ))
#
#             # Replace a file
#             _.perform_operation(Schema__Cache__Zip__Operation__Request(
#                 cache_id=self.test_cache_id, operation="replace", file_path="file1.txt",
#                 file_content=b"modified", namespace=self.test_namespace
#             ))
#
#             # Remove a file
#             _.perform_operation(Schema__Cache__Zip__Operation__Request(
#                 cache_id=self.test_cache_id, operation="remove", file_path="file2.txt",
#                 namespace=self.test_namespace
#             ))
#
#             # List final files
#             final_result = _.perform_operation(Schema__Cache__Zip__Operation__Request(
#                 cache_id=self.test_cache_id, operation="list", namespace=self.test_namespace
#             ))
#
#             # Verify all operations worked
#             assert len(final_result.file_list) == initial_count                           # +1 added, -1 removed
#             assert "test.txt" in final_result.file_list                                   # Added file exists
#             assert "file2.txt" not in final_result.file_list                              # Removed file gone
#
#             # Verify replacement worked
#             get_result = _.perform_operation(Schema__Cache__Zip__Operation__Request(
#                 cache_id=self.test_cache_id, operation="get", file_path="file1.txt",
#                 namespace=self.test_namespace
#             ))
#             assert get_result.file_content == b"modified"                                 # Content replaced