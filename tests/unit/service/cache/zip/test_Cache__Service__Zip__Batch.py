# from unittest                                                                         import TestCase
# from osbot_utils.testing.__                                                           import __, __SKIP__
# from osbot_utils.utils.Zip                                                            import zip_bytes_empty, zip_bytes__add_file, zip_bytes__file_list
# from mgraph_ai_service_cache.service.cache.Cache__Service                             import Cache__Service
# from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store             import Cache__Service__Zip__Store
# from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Batch             import Cache__Service__Zip__Batch
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Request     import Schema__Cache__Zip__Store__Request
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Request     import Schema__Cache__Zip__Batch__Request, Schema__Zip__Batch__Operation
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Response    import Schema__Cache__Zip__Batch__Response
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                 import Random_Guid
#
#
# class test_Cache__Service__Zip__Batch(TestCase):
#
#     @classmethod
#     def setUpClass(cls):                                                                # One-time expensive setup
#         cls.cache_service = Cache__Service()
#         cls.store_service = Cache__Service__Zip__Store(cache_service=cls.cache_service)
#         cls.batch_service = Cache__Service__Zip__Batch(cache_service=cls.cache_service)
#
#         # Create and store a test zip for batch operations
#         cls.test_zip = zip_bytes_empty()
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "file1.txt", b"content 1")
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "file2.txt", b"content 2")
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "temp.tmp", b"temp file")
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "logs/app.log", b"log data")
#
#         cls.test_namespace = "test-batch"
#
#     def setUp(self):                                                                   # Per-test setup - store fresh zip
#         store_request = Schema__Cache__Zip__Store__Request(
#             zip_bytes = self.test_zip      ,
#             namespace = self.test_namespace
#         )
#         store_result = self.store_service.store_zip(store_request)
#         self.test_cache_id = store_result.cache_id
#
#     def test__init__(self):                                                           # Test service initialization
#         with Cache__Service__Zip__Batch() as _:
#             assert type(_.cache_service) is Cache__Service
#
#     def test_perform_batch__single_add(self):                                         # Test adding single file
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action  = "add"                ,
#                     path    = "new_file.txt"       ,
#                     content = b"new content"
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = operations         ,
#                 namespace  = self.test_namespace
#             )
#
#             result = _.perform_batch(request)
#
#             assert type(result) is Schema__Cache__Zip__Batch__Response
#             assert result.success == True
#             assert result.operations_applied == 1
#             assert result.operations_failed == 0
#             assert "new_file.txt" in result.files_added
#             assert result.new_file_count == 5                                         # 4 original + 1 new
#
#     def test_perform_batch__multiple_operations(self):                                # Test mixed operations
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(                                        # Add file
#                     action  = "add"         ,
#                     path    = "added.txt"   ,
#                     content = b"added"
#                 ),
#                 Schema__Zip__Batch__Operation(                                        # Remove file
#                     action = "remove"      ,
#                     path   = "temp.tmp"
#                 ),
#                 Schema__Zip__Batch__Operation(                                        # Replace file
#                     action  = "replace"           ,
#                     path    = "file1.txt"         ,
#                     content = b"replaced content"
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = operations         ,
#                 namespace  = self.test_namespace,
#                 atomic     = False                                                    # Allow partial success
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert result.operations_applied == 3
#             assert "added.txt" in result.files_added
#             assert "temp.tmp" in result.files_removed
#             assert "file1.txt" in result.files_modified
#             assert result.new_file_count == 4                                         # 4 + 1 - 1 = 4
#
#     def test_perform_batch__atomic_failure(self):                                     # Test atomic rollback on failure
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action  = "add"         ,
#                     path    = "good.txt"    ,
#                     content = b"good"
#                 ),
#                 Schema__Zip__Batch__Operation(                                        # This will fail
#                     action = "remove"             ,
#                     path   = "nonexistent.txt"                                        # File doesn't exist
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = operations         ,
#                 namespace  = self.test_namespace,
#                 atomic     = True                                                     # Atomic mode
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == False
#             assert result.rollback_performed == True
#             assert result.operations_applied == 1                                     # First succeeded
#             assert result.operations_failed == 1                                      # Second failed
#
#             # Verify zip unchanged (rollback worked)
#             retrieved = _.cache_service.retrieve_by_id(self.test_cache_id, self.test_namespace)
#             files = zip_bytes__file_list(retrieved['data'])
#             assert "good.txt" not in files                                            # Rolled back
#
#     def test_perform_batch__conditional_operations(self):                             # Test conditional execution
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action    = "add"              ,
#                     path      = "file1.txt"        ,                                  # Already exists
#                     content   = b"new content"     ,
#                     condition = "if_not_exists"                                       # Skip if exists
#                 ),
#                 Schema__Zip__Batch__Operation(
#                     action    = "remove"           ,
#                     path      = "temp.tmp"         ,                                  # Does exist
#                     condition = "if_exists"                                           # Only remove if exists
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = operations         ,
#                 namespace  = self.test_namespace
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert result.operations_applied == 2                                     # Both "succeeded"
#             assert len(result.files_added) == 0                                       # First was skipped
#             assert "temp.tmp" in result.files_removed                                 # Second executed
#
#     def test_perform_batch__pattern_removal(self):                                    # Test pattern-based operations
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action  = "remove"     ,
#                     pattern = "*.tmp"                                                  # Remove all .tmp files
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = operations         ,
#                 namespace  = self.test_namespace
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert "temp.tmp" in result.files_removed
#             assert result.new_file_count == 3                                         # One .tmp file removed
#
#     def test_perform_batch__rename_operation(self):                                   # Test file renaming
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action   = "rename"         ,
#                     path     = "file1.txt"      ,
#                     new_path = "renamed.txt"
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = operations         ,
#                 namespace  = self.test_namespace
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert "file1.txt" in result.files_modified
#             assert "renamed.txt" in result.files_modified
#
#             # Verify rename worked
#             retrieved = _.cache_service.retrieve_by_id(result.new_cache_id or self.test_cache_id,
#                                                       self.test_namespace)
#             files = zip_bytes__file_list(retrieved['data'])
#             assert "renamed.txt" in files
#             assert "file1.txt" not in files
#
#     def test_perform_batch__with_backup(self):                                        # Test backup creation
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action  = "remove"     ,
#                     path    = "file1.txt"
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id      = self.test_cache_id,
#                 operations    = operations         ,
#                 namespace     = self.test_namespace,
#                 create_backup = True                                                   # Request backup
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert result.backup_cache_id is not None                                 # Backup created
#
#             # Verify backup exists and has original content
#             backup = _.cache_service.retrieve_by_id(result.backup_cache_id, self.test_namespace)
#             assert backup is not None
#             backup_files = zip_bytes__file_list(backup['data'])
#             assert "file1.txt" in backup_files                                        # Original preserved
#
#     def test_perform_batch__versioning_strategy(self):                                # Test versioned storage
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action  = "add"         ,
#                     path    = "version.txt" ,
#                     content = b"v1"
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id               ,
#                 operations = operations                       ,
#                 namespace  = self.test_namespace               ,
#                 strategy   = "temporal_versioned"                                     # Create new version
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert result.new_cache_id is not None                                    # New ID generated
#             assert result.new_cache_id != self.test_cache_id                          # Different from original
#
#             # Original should be unchanged
#             original = _.cache_service.retrieve_by_id(self.test_cache_id, self.test_namespace)
#             original_files = zip_bytes__file_list(original['data'])
#             assert "version.txt" not in original_files
#
#             # New version should have changes
#             new_version = _.cache_service.retrieve_by_id(result.new_cache_id, self.test_namespace)
#             new_files = zip_bytes__file_list(new_version['data'])
#             assert "version.txt" in new_files
#
#     def test_perform_batch__empty_operations(self):                                   # Test with no operations
#         with self.batch_service as _:
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = self.test_cache_id,
#                 operations = []                 ,                                     # Empty list
#                 namespace  = self.test_namespace
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == True
#             assert result.operations_applied == 0
#             assert result.operations_failed == 0
#             assert result.new_cache_id is None                                        # No changes, no new ID
#
#     def test_perform_batch__invalid_cache_id(self):                                   # Test with non-existent zip
#         with self.batch_service as _:
#             operations = [
#                 Schema__Zip__Batch__Operation(
#                     action  = "add"         ,
#                     path    = "test.txt"    ,
#                     content = b"test"
#                 )
#             ]
#
#             request = Schema__Cache__Zip__Batch__Request(
#                 cache_id   = Random_Guid()     ,                                      # Random ID that doesn't exist
#                 operations = operations         ,
#                 namespace  = self.test_namespace
#             )
#
#             result = _.perform_batch(request)
#
#             assert result.success == False
#             assert result.error_message == "Zip file not found in cache"