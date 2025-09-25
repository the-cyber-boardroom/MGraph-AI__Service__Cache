# from typing                                                                              import List, Tuple
# from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
# from osbot_utils.type_safe.type_safe_core.decorators.type_safe                           import type_safe
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
# from osbot_utils.utils.Zip                                                               import (zip_bytes__file_list, zip_bytes__add_file,
#                                                                                              zip_bytes__remove_files, zip_bytes__replace_file)
# from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Request        import Schema__Cache__Zip__Batch__Request, Schema__Zip__Batch__Operation
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Response       import Schema__Cache__Zip__Batch__Response, Schema__Zip__Operation__Result
#
#
# class Cache__Service__Zip__Batch(Type_Safe):                                             # Service layer for batch zip operations
#     cache_service : Cache__Service                                                       # Underlying cache service
#
#     @type_safe
#     def perform_batch(self, request: Schema__Cache__Zip__Batch__Request
#                      ) -> Schema__Cache__Zip__Batch__Response:                           # Execute batch operations on zip
#
#         original_zip = self._retrieve_zip(request.cache_id, request.namespace)           # Get original zip
#         if not original_zip:
#             return Schema__Cache__Zip__Batch__Response(
#                 success        = False                                ,
#                 cache_id       = request.cache_id                     ,
#                 error_message  = "Zip file not found in cache"
#             )
#
#         backup_id = None
#         if request.create_backup:                                                        # Create backup if requested
#             backup_id = self._create_backup(original_zip, request.namespace)
#
#         working_zip       = original_zip                                                 # Start with original
#         results           = []                                                           # Track individual results
#         files_added       = []
#         files_removed     = []
#         files_modified    = []
#         operations_applied = 0
#         operations_failed  = 0
#
#         for operation in request.operations:                                             # Process each operation
#             try:
#                 success, modified_zip, affected_files = self._apply_operation(
#                     working_zip, operation
#                 )
#
#                 if success:
#                     working_zip = modified_zip                                           # Update working copy
#                     operations_applied += 1
#
#                     if operation.action == "add":                                        # Track changes
#                         files_added.extend(affected_files)
#                     elif operation.action == "remove":
#                         files_removed.extend(affected_files)
#                     elif operation.action in ["replace", "rename", "move"]:
#                         files_modified.extend(affected_files)
#
#                     results.append(Schema__Zip__Operation__Result(
#                         action  = operation.action      ,
#                         path    = operation.path        ,
#                         success = True
#                     ))
#                 else:
#                     operations_failed += 1
#                     results.append(Schema__Zip__Operation__Result(
#                         action  = operation.action                          ,
#                         path    = operation.path                            ,
#                         success = False                                      ,
#                         error   = "Operation failed"
#                     ))
#
#                     if request.atomic:                                                   # Atomic mode - stop on failure
#                         return self._rollback_batch(
#                             request, backup_id, operations_applied, operations_failed,
#                             results, "Atomic batch failed"
#                         )
#
#             except Exception as e:
#                 operations_failed += 1
#                 results.append(Schema__Zip__Operation__Result(
#                     action  = operation.action  ,
#                     path    = operation.path    ,
#                     success = False             ,
#                     error   = str(e)
#                 ))
#
#                 if request.atomic:
#                     return self._rollback_batch(
#                         request, backup_id, operations_applied, operations_failed,
#                         results, f"Atomic batch failed: {str(e)}"
#                     )
#
#         new_cache_id = None
#         if operations_applied > 0:                                                       # Save modified zip
#             new_cache_id = self._save_modified_zip(
#                 working_zip, request.cache_id, request.namespace, request.strategy
#             )
#
#         final_file_list = zip_bytes__file_list(working_zip)                              # Get final stats
#
#         return Schema__Cache__Zip__Batch__Response(
#             success              = operations_failed == 0       ,
#             cache_id             = request.cache_id            ,
#             new_cache_id         = new_cache_id                ,
#             backup_cache_id      = backup_id                   ,
#             operations_applied   = operations_applied          ,
#             operations_failed    = operations_failed           ,
#             operation_results    = results                     ,
#             files_added          = files_added                 ,
#             files_removed        = files_removed               ,
#             files_modified       = files_modified              ,
#             new_file_count       = len(final_file_list)        ,
#             new_size             = len(working_zip)            ,
#             rollback_performed   = False
#         )
#
#     def _retrieve_zip(self, cache_id: Random_Guid, namespace: str) -> bytes:             # Get zip from cache
#         result = self.cache_service.retrieve_by_id(cache_id, namespace)
#         if result and result.get('data_type') == 'binary':
#             return result.get('data')
#         return None
#
#     def _create_backup(self, zip_bytes: bytes, namespace: str) -> Random_Guid:           # Create backup of zip
#         backup_id  = Random_Guid()
#         cache_hash = self.cache_service.hash_from_bytes(zip_bytes)
#
#         self.cache_service.store_with_strategy(
#             storage_data = zip_bytes         ,
#             cache_hash   = cache_hash       ,
#             cache_id     = backup_id        ,
#             namespace    = namespace         ,
#             strategy     = "direct"             # Backups are direct storage
#         )
#         return backup_id
#
#     # tod: refactor each operation into it's own separate method
#     def _apply_operation(self, zip_bytes: bytes, operation: Schema__Zip__Batch__Operation
#                         ) -> Tuple[bool, bytes, List[str]]:                              # Apply single operation
#
#         if operation.condition != "always":                                              # Check conditions
#             file_list = zip_bytes__file_list(zip_bytes)
#             file_exists = str(operation.path) in file_list
#
#             if operation.condition == "if_exists" and not file_exists:
#                 return True, zip_bytes, []                                               # Skip - condition not met
#             if operation.condition == "if_not_exists" and file_exists:
#                 return True, zip_bytes, []                                               # Skip - condition not met
#
#         if operation.action == "add":                                                    # Execute operation
#             if not operation.content:
#                 raise ValueError(f"Content required for add operation on {operation.path}")
#             new_zip = zip_bytes__add_file(zip_bytes, str(operation.path), operation.content)
#             return True, new_zip, [operation.path]
#
#         elif operation.action == "remove":
#             if operation.pattern:                                                        # Pattern-based removal
#                 files_to_remove = self._match_pattern(zip_bytes, operation.pattern)
#                 new_zip = zip_bytes__remove_files(zip_bytes, files_to_remove)
#                 return True, new_zip, files_to_remove
#             else:
#                 new_zip = zip_bytes__remove_files(zip_bytes, [str(operation.path)])
#                 return True, new_zip, [operation.path]
#
#         elif operation.action == "replace":
#             if not operation.content:
#                 raise ValueError(f"Content required for replace operation on {operation.path}")
#             new_zip = zip_bytes__replace_file(zip_bytes, str(operation.path), operation.content)
#             return True, new_zip, [operation.path]
#
#         elif operation.action in ["rename", "move"]:
#             if not operation.new_path:
#                 raise ValueError(f"new_path required for {operation.action} operation")
#             # Extract, remove old, add with new name
#             import io
#             import zipfile
#             with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zf:
#                 content = zf.read(str(operation.path))
#             new_zip = zip_bytes__remove_files(zip_bytes, [str(operation.path)])
#             new_zip = zip_bytes__add_file(new_zip, str(operation.new_path), content)
#             return True, new_zip, [operation.path, operation.new_path]
#
#         else:
#             raise ValueError(f"Unknown action: {operation.action}")
#
#     def _match_pattern(self, zip_bytes: bytes, pattern: str) -> List[str]:               # Match files by pattern
#         import fnmatch
#         file_list = zip_bytes__file_list(zip_bytes)
#         return [f for f in file_list if fnmatch.fnmatch(f, str(pattern))]
#
#     def _save_modified_zip(self, zip_bytes: bytes, original_id: Random_Guid,
#                            namespace: str, strategy: str) -> Random_Guid:                # Save modified zip
#
#         cache_hash = self.cache_service.hash_from_bytes(zip_bytes)
#
#         if strategy == "direct":                                                         # Reuse original ID for direct
#             cache_id = original_id
#         else:
#             cache_id = Random_Guid()                                                     # New ID for versioned
#
#         result = self.cache_service.store_with_strategy(
#             storage_data = zip_bytes         ,
#             cache_hash   = cache_hash       ,
#             cache_id     = cache_id         ,
#             namespace    = namespace         ,
#             strategy     = strategy
#         )
#
#         return result.cache_id if result else None
#
#     def _rollback_batch(self, request, backup_id, ops_applied, ops_failed,
#                        results, error_msg) -> Schema__Cache__Zip__Batch__Response:       # Build rollback response
#         return Schema__Cache__Zip__Batch__Response(
#             success              = False                    ,
#             cache_id             = request.cache_id        ,
#             backup_cache_id      = backup_id               ,
#             operations_applied   = ops_applied             ,
#             operations_failed    = ops_failed              ,
#             operation_results    = results                 ,
#             rollback_performed   = True                    ,
#             error_message        = error_msg
#         )