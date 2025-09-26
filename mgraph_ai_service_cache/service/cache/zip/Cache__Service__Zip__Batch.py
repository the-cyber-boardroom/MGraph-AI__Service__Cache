import fnmatch
import io
import zipfile
from typing                                                                              import List, Tuple, Optional, Any
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                           import type_safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.utils.Zip                                                               import (zip_bytes__file_list, zip_bytes__add_file,
                                                                                                 zip_bytes__remove_files, zip_bytes__replace_file)
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Batch__Request        import Schema__Cache__Zip__Batch__Request, Schema__Zip__Batch__Operation
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Batch__Response       import Schema__Cache__Zip__Batch__Response, Schema__Zip__Operation__Result


class Cache__Service__Zip__Batch(Type_Safe):                                             # Service layer for batch zip operations
    cache_service : Cache__Service                                                       # Underlying cache service

    @type_safe
    def perform_batch(self, request: Schema__Cache__Zip__Batch__Request
                      ) -> Any:
                      # ) -> Schema__Cache__Zip__Batch__Response:                            # Execute batch operations on zip

        original_zip = self.retrieve_zip(request.cache_id, request.namespace)               # Get original zip

        if not original_zip:
            return Schema__Cache__Zip__Batch__Response(success              = False                         ,
                                                       cache_id             = request.cache_id              ,
                                                       original_cache_id    = request.cache_id              ,
                                                       error_message        = "Zip file not found in cache" )

        working_zip        = original_zip                                                # Start with original
        results            = []                                                          # Track individual results
        files_added        = []
        files_removed      = []
        files_modified     = []
        operations_applied = 0
        operations_failed  = 0

        for operation in request.operations:                                             # Process each operation
            try:
                success, modified_zip, affected_files = self.apply_operation(working_zip, operation)        # todo: replace the return value with a Type_Safe class

                if success:
                    working_zip = modified_zip                                           # Update working copy
                    operations_applied += 1

                    if operation.action == "add":                                        # Track changes
                        files_added.extend(affected_files)
                    elif operation.action == "remove":
                        files_removed.extend(affected_files)
                    elif operation.action in ["replace", "rename", "move"]:
                        files_modified.extend(affected_files)

                    results.append(Schema__Zip__Operation__Result(action  = operation.action      ,
                                                                  path    = operation.path        ,
                                                                  success = True                  ))
                else:
                    operations_failed += 1
                    results.append(Schema__Zip__Operation__Result(action  = operation.action     ,
                                                                  path    = operation.path       ,
                                                                  success = False                ,
                                                                  error   = "Operation failed"  ))

                    if request.atomic:                                                   # Atomic mode - stop on failure
                        return self.rollback_batch(request                  ,
                                                   operations_applied       ,
                                                   operations_failed,results,
                                                   "Atomic batch failed"    )

            except Exception as e:
                operations_failed += 1
                results.append(Schema__Zip__Operation__Result(action  = operation.action  ,
                                                              path    = operation.path    ,
                                                              success = False             ,
                                                              error   = str(e)            ))

                if request.atomic:
                    return self.rollback_batch(request, operations_applied, operations_failed,
                                               results, f"Atomic batch failed: {str(e)}")

        # Create single new cache entry for all successful operations
        new_cache_id = None
        if operations_applied > 0:
            # metadata = { "parent_id"         : str(request.cache_id)       ,
            #              "operations_count"  : operations_applied          ,
            #              "batch_operation"   : True                        ,
            #              "operations"        : [r.obj() for r in results if r.success]}
            new_cache_id = self.create_modified_zip(original_id = request.cache_id      ,
                                                    namespace   = request.namespace     ,
                                                    zip_bytes   = working_zip           ,
                                                    #metadata    = metadata                         # todo: fix, clashing with Schema__Cache__Store__Metadata
                                                    )

        final_file_list = zip_bytes__file_list(working_zip)                              # Get final stats

        return Schema__Cache__Zip__Batch__Response(success              = operations_failed == 0                   ,
                                                   cache_id             = new_cache_id or request.cache_id        ,  # New ID if changes made
                                                   original_cache_id    = request.cache_id                        ,  # Always preserve original
                                                   operations_applied   = operations_applied                      ,
                                                   operations_failed    = operations_failed                       ,
                                                   operation_results    = results                                 ,
                                                   files_added          = files_added                             ,
                                                   files_removed        = files_removed                           ,
                                                   files_modified       = files_modified                          ,
                                                   new_file_count       = len(final_file_list)                    ,
                                                   new_size             = len(working_zip)                        ,
                                                   rollback_performed   = False                                   )

    def retrieve_zip(self, cache_id: Random_Guid, namespace: str) -> Optional[bytes]:    # Get zip from cache
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result and result.get('data_type') == 'binary':
            return result.get('data')
        return None

    def create_modified_zip(self, original_id : Random_Guid ,
                                  namespace   : str          ,
                                  zip_bytes   : bytes        ,
                                  metadata    : dict = None
                           ) -> Optional[Random_Guid]:                                   # Create new immutable cache entry
        cache_hash = self.cache_service.hash_from_bytes(zip_bytes)

        original_entry    = self.cache_service.retrieve_by_id(original_id, namespace)               # Get original entry to preserve strategy
        original_strategy = original_entry.get('strategy', 'direct') if original_entry else 'direct'

        # Store as new immutable entry
        result = self.cache_service.store_with_strategy(storage_data = zip_bytes        ,
                                                        cache_hash   = cache_hash       ,
                                                        cache_id     = Random_Guid()    ,  # Always new ID for immutability
                                                        namespace    = namespace        ,
                                                        strategy     = original_strategy,  # Preserve original's strategy
                                                        metadata     = metadata         )   # Include batch metadata

        return result.cache_id if result else None

    def apply_operation(self, zip_bytes: bytes,
                              operation: Schema__Zip__Batch__Operation
                         ) -> Tuple[bool, bytes, List[str]]:                             # Apply single operation

        if operation.condition != "always":                                              # Check conditions
            file_list   = zip_bytes__file_list(zip_bytes)
            file_exists = str(operation.path) in file_list

            if operation.condition == "if_exists" and not file_exists:
                return True, zip_bytes, []                                               # Skip - condition not met
            if operation.condition == "if_not_exists" and file_exists:
                return True, zip_bytes, []                                               # Skip - condition not met

        if operation.action == "add":                                                    # Execute operation
            if not operation.content:
                raise ValueError(f"Content required for add operation on {operation.path}")
            new_zip = zip_bytes__add_file(zip_bytes, str(operation.path), operation.content)
            return True, new_zip, [operation.path]

        elif operation.action == "remove":
            if operation.pattern:                                                        # Pattern-based removal
                files_to_remove = self.match_pattern(zip_bytes, operation.pattern)
                new_zip = zip_bytes__remove_files(zip_bytes, files_to_remove)
                success = zip_bytes != new_zip                                           # was any file removed
                return success, new_zip, files_to_remove
            else:
                new_zip = zip_bytes__remove_files(zip_bytes, [str(operation.path)])
                success = zip_bytes != new_zip                                           # was any file removed
                return success, new_zip, [operation.path]

        elif operation.action == "replace":
            if not operation.content:
                raise ValueError(f"Content required for replace operation on {operation.path}")
            new_zip = zip_bytes__replace_file(zip_bytes, str(operation.path), operation.content)
            return True, new_zip, [operation.path]

        elif operation.action in ["rename", "move"]:
            if not operation.new_path:
                raise ValueError(f"new_path required for {operation.action} operation")
            # Extract, remove old, add with new name

            with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zf:                         # todo: we should be using OSBot-Utils here
                content = zf.read(str(operation.path))
            new_zip = zip_bytes__remove_files(zip_bytes, [str(operation.path)])
            new_zip = zip_bytes__add_file(new_zip, str(operation.new_path), content)
            return True, new_zip, [operation.path, operation.new_path]

        else:
            raise ValueError(f"Unknown action: {operation.action}")

    def match_pattern(self, zip_bytes: bytes, pattern: str) -> List[str]:                # Match files by pattern
        file_list = zip_bytes__file_list(zip_bytes)
        return [f for f in file_list if fnmatch.fnmatch(f, str(pattern))]

    def rollback_batch(self, request, ops_applied, ops_failed,
                      results, error_msg) -> Schema__Cache__Zip__Batch__Response:        # Build rollback response
        return Schema__Cache__Zip__Batch__Response(success              = False                    ,
                                                   cache_id             = request.cache_id        ,  # Original unchanged on rollback
                                                   original_cache_id    = request.cache_id        ,
                                                   operations_applied   = ops_applied             ,
                                                   operations_failed    = ops_failed              ,
                                                   operation_results    = results                 ,
                                                   rollback_performed   = True                    ,
                                                   error_message        = error_msg               )