# from typing                                                                              import Optional, List
#
# from osbot_utils.decorators.methods.cache_on_self import cache_on_self
# from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
# from osbot_utils.type_safe.type_safe_core.decorators.type_safe                           import type_safe
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
# from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
# from osbot_utils.utils.Json                                                              import json_to_bytes
# from osbot_utils.utils.Misc                                                              import timestamp_now
# from osbot_utils.utils.Zip                                                               import (zip_bytes__file_list, zip_bytes__file,
#                                                                                                  zip_bytes__add_file, zip_bytes__remove_file,
#                                                                                                  zip_bytes__replace_file)
# from mgraph_ai_service_cache.schemas.cache.zip.enums.Enum__Cache__Zip__Operation         import Enum__Cache__Zip__Operation
# from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Request    import Schema__Cache__Zip__Operation__Request
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Response   import Schema__Cache__Zip__Operation__Response
#
#
# class Cache__Service__Zip__Operations(Type_Safe):                                        # Service layer for zip file operations
#     cache_service : Cache__Service                                                       # Underlying cache service
#
#     @cache_on_self
#     def operation_handlers(self):
#         return { Enum__Cache__Zip__Operation.LIST    : self.operation__list    ,
#                  Enum__Cache__Zip__Operation.GET     : self.operation__get     ,
#                  Enum__Cache__Zip__Operation.ADD     : self.operation__add     ,
#                  Enum__Cache__Zip__Operation.REMOVE  : self.operation__remove  ,
#                  Enum__Cache__Zip__Operation.REPLACE : self.operation__replace ,
#                  Enum__Cache__Zip__Operation.MOVE    : self.operation__move    }
#     @type_safe
#     def perform_operation(self, request: Schema__Cache__Zip__Operation__Request
#                            ) -> Schema__Cache__Zip__Operation__Response:                   # Route operation to appropriate handler
#
#         # Map operations to their handler methods
#
#         try:
#             handler = self.operation_handlers().get(request.operation)
#             if not handler:
#                 return Schema__Cache__Zip__Operation__Response(success       = False                             ,
#                                                                operation     = request.operation                 ,
#                                                                cache_id      = request.cache_id                  ,
#                                                                error_details = f"Unknown operation: {request.operation}")
#
#             response = handler(request)
#             return response
#         except Exception as e:
#             return self.error_response(request, str(e))
#
#     @type_safe
#     def operation__list(self, request: Schema__Cache__Zip__Operation__Request
#                          ) -> Schema__Cache__Zip__Operation__Response:                     # List all files in zip
#
#         zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
#         if not zip_bytes:
#             return self.error_response(request, "Zip file not found in cache")
#
#
#         file_list = zip_bytes__file_list(zip_bytes)
#         return Schema__Cache__Zip__Operation__Response(success    = True                                  ,
#                                                        operation  = Enum__Cache__Zip__Operation.LIST      ,
#                                                        cache_id   = request.cache_id                      ,
#                                                        file_list  = file_list                             ,
#                                                        message    = f"Found {len(file_list)} files in zip")
#
#
#     @type_safe
#     def operation__get(self, request: Schema__Cache__Zip__Operation__Request
#                         ) -> Schema__Cache__Zip__Operation__Response:                      # Get single file from zip
#
#         if not request.file_path:
#             return self.error_response(request, "file_path required for get operation")
#
#         zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
#         if not zip_bytes:
#             return self.error_response(request, "Zip file not found in cache")
#
#         file_content = zip_bytes__file(zip_bytes, str(request.file_path))
#         if file_content is None:
#             return self.error_response(request, f"File '{request.file_path}' not found in zip")
#
#         return Schema__Cache__Zip__Operation__Response(success      = True                                ,
#                                                        operation    = Enum__Cache__Zip__Operation.GET     ,
#                                                        cache_id     = request.cache_id                    ,
#                                                        file_content = file_content                        ,
#                                                        file_size    = len(file_content)                   ,
#                                                        message      = f"Retrieved '{request.file_path}'"  )
#
#     @type_safe
#     def operation__add(self, request: Schema__Cache__Zip__Operation__Request
#                       ) -> Schema__Cache__Zip__Operation__Response:                      # Add new file to zip
#
#         if not request.file_path or not request.file_content:
#             return self.error_response(request, "file_path and file_content required for add operation")
#
#         zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
#         if not zip_bytes:
#             return self.error_response(request, "Zip file not found in cache")
#
#         new_zip = zip_bytes__add_file(zip_bytes, str(request.file_path), request.file_content)
#         success = self.update_zip(request.cache_id, request.namespace, new_zip)
#
#         return Schema__Cache__Zip__Operation__Response(success        = success                               ,
#                                                        operation      = Enum__Cache__Zip__Operation.ADD       ,
#                                                        cache_id       = request.cache_id                      ,
#                                                        files_affected = [request.file_path]                   ,
#                                                        message        = f"Added '{request.file_path}' to zip" )
#
#     @type_safe
#     def operation__remove(self, request: Schema__Cache__Zip__Operation__Request
#                          ) -> Schema__Cache__Zip__Operation__Response:                   # Remove file from zip
#
#         if not request.file_path:
#             return self.error_response(request, "file_path required for remove operation")
#
#         zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
#         if not zip_bytes:
#             return self.error_response(request, "Zip file not found in cache")
#
#         new_zip = zip_bytes__remove_file(zip_bytes, str(request.file_path))
#         success = self.update_zip(request.cache_id, request.namespace, new_zip)
#
#         return Schema__Cache__Zip__Operation__Response(success        = success                                  ,
#                                                        operation      = Enum__Cache__Zip__Operation.REMOVE       ,
#                                                        cache_id       = request.cache_id                         ,
#                                                        files_affected = [request.file_path]                      ,
#                                                        message        = f"Removed '{request.file_path}' from zip")
#
#     @type_safe
#     def operation__replace(self, request: Schema__Cache__Zip__Operation__Request
#                           ) -> Schema__Cache__Zip__Operation__Response:                  # Replace existing file in zip
#
#         if not request.file_path or not request.file_content:
#             return self.error_response(request, "file_path and file_content required for replace operation")
#
#         zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
#         if not zip_bytes:
#             return self.error_response(request, "Zip file not found in cache")
#
#         new_zip = zip_bytes__replace_file(zip_bytes, str(request.file_path), request.file_content)
#         success = self.update_zip(request.cache_id, request.namespace, new_zip)
#
#         return Schema__Cache__Zip__Operation__Response(success        = success                                    ,
#                                                        operation      = Enum__Cache__Zip__Operation.REPLACE        ,
#                                                        cache_id       = request.cache_id                           ,
#                                                        files_affected = [request.file_path]                        ,
#                                                        message        = f"Replaced '{request.file_path}' in zip"  )
#
#     @type_safe
#     def operation__move(self, request: Schema__Cache__Zip__Operation__Request
#                        ) -> Schema__Cache__Zip__Operation__Response:                     # Move/rename file in zip
#
#         if not request.file_path or not request.new_file_path:
#             return self.error_response(request, "file_path and new_file_path required for move operation")
#
#         zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
#         if not zip_bytes:
#             return self.error_response(request, "Zip file not found in cache")
#
#         new_zip = zip_bytes__move_file(zip_bytes, str(request.file_path), str(request.new_file_path))       # todo: this will need to be implemented as a delete and add
#         success = self.update_zip(request.cache_id, request.namespace, new_zip)
#
#         return Schema__Cache__Zip__Operation__Response(success        = success                                                                    ,
#                                                        operation      = Enum__Cache__Zip__Operation.MOVE                                           ,
#                                                        cache_id       = request.cache_id                                                           ,
#                                                        files_affected = [request.file_path, request.new_file_path]                                 ,
#                                                        message        = f"Moved '{request.file_path}' to '{request.new_file_path}' in zip"        )
#
#     def retrieve_zip_bytes(self, cache_id  : Random_Guid    ,
#                                  namespace : str
#                           ) -> Optional[bytes]:                                          # Get zip from cache
#         result = self.cache_service.retrieve_by_id(cache_id, namespace)
#         if result and result.get('data_type') == 'binary':
#             return result.get('data')
#         return None
#
#     def error_response(self, request       : Schema__Cache__Zip__Operation__Request,
#                              error_details : str
#                         ) -> Schema__Cache__Zip__Operation__Response:                    # Create error response
#         return Schema__Cache__Zip__Operation__Response(success       = False                 ,
#                                                        operation     = request.operation     ,
#                                                        cache_id      = request.cache_id      ,
#                                                        error_details = error_details         )
#
#     def update_zip(self, cache_id: Random_Guid, namespace: str, new_zip_bytes: bytes) -> bool:
#         handler = self.cache_service.get_or_create_handler(namespace=namespace)                         # Get the cache handler for this namespace
#
#         cache_entry = self.cache_service.retrieve_by_id(cache_id, namespace)                            # Retrieve the existing cache entry to get file paths and configuration
#         if not cache_entry:
#             return False
#
#         content_files = cache_entry.get('file_paths', {}).get('content_files', [])                      # Get the content file paths from the cache entry
#         if not content_files:
#             return False
#
#         files_updated = []                                                                              # Update the file at all existing paths using Memory_FS
#         for content_path in content_files:
#             success = handler.storage_backend.file__save(content_path, new_zip_bytes)
#             if success:
#                 files_updated.append(content_path)
#
#         new_hash        = self.cache_service.hash_from_bytes(new_zip_bytes)                             # Update metadata with new hash and size
#         metadata_update = { 'content__hash' : new_hash,
#                             'content__size' : len(new_zip_bytes),
#                             'updated_at'    : timestamp_now() }
#
#         # todo: review this pattern, since we should be leveraging memory-fs to save the metadata
#         for content_path in content_files:                                                              # Update metadata files (if they exist)
#             metadata_path = content_path + '.metadata'
#             existing_metadata = handler.storage_backend.file__json(metadata_path)
#             if existing_metadata:
#                 existing_metadata.update(metadata_update)
#                 handler.storage_backend.file__save(metadata_path,
#                                                    json_to_bytes(existing_metadata) )
#
#         return len(files_updated) > 0