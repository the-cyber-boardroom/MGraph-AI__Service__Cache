# from typing                                                                                 import Any, List, Dict, Optional
# from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
# from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
# from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
# from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text                import Safe_Str__Text
# from osbot_utils.type_safe.primitives.core.Safe_UInt                                        import Safe_UInt
# from osbot_utils.type_safe.type_safe_core.decorators.type_safe                              import type_safe
# from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                            import DEFAULT_CACHE__STORE__STRATEGY, DEFAULT_CACHE__NAMESPACE
# from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
# from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
#
#
# class Schema__Child__File__Info(Type_Safe):                                                     # Information about a child file
#     child_id  : Safe_Str__Id                                                                    # Child file identifier
#     path      : Safe_Str__File__Path                                                            # Full file path
#     data_type : Safe_Str__Text                                                                  # Data type: json, string, binary
#     extension : Safe_Str__Text                                                                  # File extension
#
#
# class Schema__Child__File__Data(Type_Safe):                                                     # Child file with its data
#     data      : Any                                                                              # Actual file content
#     data_type : Safe_Str__Text                                                                  # Data type: json, string, binary
#     child_id  : Safe_Str__Id                                                                    # Child file identifier
#     path      : Safe_Str__File__Path                                                            # Full file path
#
#
# class Service__Cache__Retrieve__Child(Type_Safe):                                               # Service layer for retrieving child files
#     cache_service : Cache__Service                                                              # Underlying cache service instance
#
#     @type_safe
#     def retrieve_child(self, child_id  : Safe_Str__Id                                         ,  # Child identifier to retrieve
#                              namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE,
#                              strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
#                              cache_key : Safe_Str__File__Path         = None,
#                              file_id   : Safe_Str__Id                 = None
#                         ) -> Optional[Schema__Child__File__Data]:                                # Returns child data or None
#
#         if not file_id:
#             raise ValueError("file_id is required to retrieve child")
#         if not cache_key:
#             raise ValueError("cache_key is required to retrieve child")
#         if not child_id:
#             raise ValueError("child_id is required to retrieve child")
#
#         handler    = self.cache_service.get_or_create_handler(namespace)                        # Get handler and calculate path
#         child_path = self._get_child_path(handler, strategy, cache_key, file_id, child_id)
#
#         if not child_path:
#             return None
#
#         for ext, data_type in [('.json', 'json'), ('.txt', 'string'), ('.bin', 'binary')]:     # Try different extensions
#             full_path = Safe_Str__File__Path(str(child_path) + ext)
#
#             if handler.storage_backend.file__exists(full_path):
#                 if data_type == 'json':                                                         # Read file based on type
#                     data = handler.storage_backend.file__json(full_path)
#                 elif data_type == 'string':
#                     data = handler.storage_backend.file__str(full_path)
#                 else:
#                     data = handler.storage_backend.file__bytes(full_path)
#
#                 return Schema__Child__File__Data(data      = data                           ,
#                                                  data_type = Safe_Str__Text(data_type)     ,
#                                                  child_id  = child_id                       ,
#                                                  path      = full_path                      )
#         return None
#
#     @type_safe
#     def list_children(self, namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE,
#                             strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
#                             cache_key : Safe_Str__File__Path         = None,
#                             file_id   : Safe_Str__Id                 = None
#                        ) -> List[Schema__Child__File__Info]:                                    # List all child files for parent
#
#         if not file_id:
#             raise ValueError("file_id is required to list children")
#         if not cache_key:
#             raise ValueError("cache_key is required to list children")
#
#         handler      = self.cache_service.get_or_create_handler(namespace)                      # Get handler and calculate path
#         parent_base  = self._get_parent_base_path(handler, strategy, cache_key, file_id)
#
#         if not parent_base:
#             return []
#
#         child_folder = Safe_Str__File__Path(f"{parent_base}/{file_id}")                        # Child folder path
#         child_files  = []
#
#         if hasattr(handler.storage_backend, 'folder__files'):                                  # Use folder__files if available
#             files = handler.storage_backend.folder__files(child_folder, return_full_path=True)
#         else:
#             all_files        = handler.storage_backend.files__paths()                          # Fallback to filtering
#             child_folder_str = str(child_folder) + '/'
#             files            = [f for f in all_files if str(f).startswith(child_folder_str)]
#
#         for file_path in files:                                                                 # Parse child file information
#             file_str = str(file_path)
#             filename = file_str.split('/')[-1] if '/' in file_str else file_str
#
#             if '.' in filename:
#                 child_id_part = filename.rsplit('.', 1)[0]
#                 extension     = filename.rsplit('.', 1)[1]
#                 data_type     = 'string' if extension == 'txt' else (
#                                'json' if extension == 'json' else 'binary')
#
#                 child_files.append(Schema__Child__File__Info(child_id  = Safe_Str__Id(child_id_part),
#                                                              path      = Safe_Str__File__Path(file_str),
#                                                              data_type = Safe_Str__Text(data_type),
#                                                              extension = Safe_Str__Text(extension)))
#         return child_files
#
#     @type_safe
#     def count_children(self, namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE,
#                              strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
#                              cache_key : Safe_Str__File__Path         = None,
#                              file_id   : Safe_Str__Id                 = None
#                         ) -> Safe_UInt:                                                          # Count number of child files
#         children = self.list_children(namespace, strategy, cache_key, file_id)
#         return Safe_UInt(len(children))
#
#     @type_safe
#     def delete_child(self, child_id  : Safe_Str__Id                                           ,  # Delete specific child file
#                            namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE,
#                            strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
#                            cache_key : Safe_Str__File__Path         = None,
#                            file_id   : Safe_Str__Id                 = None
#                       ) -> bool:                                                                 # Returns True if deleted
#
#         if not file_id:
#             raise ValueError("file_id is required to delete child")
#         if not cache_key:
#             raise ValueError("cache_key is required to delete child")
#         if not child_id:
#             raise ValueError("child_id is required to delete child")
#
#         handler    = self.cache_service.get_or_create_handler(namespace)                       # Get handler and calculate path
#         child_path = self._get_child_path(handler, strategy, cache_key, file_id, child_id)
#
#         if not child_path:
#             return False
#
#         deleted = False
#         for ext in ['.json', '.txt', '.bin']:                                                  # Try to delete all extensions
#             full_path = Safe_Str__File__Path(str(child_path) + ext)
#             if handler.storage_backend.file__exists(full_path):
#                 deleted = handler.storage_backend.file__delete(full_path) or deleted
#
#         return deleted
#
#     @type_safe
#     def delete_all_children(self, namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE,
#                                   strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
#                                   cache_key : Safe_Str__File__Path         = None,
#                                   file_id   : Safe_Str__Id                 = None
#                              ) -> Safe_UInt:                                                     # Delete all children, return count
#
#         children      = self.list_children(namespace, strategy, cache_key, file_id)
#         deleted_count = Safe_UInt(0)
#
#         for child_info in children:
#             if self.delete_child(child_info.child_id, namespace, strategy, cache_key, file_id):
#                 deleted_count = Safe_UInt(deleted_count + 1)
#
#         return deleted_count
#
#     def _get_parent_base_path(self, handler  : Any                         ,                    # Calculate parent base path
#                                     strategy : Enum__Cache__Store__Strategy,
#                                     cache_key: Safe_Str__File__Path        ,
#                                     file_id  : Safe_Str__Id
#                                ) -> Optional[str]:
#         fs_data      = handler.get_fs_for_strategy(strategy)
#         parent_paths = []
#
#         for path_handler in handler.path_handlers:
#             if hasattr(fs_data, 'path_handlers'):
#                 for ph in fs_data.path_handlers:
#                     parent_path = ph.generate_path(file_id=file_id, file_key=cache_key)
#                     parent_paths.append(parent_path)
#             else:
#                 parent_path = path_handler.generate_path(file_id=file_id, file_key=cache_key)
#                 parent_paths.append(parent_path)
#
#         if not parent_paths:
#             parent_paths = [Safe_Str__File__Path(f"{strategy}/{cache_key}/{file_id}")]
#
#         parent_base_path = str(parent_paths[0])
#
#         for ext in ['.json', '.txt', '.bin', '.data']:                                         # Remove file extension
#             if parent_base_path.endswith(ext):
#                 parent_base_path = parent_base_path[:-len(ext)]
#                 break
#
#         return parent_base_path
#
#     def _get_child_path(self, handler  : Any                         ,                         # Get path for a child file
#                              strategy : Enum__Cache__Store__Strategy,
#                              cache_key: Safe_Str__File__Path        ,
#                              file_id  : Safe_Str__Id                ,
#                              child_id : Safe_Str__Id
#                         ) -> Optional[str]:
#         parent_base = self._get_parent_base_path(handler, strategy, cache_key, file_id)
#         if parent_base:
#             return f"{parent_base}/{file_id}/{child_id}"
#         return None