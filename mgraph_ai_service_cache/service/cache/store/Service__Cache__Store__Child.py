# from typing                                                                                 import Any, List
# from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
# from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
# from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
# from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text                import Safe_Str__Text
# from osbot_utils.type_safe.type_safe_core.decorators.type_safe                              import type_safe
# from osbot_utils.utils.Json                                                                 import json_to_bytes
# from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                            import DEFAULT_CACHE__STORE__STRATEGY, DEFAULT_CACHE__NAMESPACE
# from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
# from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
# from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response                   import Schema__Cache__Store__Response
#
#
# class Service__Cache__Store__Child(Type_Safe):                                                  # Service layer for lightweight child file storage
#     cache_service : Cache__Service                                                              # Underlying cache service instance
#
#     @type_safe
#     def store_child(self, data      : Any                                   ,                   # Data to store as child
#                           data_type : Safe_Str__Text                        ,                   # Type: 'string', 'json', or 'binary'
#                           namespace : Safe_Str__Id                         = DEFAULT_CACHE__NAMESPACE,
#                           strategy  : Enum__Cache__Store__Strategy         = DEFAULT_CACHE__STORE__STRATEGY,
#                           cache_key : Safe_Str__File__Path                 = None,
#                           file_id   : Safe_Str__Id                         = None,
#                           child_id  : Safe_Str__Id                         = None
#                      ) -> Schema__Cache__Store__Response:                                       # Returns storage response with paths
#
#         if not file_id:                                                                         # Validate required parameters
#             raise ValueError("file_id is required for child storage")
#         if not cache_key:
#             raise ValueError("cache_key is required for child storage")
#
#         child_id         = child_id or Safe_Str__Id(str(Random_Guid()))                        # Generate child_id if not provided
#         handler          = self.cache_service.get_or_create_handler(namespace)                 # Get handler for namespace and strategy
#         fs_data          = handler.get_fs_for_strategy(strategy)
#         parent_paths     = []                                                                   # Calculate parent base path
#
#
#         return Schema__Cache__Store__Response()
#         for path_handler in handler.path_handlers:                                             # Build parent paths using handlers
#             if hasattr(fs_data, 'path_handlers'):
#                 for ph in fs_data.path_handlers:
#                     parent_path = ph.generate_path(file_id=file_id, file_key=cache_key)
#                     parent_paths.append(parent_path)
#             else:
#                 parent_path = path_handler.generate_path(file_id=file_id, file_key=cache_key)
#                 parent_paths.append(parent_path)
#
#         if not parent_paths:                                                                   # Default path if no handlers
#             parent_paths = [Safe_Str__File__Path(f"{strategy}/{cache_key}/{file_id}")]
#
#         parent_base_path = str(parent_paths[0])                                                # Get first path as base
#
#         for ext in ['.json', '.txt', '.bin', '.data']:                                         # Remove file extension from parent
#             if parent_base_path.endswith(ext):
#                 parent_base_path = parent_base_path[:-len(ext)]
#                 break
#
#         extension        = self._get_extension_for_type(data_type)                            # Determine file extension
#         child_path       = Safe_Str__File__Path(f"{parent_base_path}/{file_id}/{child_id}.{extension}")
#         serialized_data  = self._serialize_data(data, data_type)                               # Serialize data based on type
#
#         success = handler.storage_backend.file__save(child_path, serialized_data)              # Direct write without config/metadata
#
#         if not success:
#             raise RuntimeError(f"Failed to save child file at {child_path}")
#
#         if str(data_type) == 'string':                                                         # Calculate hash for response
#             child_hash = self.cache_service.hash_from_string(str(data))
#         elif str(data_type) == 'json':
#             child_hash = self.cache_service.hash_from_json(data)
#         else:
#             child_hash = self.cache_service.hash_from_bytes(serialized_data)
#
#         return Schema__Cache__Store__Response(cache_id  = Random_Guid(child_id)              ,  # Return response with child details
#                                               hash      = child_hash                         ,
#                                               namespace = namespace                          ,
#                                               paths     = { "child"           : [str(child_path)]  ,
#                                                            "parent_file_id"  : str(file_id)        ,
#                                                            "child_id"        : str(child_id)       },
#                                               size      = len(serialized_data)              )
#
#     def _get_extension_for_type(self, data_type : Safe_Str__Text                                # Map data type to file extension
#                                  ) -> str:
#         extensions = { 'string' : 'txt'  ,
#                        'json'   : 'json' ,
#                        'binary' : 'bin'  }
#         return extensions.get(str(data_type), 'data')
#
#     def _serialize_data(self, data      : Any             ,                                     # Serialize data based on its type
#                               data_type : Safe_Str__Text
#                          ) -> bytes:
#         data_type_str = str(data_type)
#         if data_type_str == 'string':
#             return str(data).encode('utf-8')
#         elif data_type_str == 'json':
#             if isinstance(data, bytes):
#                 return data
#             return json_to_bytes(data)
#         elif data_type_str == 'binary':
#             if isinstance(data, bytes):
#                 return data
#             raise ValueError(f"Binary data must be bytes, got {type(data)}")
#         else:
#             raise ValueError(f"Unknown data type: {data_type_str}")
#
#     @type_safe
#     def retrieve_children(self, namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE,
#                                 strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
#                                 cache_key : Safe_Str__File__Path         = None,
#                                 file_id   : Safe_Str__Id                 = None
#                            ) -> List[Safe_Str__File__Path]:                                     # Retrieve list of child files
#         if not file_id:
#             raise ValueError("file_id is required to retrieve children")
#         if not cache_key:
#             raise ValueError("cache_key is required to retrieve children")
#
#         handler          = self.cache_service.get_or_create_handler(namespace)                 # Get handler and calculate paths
#         fs_data          = handler.get_fs_for_strategy(strategy)
#         parent_paths     = []
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
#         for ext in ['.json', '.txt', '.bin', '.data']:
#             if parent_base_path.endswith(ext):
#                 parent_base_path = parent_base_path[:-len(ext)]
#                 break
#
#         child_folder = Safe_Str__File__Path(f"{parent_base_path}/{file_id}")                   # List files in child folder
#
#         if hasattr(handler.storage_backend, 'folder__files'):
#             child_files = handler.storage_backend.folder__files(child_folder, return_full_path=True)
#         else:
#             all_files        = handler.storage_backend.files__paths()                          # Fallback to filtering
#             child_folder_str = str(child_folder) + '/'
#             child_files      = [f for f in all_files if str(f).startswith(child_folder_str)]
#
#         return child_files