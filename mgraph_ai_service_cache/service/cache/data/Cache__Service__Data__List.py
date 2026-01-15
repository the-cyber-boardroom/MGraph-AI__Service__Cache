from typing                                                                                     import List, Optional
from osbot_utils.decorators.methods.cache_on_self                                               import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                            import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path               import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                              import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                 import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                                  import type_safe
from osbot_utils.utils.Http                                                                     import url_join_safe
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__List__Response      import Schema__Cache__Data__List__Response
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__File__Info          import Schema__Cache__Data__File__Info
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                  import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve                    import Cache__Service__Retrieve


class Cache__Service__Data__List(Type_Safe):                                                    # Service for listing data files in cache entries
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service=self.cache_service)

    def get_type_from_extension(self, extension: str) -> Enum__Cache__Data_Type:                # Map extension to data type
        ext_map = { 'json' : Enum__Cache__Data_Type.JSON  ,
                    'txt'  : Enum__Cache__Data_Type.STRING,
                    'bin'  : Enum__Cache__Data_Type.BINARY }
        return ext_map.get(extension, Enum__Cache__Data_Type.BINARY)

    @type_safe
    def list_data_files(self,
                        cache_id  : Cache_Id             ,
                        namespace : Safe_Str__Id         ,
                        data_key  : Safe_Str__File__Path = None,
                        recursive : bool                 = True
                   ) -> Optional[Schema__Cache__Data__List__Response]:                          # List all data files under a cache entry

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id  = cache_id,
                                                                 namespace = namespace)

        if not file_refs:
            return None                                                                         # Cache entry doesn't exist

        data_folders = file_refs.file_paths.data_folders

        if not data_folders:
            return Schema__Cache__Data__List__Response(cache_id   = cache_id  ,                 # Return empty response if no data folders
                                                       namespace  = namespace ,
                                                       data_key   = data_key  ,
                                                       file_count = 0         ,
                                                       files      = []        ,
                                                       total_size = 0         )

        handler    = self.cache_service.get_or_create_handler(namespace)
        files      = []
        total_size = 0

        for data_folder in data_folders:
            target_folder = data_folder                                                         # Build target folder path

            if data_key:
                target_folder = url_join_safe(data_folder, data_key)

            file_paths = handler.storage_backend.folder__files__all(parent_folder=target_folder)  # Always use folder__files__all (only available method)

            if not recursive:                                                                   # Filter for non-recursive: only files directly in target_folder
                target_folder_str = str(target_folder)
                if not target_folder_str.endswith('/'):
                    target_folder_str += '/'
                file_paths = [fp for fp in file_paths
                              if '/' not in str(fp).replace(target_folder_str, '', 1)]          # No subdirectories

            for file_path in file_paths:                                                        # Parse file info for each file
                if file_path.endswith('.config') or file_path.endswith('.metadata'):            # Skip config and metadata files
                    continue

                file_info = self._parse_file_info(file_path       = file_path      ,
                                                  data_folder     = data_folder    ,
                                                  storage_backend = handler.storage_backend)
                if file_info:
                    files.append(file_info)
                    if file_info.file_size:
                        total_size += file_info.file_size

        return Schema__Cache__Data__List__Response(cache_id   = cache_id      ,
                                                   namespace  = namespace     ,
                                                   data_key   = data_key      ,
                                                   file_count = len(files)    ,
                                                   files      = files         ,
                                                   total_size = total_size    )

    def _parse_file_info(self,
                         file_path       : str,
                         data_folder     : str,
                         storage_backend
                    ) -> Schema__Cache__Data__File__Info:                                    # Parse a file path into Schema__Cache__Data__File__Info

        try:
            filename = file_path.split('/')[-1]                                                 # Extract filename from path

            if '.' in filename:                                                                 # Parse extension and data_file_id
                parts        = filename.rsplit('.', 1)
                data_file_id = parts[0]
                extension    = parts[1]
            else:
                data_file_id = filename
                extension    = ''

            data_type = self.get_type_from_extension(extension)                                 # Determine data type from extension

            data_folder_with_slash = data_folder if data_folder.endswith('/') else data_folder + '/'
            relative_path = file_path.replace(data_folder_with_slash, '', 1)                    # Extract data_key from path

            if '/' in relative_path:
                data_key = '/'.join(relative_path.split('/')[:-1])
            else:
                data_key = ''

            file_size = self._get_file_size(storage_backend, file_path)                         # Get file size safely

            return Schema__Cache__Data__File__Info(data_file_id = data_file_id,
                                                   data_key     = data_key    ,
                                                   data_type    = data_type   ,
                                                   file_path    = file_path   ,
                                                   file_size    = file_size   ,
                                                   extension    = extension   )
        except Exception:
            return None

    def _get_file_size(self, storage_backend, file_path: str) -> int:                           # Get file size safely - not all backends have file__size
        try:
            if hasattr(storage_backend, 'file__size'):
                return storage_backend.file__size(file_path) or 0
            file_bytes = storage_backend.file__bytes(file_path)                                 # Fallback: read bytes and get length
            return len(file_bytes) if file_bytes else 0
        except Exception:
            return 0