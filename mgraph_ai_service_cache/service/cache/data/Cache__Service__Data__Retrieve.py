from typing                                                                                 import Optional
from osbot_utils.decorators.methods.cache_on_self                                           import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.core.Safe_UInt                                        import Safe_UInt
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                              import type_safe
from osbot_utils.utils.Http                                                                 import url_join_safe
from osbot_utils.utils.Json                                                                 import json_to_bytes
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Files__List            import Schema__Cache__Data__Files__List
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Request      import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Response     import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                     import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve                import Cache__Service__Retrieve


class Cache__Service__Data__Retrieve(Type_Safe):                                                # Service layer for retrieving data files from cache entries
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    @type_safe
    def build_data_file_path(self, data_folder  : Safe_Str__File__Path,                         # Centralized path construction matching Store pattern
                                   data_key     : Safe_Str__File__Path = None,
                                   data_file_id : Safe_Str__Id         = None,
                                   data_type    : Enum__Cache__Data_Type = None
                            ) -> Safe_Str__File__Path:

        if not data_file_id:                                                                    # data_file_id is now required
            raise ValueError("data_file_id is required for data file operations")

        path = data_folder                                                                      # Start with base data folder

        if data_key:                                                                            # Add optional data_key path segment
            path = url_join_safe(path, data_key)

        path = url_join_safe(path, data_file_id)                                                # Add data_file_id

        if data_type:                                                                           # Add extension based on data type
            extension = self.get_extension_for_type(data_type)
            path = Safe_Str__File__Path(f"{path}.{extension}")

        return path

    @type_safe
    def retrieve_data(self, request: Schema__Cache__Data__Retrieve__Request
                       ) -> Schema__Cache__Data__Retrieve__Response:                             # Main retrieval method with explicit data type

        if not request.data_file_id:                                                           # Validate required fields
            return Schema__Cache__Data__Retrieve__Response(found=False,
                                                          data_type=request.data_type)

        if not request.data_type:
            raise ValueError("data_type is required for retrieval")

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id  = request.cache_id  ,
                                                                 namespace = request.namespace)

        if not file_refs or not file_refs.file_paths.data_folders:
            return Schema__Cache__Data__Retrieve__Response(found=False,
                                                          data_type=request.data_type)

        handler = self.cache_service.get_or_create_handler(request.namespace)

        for data_folder in file_refs.file_paths.data_folders:                                   # Try each data folder
            full_path = self.build_data_file_path(data_folder       = data_folder      ,
                                                  data_key          = request.data_key ,
                                                  data_file_id      = request.data_file_id,
                                                  data_type         = request.data_type)

            data = self.read_file_by_type(storage_backend = handler.storage_backend,
                                          path            = full_path               ,
                                          data_type       = request.data_type      )

            if data is not None:                                                                # Found the file
                size = self.calculate_size(data, request.data_type)

                return Schema__Cache__Data__Retrieve__Response(data         = data                  ,
                                                               data_type    = request.data_type     ,
                                                               data_file_id = request.data_file_id  ,
                                                               data_key     = request.data_key      ,
                                                               full_path    = full_path             ,
                                                               size         = Safe_UInt(size)       ,
                                                               found        = True                  )

        return Schema__Cache__Data__Retrieve__Response(found     = False,
                                                       data_type = request.data_type)

    @type_safe
    def list_data_files(self, cache_id  : Random_Guid          ,                                # List all data files
                              namespace : Safe_Str__Id         ,
                              data_key  : Safe_Str__File__Path = None
                        ) -> Schema__Cache__Data__Files__List:

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id = cache_id ,
                                                                 namespace = namespace)
        files_list = Schema__Cache__Data__Files__List(cache_id   = cache_id  ,
                                                      namespace  = namespace ,
                                                      data_key   = data_key  )
        if file_refs:
            data_folders = file_refs.file_paths.data_folders
            if data_folders:
                handler     = self.cache_service.get_or_create_handler(namespace)
                for data_folder in data_folders:
                    folder_path = data_folder
                    if data_key:                                                                     # Filter by data_key if provided
                        folder_path = url_join_safe(folder_path, data_key)
                    files_list.data_files = handler.storage_backend.folder__files__all(parent_folder=folder_path) # note: for now only return the list of files

        return files_list

    def read_file_by_type(self, storage_backend,                                                # Read file based on explicit type
                               path     : Safe_Str__File__Path,
                               data_type: Enum__Cache__Data_Type) -> Optional[any]:

        if not storage_backend.file__exists(path):
            return None

        if data_type == Enum__Cache__Data_Type.JSON:
            return storage_backend.file__json(path)
        elif data_type == Enum__Cache__Data_Type.STRING:
            return storage_backend.file__str(path)
        elif data_type == Enum__Cache__Data_Type.BINARY:
            return storage_backend.file__bytes(path)
        else:
            raise ValueError(f"Unknown data type: {data_type}")

    def get_extension_for_type(self, data_type: Enum__Cache__Data_Type) -> str:                 # Extension mapping (same as Store)
        extensions = { Enum__Cache__Data_Type.STRING : 'txt' ,
                      Enum__Cache__Data_Type.JSON   : 'json',
                      Enum__Cache__Data_Type.BINARY : 'bin' }
        return extensions.get(data_type, "data")

    def get_type_from_extension(self, extension: str) -> Enum__Cache__Data_Type:                # Reverse mapping for listing
        ext_map = { 'json' : Enum__Cache__Data_Type.JSON  ,
                   'txt'  : Enum__Cache__Data_Type.STRING,
                   'bin'  : Enum__Cache__Data_Type.BINARY }
        return ext_map.get(extension, Enum__Cache__Data_Type.BINARY)

    def calculate_size(self, data, data_type: Enum__Cache__Data_Type) -> int:                   # Calculate data size based on type
        if data_type == Enum__Cache__Data_Type.BINARY:
            return len(data) if data else 0
        elif data_type == Enum__Cache__Data_Type.STRING:
            return len(data.encode('utf-8')) if data else 0
        elif data_type == Enum__Cache__Data_Type.JSON:
            return len(json_to_bytes(data)) if data else 0        