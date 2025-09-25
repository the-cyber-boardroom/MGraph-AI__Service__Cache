from typing                                                                                  import List
from osbot_utils.decorators.methods.cache_on_self                                       import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path       import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.core.Safe_UInt                                    import Safe_UInt
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text            import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id         import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                          import type_safe
from osbot_utils.utils.Http                                                             import url_join_safe
from osbot_utils.utils.Json                                                             import json_to_bytes
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__File__Info         import Schema__Cache__Data__File__Info
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request  import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Response import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                 import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve            import Cache__Service__Retrieve


class Cache__Service__Data__Retrieve(Type_Safe):
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    @type_safe
    def retrieve_data(self, request: Schema__Cache__Data__Retrieve__Request
                       ) -> Schema__Cache__Data__Retrieve__Response:                             # Main retrieval method

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id  = request.cache_id  ,
                                                                 namespace = request.namespace)

        if not file_refs or not file_refs.file_paths.data_folders:
            return Schema__Cache__Data__Retrieve__Response(found=False)

        handler   = self.cache_service.get_or_create_handler(request.namespace)
        extension = self.get_extension_for_type(request.data_type)

        for data_folder in file_refs.file_paths.data_folders:                                   # Try each data folder
            data_path = data_folder
            if request.data_key:
                data_path = url_join_safe(data_path, request.data_key)
            if request.data_file_id:
                data_path = url_join_safe(data_path, request.data_file_id)

            full_path = Safe_Str__File__Path(f"{data_path}.{extension}")

            data = self.read_direct(storage_backend = handler.storage_backend,
                                   path            = full_path              ,
                                   data_type       = request.data_type      )

            if data is not None:                                                                # Found the file
                size = self.calculate_size(data)

                return Schema__Cache__Data__Retrieve__Response(data         = data                  ,
                                                               data_type    = request.data_type     ,
                                                               data_file_id = request.data_file_id  ,
                                                               data_key     = request.data_key      ,
                                                               full_path    = full_path             ,
                                                               size         = Safe_UInt(size)       ,
                                                               found        = True                  )

        return Schema__Cache__Data__Retrieve__Response(found=False)

    @type_safe
    def retrieve_data_auto_detect(self, cache_id     : Random_Guid          ,                   # Auto-detect file type
                                        namespace    : Safe_Str__Id         ,
                                        data_key     : Safe_Str__File__Path = None,
                                        data_file_id : Safe_Str__Id         = None
                                  ) -> Schema__Cache__Data__Retrieve__Response:

        for data_type in [Enum__Cache__Data_Type.JSON   ,                                       # Try each type in order
                          Enum__Cache__Data_Type.STRING ,
                          Enum__Cache__Data_Type.BINARY ]:

            request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id    ,
                                                             data_type    = data_type   ,
                                                             data_key     = data_key    ,
                                                             data_file_id = data_file_id,
                                                             namespace    = namespace   )
            result = self.retrieve_data(request)

            if result and result.found:
                return result

        return Schema__Cache__Data__Retrieve__Response(found=False)

    @type_safe
    def list_data_files(self, cache_id  : Random_Guid          ,                                # List all data files
                             namespace : Safe_Str__Id         ,
                             data_key  : Safe_Str__File__Path = None
                       ) -> List[Schema__Cache__Data__File__Info]:

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id = cache_id ,
                                                                 namespace = namespace)

        if not file_refs or not file_refs.file_paths.data_folders:
            return []

        handler = self.cache_service.get_or_create_handler(namespace)
        data_files = []

        for data_folder in file_refs.file_paths.data_folders:
            if hasattr(handler.storage_backend, 'folder__files'):
                folder_files = handler.storage_backend.folder__files(folder_path      = data_folder,
                                                                     return_full_path = True        )

                for file_path in folder_files:
                    file_info = self.create_file_info(file_path, data_key, handler)
                    if file_info:
                        data_files.append(file_info)

        return data_files

    @type_safe
    def count_data_files(self, cache_id  : Random_Guid          ,                               # Count data files
                              namespace : Safe_Str__Id         ,
                              data_key  : Safe_Str__File__Path = None
                        ) -> int:

        data_files = self.list_data_files(cache_id  = cache_id ,
                                          namespace = namespace,
                                          data_key  = data_key )
        return len(data_files)

    @type_safe
    def get_data_folder_size(self, cache_id  : Random_Guid  ,                                   # Get total size
                                   namespace : Safe_Str__Id
                            ) -> int:

        data_files = self.list_data_files(cache_id  = cache_id ,
                                          namespace = namespace)

        return sum(f.file_size for f in data_files)

    def read_direct(self, storage_backend,                                                      # Direct read helper
                          path     : Safe_Str__File__Path,
                          data_type: Enum__Cache__Data_Type):

        if data_type == Enum__Cache__Data_Type.JSON:
            return storage_backend.file__json(path)
        elif data_type == Enum__Cache__Data_Type.STRING:
            return storage_backend.file__str(path)
        else:                                                                                    # BINARY
            return storage_backend.file__bytes(path)

    def get_extension_for_type(self, data_type: Enum__Cache__Data_Type) -> str:                 # Extension mapping
        extensions = { Enum__Cache__Data_Type.STRING : 'txt' ,
                       Enum__Cache__Data_Type.JSON   : 'json',
                       Enum__Cache__Data_Type.BINARY : 'bin' }
        return extensions.get(data_type, "data")

    def get_type_from_extension(self, extension: str) -> Enum__Cache__Data_Type:                # Reverse mapping
        ext_map = { 'json' : Enum__Cache__Data_Type.JSON  ,
                    'txt'  : Enum__Cache__Data_Type.STRING,
                    'bin'  : Enum__Cache__Data_Type.BINARY }
        return ext_map.get(extension, Enum__Cache__Data_Type.BINARY)

    def calculate_size(self, data) -> int:                                                      # Calculate data size
        if isinstance(data, bytes):
            return len(data)
        elif isinstance(data, str):
            return len(data.encode('utf-8'))
        else:                                                                                    # dict/json
            return len(json_to_bytes(data))

    def create_file_info(self, file_path,                                                       # Create file info object
                              data_key,
                              handler) -> Schema__Cache__Data__File__Info:

        file_name = str(file_path).split('/')[-1]
        file_id   = file_name.split('.')[0] if '.' in file_name else file_name
        extension = file_name.split('.')[-1] if '.' in file_name else ''

        data_type = self.get_type_from_extension(extension)

        file_size = Safe_UInt(0)
        if hasattr(handler.storage_backend, 'file__size'):
            size_value = handler.storage_backend.file__size(file_path)
            if size_value:
                file_size = Safe_UInt(size_value)

        return Schema__Cache__Data__File__Info(data_file_id = Safe_Str__Id(file_id)               ,
                                               data_key     = data_key or Safe_Str__File__Path(""),
                                               full_path    = file_path                           ,
                                               data_type    = data_type                           ,
                                               extension    = Safe_Str__Text(extension)           ,
                                               file_size    = file_size                           )