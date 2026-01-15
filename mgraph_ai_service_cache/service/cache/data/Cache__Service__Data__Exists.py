from osbot_utils.decorators.methods.cache_on_self                                   import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                  import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                      import type_safe
from osbot_utils.utils.Http                                                         import url_join_safe
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type      import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve        import Cache__Service__Retrieve


class Cache__Service__Data__Exists(Type_Safe):                                              # Service for checking if data files exist in cache entries
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service=self.cache_service)

    def get_extension_for_type(self, data_type: Enum__Cache__Data_Type) -> str:             # Map data type to file extension
        extensions = { Enum__Cache__Data_Type.STRING : 'txt' ,
                       Enum__Cache__Data_Type.JSON   : 'json',
                       Enum__Cache__Data_Type.BINARY : 'bin' }
        return extensions.get(data_type, 'bin')

    @type_safe
    def build_data_file_path(self, data_folder  : Safe_Str__File__Path,                     # Centralized path construction matching other data services
                                   data_key     : Safe_Str__File__Path = None,
                                   data_file_id : Safe_Str__Id         = None,
                                   data_type    : Enum__Cache__Data_Type = None
                            ) -> Safe_Str__File__Path:

        if not data_file_id:
            raise ValueError("data_file_id is required for data file operations")

        path = data_folder                                                                  # Start with base data folder

        if data_key:                                                                        # Add optional data_key path segment
            path = url_join_safe(path, data_key)

        path = url_join_safe(path, data_file_id)                                            # Add data_file_id

        if data_type:                                                                       # Add extension based on data type
            extension = self.get_extension_for_type(data_type)
            path = Safe_Str__File__Path(f"{path}.{extension}")

        return path

    @type_safe
    def data_file_exists(self,
                         cache_id     : Cache_Id              ,
                         namespace    : Safe_Str__Id          ,
                         data_type    : Enum__Cache__Data_Type,
                         data_key     : Safe_Str__File__Path  ,
                         data_file_id : Safe_Str__Id
                    ) -> bool:                                                              # Check if a specific data file exists under a cache entry

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id  = cache_id,
                                                                 namespace = namespace)

        if not file_refs or not file_refs.file_paths.data_folders:
            return False

        handler = self.cache_service.get_or_create_handler(namespace)

        for data_folder in file_refs.file_paths.data_folders:                               # Check each data folder
            full_path = self.build_data_file_path(data_folder   = data_folder   ,
                                                  data_key      = data_key      ,
                                                  data_file_id  = data_file_id  ,
                                                  data_type     = data_type     )

            if handler.storage_backend.file__exists(full_path):                             # file__exists is available in Storage_FS
                return True

        return False