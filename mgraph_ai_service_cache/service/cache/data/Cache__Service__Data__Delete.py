from osbot_utils.decorators.methods.cache_on_self                                                import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                            import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                  import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                                   import type_safe
from osbot_utils.utils.Http                                                                      import url_join_safe
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Delete__All_Files__Response import Schema__Cache__Data__Delete__All_Files__Response
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Request           import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                          import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                                        import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve                     import Cache__Service__Retrieve


class Cache__Service__Data__Delete(Type_Safe):                                                  # Service for deleting data files from cache entries
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    @type_safe
    def build_data_file_path(self, data_folder  : Safe_Str__File__Path,                         # Same path building logic as Store/Retrieve
                                   data_key     : Safe_Str__File__Path = None,
                                   data_file_id : Safe_Str__Id         = None,
                                   data_type    : Enum__Cache__Data_Type = None
                            ) -> Safe_Str__File__Path:

        if not data_file_id:
            raise ValueError("data_file_id is required for data file operations")

        path = data_folder                                                                      # Start with base data folder

        if data_key:                                                                            # Add optional data_key path segment
            path = url_join_safe(path, data_key)

        path = url_join_safe(path, data_file_id)                                                # Add data_file_id

        if data_type:                                                                           # Add extension based on data type
            extension = self.get_extension_for_type(data_type)
            path = Safe_Str__File__Path(f"{path}.{extension}")

        return path

    def get_extension_for_type(self, data_type: Enum__Cache__Data_Type) -> str:                 # Map data type to file extension
        extensions = { Enum__Cache__Data_Type.STRING : 'txt' ,
                      Enum__Cache__Data_Type.JSON   : 'json',
                      Enum__Cache__Data_Type.BINARY : 'bin' }
        return extensions.get(data_type, "data")

    @type_safe
    def delete_data_file(self, request: Schema__Cache__Data__Retrieve__Request) -> bool:        # Delete a specific data file

        if not request.data_file_id:
            raise ValueError("data_file_id is required for deletion")

        if not request.data_type:
            raise ValueError("data_type is required for deletion")

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id  = request.cache_id,
                                                                 namespace = request.namespace)

        if not file_refs or not file_refs.file_paths.data_folders:
            return False

        handler = self.cache_service.get_or_create_handler(request.namespace)
        deleted = False

        for data_folder in file_refs.file_paths.data_folders:                                   # Try each data folder
            full_path = self.build_data_file_path(data_folder   = data_folder        ,
                                                  data_key      = request.data_key  ,
                                                  data_file_id  = request.data_file_id,
                                                  data_type     = request.data_type  )

            if handler.storage_backend.file__exists(full_path):
                deleted = handler.storage_backend.file__delete(full_path) or deleted

        return deleted

    @type_safe
    def delete_all_data_files(self, cache_id  : Random_Guid                 ,                   # Delete all data files for a cache entry
                                    namespace : Safe_Str__Id                ,
                                    data_key  : Safe_Str__File__Path = None
                             ) -> Schema__Cache__Data__Delete__All_Files__Response:

        delete_response = Schema__Cache__Data__Delete__All_Files__Response()
        file_refs       = self.retrieve_service().retrieve_by_id__refs(cache_id  = cache_id,
                                                                       namespace = namespace)
        if file_refs:
            handler = self.cache_service.get_or_create_handler(namespace)

            for data_folder in file_refs.file_paths.data_folders:
                if data_key:                                                                         # Delete only files under specific data_key
                    target_folder = url_join_safe(data_folder, data_key)
                else:                                                                               # Delete all data files
                    target_folder = data_folder


                files = handler.storage_backend.folder__files__all(parent_folder= target_folder)

                for file_path in files:
                    if handler.storage_backend.file__delete(file_path):
                        delete_response.deleted_count += 1
                        delete_response.deleted_files.append(str(file_path))

        return delete_response