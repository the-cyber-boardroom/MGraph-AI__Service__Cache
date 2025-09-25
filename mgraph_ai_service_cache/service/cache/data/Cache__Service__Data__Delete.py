from osbot_utils.decorators.methods.cache_on_self                                       import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path       import Safe_Str__File__Path
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                          import type_safe
from osbot_utils.utils.Http                                                             import url_join_safe
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request  import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                 import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve            import Cache__Service__Retrieve



class Cache__Service__Data__Delete(Type_Safe):
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    # todo: this should be refactored to a share method/capability since there are multiple parts of the codebase that have this pattern
    def get_extension_for_type(self, data_type: Enum__Cache__Data_Type) -> str:             # Same logic as Store to ensure consistency
        extensions = {
            Enum__Cache__Data_Type.STRING : 'txt',
            Enum__Cache__Data_Type.JSON   : 'json',
            Enum__Cache__Data_Type.BINARY : 'bin'
        }
        return extensions.get(data_type, "data")

    @type_safe
    def delete_data(self, request: Schema__Cache__Data__Retrieve__Request) -> bool:         # Direct delete using storage backend

        file_refs = self.retrieve_service().retrieve_by_id__refs(
            cache_id=request.cache_id,
            namespace=request.namespace
        )

        if not file_refs or not file_refs.file_paths.data_folders:
            return False

        handler = self.cache_service.get_or_create_handler(request.namespace)
        extension = self.get_extension_for_type(request.data_type)
        deleted = False

        for data_folder in file_refs.file_paths.data_folders:
            data_path = data_folder
            if request.data_key:
                data_path = url_join_safe(data_path, request.data_key)
            if request.data_file_id:
                data_path = url_join_safe(data_path, request.data_file_id)

            full_path = Safe_Str__File__Path(f"{data_path}.{extension}")

            if handler.storage_backend.file__exists(full_path):                              # Direct delete
                deleted = handler.storage_backend.file__delete(full_path) or deleted

        return deleted