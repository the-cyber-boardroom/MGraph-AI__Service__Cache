from osbot_utils.decorators.methods.cache_on_self                                   import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.core.Safe_UInt                                import Safe_UInt
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                      import type_safe
from osbot_utils.utils.Http                                                         import url_join_safe
from osbot_utils.utils.Json                                                         import json_to_bytes

from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Response import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type             import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve        import Cache__Service__Retrieve



class Cache__Service__Data__Retrieve(Type_Safe):
    cache_service : Cache__Service

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    @type_safe
    def retrieve_data(self, request: Schema__Cache__Data__Retrieve__Request
                      ) -> Schema__Cache__Data__Retrieve__Response:

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id  = request.cache_id  ,        # Get the refs to find data folders
                                                                 namespace = request.namespace)

        if not file_refs or not file_refs.file_paths.data_folders:
            return Schema__Cache__Data__Retrieve__Response(found=False)

        handler = self.cache_service.get_or_create_handler(request.namespace)

        extension = self.get_extension_for_type(request.data_type)                                      # Determine extension from data_type (matching Store's logic)

        # todo: refactor this logic with the similar logic in Cache__Service__Data__Retrieve so that we only have one place where this logic exists
        for data_folder in file_refs.file_paths.data_folders:                                           # Build exact path (matching Store's approach)
            data_path = data_folder
            if request.data_key:
                data_path = url_join_safe(data_path, request.data_key)
            if request.data_file_id:
                data_path = url_join_safe(data_path, request.data_file_id)

            full_path = Safe_Str__File__Path(f"{data_path}.{extension}")

            data = self.read_direct(storage_backend = handler.storage_backend,                  # Direct read using storage backend
                                    path            = full_path              ,
                                    data_type       = request.data_type      )
            if data is not None:                                                                # if file exists and we got data from it
                if isinstance(data, bytes):                                                     # Calculate size based on actual data
                    size = len(data)
                elif isinstance(data, str):
                    size = len(data.encode('utf-8'))
                else:                                                                           # dict/json
                    size = len(json_to_bytes(data))

                return Schema__Cache__Data__Retrieve__Response(data         = data                  ,
                                                               data_type    = request.data_type     ,
                                                               data_file_id = request.data_file_id  ,
                                                               data_key     = request.data_key      ,
                                                               full_path    = full_path             ,
                                                               size         = Safe_UInt(size)       ,
                                                               found        = True                  )

        return Schema__Cache__Data__Retrieve__Response(found=False)

    def read_direct(self, storage_backend,
                          path     : Safe_Str__File__Path,
                          data_type: Enum__Cache__Data_Type):          # Direct read without Memory_FS overhead - matches Store's serialization
        if data_type == Enum__Cache__Data_Type.JSON:
            return storage_backend.file__json(path)
        elif data_type == Enum__Cache__Data_Type.STRING:
            return storage_backend.file__str(path)
        else:  # BINARY
            return storage_backend.file__bytes(path)

    def get_extension_for_type(self, data_type: Enum__Cache__Data_Type) -> str:
        """Same logic as Store to ensure consistency"""
        extensions = {
            Enum__Cache__Data_Type.STRING : 'txt',
            Enum__Cache__Data_Type.JSON   : 'json',
            Enum__Cache__Data_Type.BINARY : 'bin'
        }
        return extensions.get(data_type, "data")

    @type_safe
    def delete_data(self, request: Schema__Cache__Data__Retrieve__Request) -> bool:
        """Direct delete using storage backend"""

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

            # Direct delete
            if handler.storage_backend.file__exists(full_path):
                deleted = handler.storage_backend.file__delete(full_path) or deleted

        return deleted