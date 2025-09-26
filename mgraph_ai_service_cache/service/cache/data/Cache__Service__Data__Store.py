from typing                                                                           import Any
from osbot_utils.decorators.methods.cache_on_self                                     import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                  import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                 import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id       import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text          import Safe_Str__Text
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                        import type_safe
from osbot_utils.utils.Http                                                           import url_join_safe
from osbot_utils.utils.Json                                                           import json_to_bytes
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type               import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Store__Request   import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Store__Response  import Schema__Cache__Data__Store__Response
from mgraph_ai_service_cache.service.cache.Cache__Service                             import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve          import Cache__Service__Retrieve


class Cache__Service__Data__Store(Type_Safe):                                                  # Service layer for lightweight child file storage
    cache_service : Cache__Service                                                              # Underlying cache service instance

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    @type_safe
    def store_data(self, request: Schema__Cache__Data__Store__Request) -> Schema__Cache__Data__Store__Response:                  #
        file_refs    = self.retrieve_service().retrieve_by_id__refs(cache_id=request.cache_id, namespace=request.namespace)
        if file_refs:
            files_created    = []
            data_folders     = file_refs.file_paths.data_folders
            serialized_data  = self.serialize_data(request.data, request.data_type.value)                   # Serialize data based on type
            extension        = self.get_extension_for_type(request.data_type)                               # Determine file extension
            for data_folder in data_folders:
                data_path = data_folder
                if request.data_key:
                    data_path = url_join_safe(data_path, request.data_key)
                if not request.data_file_id:
                    request.data_file_id =  Safe_Str__Id(Random_Guid())
                data_path = url_join_safe(data_path, request.data_file_id)
                data_path  += f'.{extension}'

                handler = self.cache_service.get_or_create_handler(namespace=request.namespace)
                success = handler.storage_backend.file__save(data_path, serialized_data)              # Direct write without config/metadata

                if success:
                    files_created.append(data_path)
                else:
                    raise RuntimeError(f"Failed to save child file at {data_path}")


            return Schema__Cache__Data__Store__Response(cache_id           = request.cache_id,
                                                        data_files_created = files_created,
                                                        data_key           = request.data_key,
                                                        data_type          = request.data_type,
                                                        extension          = extension,
                                                        file_size          = len(serialized_data),
                                                        file_id            = request.data_file_id,
                                                        namespace          = request.namespace)
        else:
            return None

    # todo: refactor this to use Memory_Fs files capabilities
    def get_extension_for_type(self, data_type : Enum__Cache__Data_Type  # Map data type to file extension
                               ) -> str:
        extensions = { Enum__Cache__Data_Type.STRING : 'txt'  ,
                       Enum__Cache__Data_Type.JSON   : 'json' ,
                       Enum__Cache__Data_Type.BINARY : 'bin'  }
        return extensions.get(data_type, "data")

    # todo leverage memory_fs which already has these capabilities
    def serialize_data(self, data      : Any             ,                                     # Serialize data based on its type
                             data_type : Safe_Str__Text
                        ) -> bytes:
        data_type_str = str(data_type)
        if data_type_str == 'string':
            return str(data).encode('utf-8')
        elif data_type_str == 'json':
            if isinstance(data, bytes):
                return data
            return json_to_bytes(data)
        elif data_type_str == 'binary':
            if isinstance(data, bytes):
                return data
            raise ValueError(f"Binary data must be bytes, got {type(data)}")
        else:
            raise ValueError(f"Unknown data type: {data_type_str}")