import json
from typing import Any, Union
from osbot_utils.utils.Json                                                                      import json_to_bytes
from osbot_utils.utils.Misc                                                                      import str_to_bytes
from memory_fs.schemas.Schema__Memory_FS__File__Config                                           import Schema__Memory_FS__File__Config
from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                               import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                  import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash         import Safe_Str__Cache_Hash
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                                   import type_safe
from mgraph_ai_service_cache.service.cache.Cache__Service                                        import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Update__Response                import Schema__Cache__Update__Response
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__File__Refs                 import Schema__Cache__File__Refs


class Cache__Service__Update(Type_Safe):                                            # Service layer for updating existing cache entries
    cache_service : Cache__Service                                                  # Underlying cache service instance

    @type_safe
    def update_by_id(self,
                     cache_id  : Cache_Id               ,
                     namespace : Safe_Str__Id           ,
                     data      : Union[str, dict, bytes],
                ) -> Schema__Cache__Update__Response:                                # Update existing cache entry (hash-stable only, V1 limitation)

        existing_refs = self._load_existing_refs(cache_id  = cache_id  ,            # Load existing refs (1 S3 read)
                                                  namespace = namespace )
        if not existing_refs:                                                       # Entry doesn't exist
            return None

        paths_to_update = existing_refs.file_paths.content_files
        with self.cache_service.get_or_create_handler(namespace) as handler:        # Get direct storage access
            storage       = handler.storage_backend                                 # Direct storage backend
            serialized    = self._serialize_data(data)                              # Serialize new data

            for content_path in paths_to_update:                                    # Update each content file (N S3 writes)
                storage.file__save(content_path, serialized)

        return Schema__Cache__Update__Response(cache_id         = cache_id                        ,
                                               cache_hash       = existing_refs.cache_hash        ,  # V1: hash unchanged
                                               namespace        = namespace                       ,
                                               paths            = paths_to_update                 ,
                                               size             = len(data)                       ,
                                               updated_content  = True                            ,  # V1: content always updated
                                               updated_hash     = False                           ,  # V1: hash never updated
                                               updated_metadata = False                           ,  # V1: metadata never updated
                                               updated_id_ref   = False                           )  # V1: ID ref never updated
    @type_safe
    def _load_existing_config(self,
                              cache_id  : Cache_Id      ,
                              namespace : Safe_Str__Id
                         ) -> Schema__Memory_FS__File__Config:         # Retrieve existing entry's configuration

        config = self.cache_service.retrieve_by_id__config(cache_id  = cache_id  ,
                                                           namespace = namespace )

        return config

    @type_safe
    def _load_existing_refs(self,
                              cache_id  : Cache_Id      ,
                              namespace : Safe_Str__Id
                         ) -> Schema__Cache__File__Refs:            # Retrieve existing entry's refs

        refs = self.cache_service.retrieve_by_id__refs(cache_id  = cache_id  ,
                                                         namespace = namespace )

        return refs

    @type_safe
    def _calculate_hash(self,
                        data      : Any ,
                        data_type : str
                   ) -> Safe_Str__Cache_Hash:                                    # Calculate content hash based on data type

        if data_type == "binary":
            return self.cache_service.hash_from_bytes(data)
        elif data_type == "json":
            return self.cache_service.hash_from_json(data)
        else:                                                                   # string
            return self.cache_service.hash_from_string(data)

    @type_safe
    def _serialize_data(self,
                        data  : Union[str, dict, bytes],
                   ) -> bytes:                                              # Serialize data based on type

        if type(data) is bytes:                                             # Binary data
            return data
        elif type(data) is dict:
            return json_to_bytes(data)
        else:                                                               # String data
            data = json.dumps(data)                                        # convert to json   (strings are stored as json)
            return str_to_bytes(data)                                      # then convert to bytes