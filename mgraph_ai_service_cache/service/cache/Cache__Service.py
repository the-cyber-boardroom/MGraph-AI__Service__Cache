from typing                                                                        import Dict, Optional, Any, List, Literal
from osbot_utils.type_safe.Type_Safe                                               import Type_Safe
from osbot_utils.utils.Json                                                        import json_to_str
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                 import Safe_Id
from osbot_utils.utils.Misc                                                        import timestamp_now
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                   import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Handler                          import Cache__Handler
from mgraph_ai_service_cache.service.cache.Cache__Hash__Config                     import Cache__Hash__Config
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator                  import Cache__Hash__Generator
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response          import Schema__Cache__Store__Response

DEFAULT__CACHE__SERVICE__BUCKET_NAME        = "mgraph-ai-cache"
DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS  = 24

class Cache__Service(Type_Safe):                                                   # Main cache service orchestrator
    cache_handlers    : Dict[Safe_Id, Cache__Handler]                              # Multiple cache handlers by namespace
    default_bucket    : str                           = DEFAULT__CACHE__SERVICE__BUCKET_NAME
    default_ttl_hours : int                           = DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
    hash_config       : Cache__Hash__Config                                        # Hash generation config
    hash_generator    : Cache__Hash__Generator                                     # Hash generator instance

    def get_or_create_handler(self, namespace: Safe_Id = None                      # Get existing or create new cache handler
                              ) -> Cache__Handler:
        namespace = namespace or Safe_Id("default")
        if namespace not in self.cache_handlers:
            handler = Cache__Handler(s3__bucket      = self.default_bucket,
                                     s3__prefix      = str(namespace),
                                     cache_ttl_hours = self.default_ttl_hours).setup()
            self.cache_handlers[namespace] = handler
        return self.cache_handlers[namespace]

    def store_with_strategy(self, cache_key_data   : Any                          ,  # Store data with clear separation
                                  storage_data     : Any                          ,
                                  cache_hash       : Safe_Str__Cache_Hash         ,
                                  cache_id         : Random_Guid                  ,
                                  strategy         : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"],
                                  namespace        : Safe_Id = None               ,
                                  content_encoding : str = None
                            ) -> Schema__Cache__Store__Response:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)
        fs_data   = handler.get_fs_for_strategy(strategy)

        if cache_key_data is dict:                                                  # if the cache_key_data is a dict
            cache_key_data = json_to_str(cache_key_data)                            #    convert it to str
        # Store actual data
        with fs_data.file__json(Safe_Id(str(cache_id))) as file_fs:
            paths = file_fs.create(storage_data)

            # Add metadata
            metadata = { "cache_hash"       : str(cache_hash)    ,
                         "cache_key_data"   : str(cache_key_data),
                         "cache_id"         : str(cache_id)      ,
                         "content_encoding" : content_encoding   ,
                         "stored_at"        : timestamp_now()    ,
                         "strategy"         : strategy           ,
                         "namespace"        : str(namespace)     }
            file_fs.metadata__update(metadata)
            file_size = file_fs.metadata().content__size

        # tood: move this update xrefs into different folder
        # Update hash->ID reference
        with handler.fs__refs_hash.file__json(Safe_Id(cache_hash)) as ref_fs:
            if ref_fs.exists():
                refs = ref_fs.content() # todo: we should be using a Type_Safe class here
                refs["cache_ids"     ].append({"id": str(cache_id), "timestamp": timestamp_now()})
                refs["latest_id"     ] = str(cache_id)
                refs["total_versions"] += 1
                paths__hash_to_id =  ref_fs.update(file_data=refs)
            else:
                refs = {"hash"           : str(cache_hash)                                     ,
                       "cache_ids"      : [{"id": str(cache_id), "timestamp": timestamp_now()}],
                       "latest_id"      : str(cache_id)                                        ,
                       "total_versions" : 1                                                    }
                paths__hash_to_id = ref_fs.create(file_data=refs)


        # Update ID->hash reference
        with handler.fs__refs_id.file__json(Safe_Id(str(cache_id))) as ref_fs:
            paths__id_to_hash =  ref_fs.create({ "cache_id"  : str(cache_id)   ,
                                                 "hash"       : str(cache_hash) ,
                                                 "timestamp"  : timestamp_now()  })
        paths.extend(paths__hash_to_id)                                         # todo: find a better solution to return these paths
        paths.extend(paths__id_to_hash)                                         #       see if it is ok to merge all these in this path field
        return Schema__Cache__Store__Response(cache_id   = cache_id     ,
                                              hash       = cache_hash   ,
                                              paths      = paths        ,
                                              size       = file_size    )

    def retrieve_by_hash(self, cache_hash : Safe_Str__Cache_Hash       ,                 # Retrieve latest by hash
                              namespace   : Safe_Id = None
                         ) -> Optional[Dict[str, Any]]:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        # Get hash->ID mapping
        with handler.fs__refs_hash.file__json(Safe_Id(cache_hash)) as ref_fs:
            if not ref_fs.exists():
                return None
            refs = ref_fs.content()
            latest_id = refs.get("latest_id")

        if not latest_id:
            return None

        # Get data using cache ID
        return self.retrieve_by_id(Random_Guid(latest_id), namespace)

    def retrieve_by_id(self, cache_id : Random_Guid              ,                 # Retrieve by cache ID
                            namespace : Safe_Id = None
                       ) -> Optional[Dict[str, Any]]:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        # Get ID->hash mapping to find strategy
        with handler.fs__refs_id.file__json(Safe_Id(str(cache_id))) as ref_fs:
            if not ref_fs.exists():
                return None
            ref_data = ref_fs.content()                         # todo: review the use of this variable since it not currently being used at the moment

        # Try each strategy to find the data
        for strategy in ["direct", "temporal", "temporal_latest", "temporal_versioned"]:
            fs_data = handler.get_fs_for_strategy(strategy)
            with fs_data.file__json(Safe_Id(str(cache_id))) as file_fs:
                if file_fs.exists():
                    data     = file_fs.content()
                    metadata = file_fs.metadata()
                    return {"data": data, "metadata": metadata.data if metadata else {}}

        return None

    def hash_from_string(self, data: str) -> Safe_Str__Cache_Hash:                       # Calculate hash from string
        return self.hash_generator.from_string(data)

    def hash_from_bytes(self, data: bytes) -> Safe_Str__Cache_Hash:                      # Calculate hash from bytes
        return self.hash_generator.from_bytes(data)

    def hash_from_json(self, data          : dict        ,                         # Calculate hash from JSON
                            exclude_fields : List[str] = None
                       ) -> Safe_Str__Cache_Hash:
        return self.hash_generator.from_json(data, exclude_fields)

    def list_namespaces(self) -> List[Safe_Id]:                                    # List all active namespaces
        return list(self.cache_handlers.keys())