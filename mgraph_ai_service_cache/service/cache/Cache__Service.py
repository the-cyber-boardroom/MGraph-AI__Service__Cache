from typing                                                                         import Dict, Optional, Any, List
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import Safe_Str__Hash
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                      import type_safe
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import bytes_sha256, timestamp_now
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request            import Schema__Cache__Store__Request
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Request         import Schema__Cache__Retrieve__Request
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__SHA1__Short                   import Safe_Str__SHA1__Short


class Cache__Service(Type_Safe):                                                      # Main cache service orchestrator
    cache_handlers   : Dict[Safe_Id, Cache__Handler]                                  # Multiple cache handlers by namespace
    default_bucket   : str                           = "mgraph-ai-cache"              # Default S3 bucket
    default_ttl_hours: int                           = 24                             # Default TTL in hours

    def setup(self) -> 'Cache__Service':                                              # Initialize cache service
        if not self.cache_handlers:
            self.cache_handlers = {}
        return self

    @type_safe
    def get_or_create_handler(self, namespace : Safe_Id                              # Get existing or create new cache handler
                              ) -> Cache__Handler:
        if namespace not in self.cache_handlers:
            handler = Cache__Handler( s3__bucket      = self.default_bucket        ,
                                     s3__prefix      = str(namespace)             ,
                                     cache_ttl_hours = self.default_ttl_hours     ).setup()
            self.cache_handlers[namespace] = handler
        return self.cache_handlers[namespace]

    @type_safe
    def generate_cache_hash(self, data: Any                                           # Generate deterministic hash from data
                            ) -> Safe_Str__Hash:
        if isinstance(data, dict):
            cache_key = json_to_str(data)
        else:
            cache_key = str(data)
        hash_value = bytes_sha256(cache_key.encode())[:SIZE__VALUE_HASH]
        return Safe_Str__Hash(hash_value)

    @type_safe
    def generate_cache_id_short(self, hash_value: Safe_Str__Hash                      # Generate short cache ID from hash
                                ) -> Safe_Str__SHA1__Short:
        return Safe_Str__SHA1__Short(str(hash_value)[:7])

    @type_safe
    def store(self, request   : Schema__Cache__Store__Request ,                       # Store data in cache
                    namespace : Safe_Id = None
              ) -> Schema__Cache__Store__Response:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        if request.hash:                                                              # Use provided hash or generate one
            hash_value = Safe_Str__Hash(str(request.hash))
        else:
            hash_value = self.generate_cache_hash(request.data)

        cache_id_short = self.generate_cache_id_short(hash_value)
        cache_id       = Safe_Id(str(cache_id_short))

        cache_entry = { 'data'         : request.data                             ,   # Build cache entry
                        'content_type' : str(request.content_type)                ,
                        'metadata'     : request.metadata or {}                   ,
                        'tags'         : request.tags or []                       ,
                        'stored_at'    : timestamp_now()                          ,
                        'ttl_hours'    : handler.cache_ttl_hours                  ,
                        'namespace'    : str(namespace)                           ,
                        'hash'         : str(hash_value)                          }

        with handler.fs__latest_temporal.file__json(cache_id) as file_fs:             # Store using Memory-FS
            paths = file_fs.create(cache_entry)

            if request.metadata:                                                       # Update metadata if provided
                metadata_dict = {Safe_Id(k): v for k, v in request.metadata.items()}
                file_fs.metadata__update(metadata_dict)

            version = 1                                                                # TODO: implement versioning logic

            return Schema__Cache__Store__Response( cache_id = cache_id                              ,
                                                   hash     = hash_value                            ,
                                                   version  = version                                ,
                                                   path     = paths[0] if paths else ""             ,
                                                   size     = len(json_to_str(cache_entry).encode()))

    @type_safe
    def retrieve(self, request   : Schema__Cache__Retrieve__Request ,                 # Retrieve data from cache
                       namespace : Safe_Id = None
                 ) -> Optional[Dict[str, Any]]:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        if request.hash:                                                              # Determine cache ID from hash or use provided ID
            cache_id = Safe_Id(str(self.generate_cache_id_short(Safe_Str__Hash(str(request.hash)))))
        elif request.cache_id:
            cache_id = request.cache_id
        else:
            return None

        with handler.fs__latest_temporal.file__json(cache_id) as file_fs:
            if not file_fs.exists():
                return None

            result = {}

            if request.include_data:
                cache_entry = file_fs.content()
                result['data'] = cache_entry.get('data') if cache_entry else None

            if request.include_metadata:
                metadata = file_fs.metadata()
                result['metadata'] = metadata.data if metadata else {}

            if request.include_config:
                config = file_fs.config()
                result['config'] = config.json() if config else {}

            return result

    @type_safe
    def list_namespaces(self) -> List[Safe_Id]:                                       # List all active namespaces
        return list(self.cache_handlers.keys())