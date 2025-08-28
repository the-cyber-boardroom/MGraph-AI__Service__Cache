from typing                                                                         import Dict, Optional, Any, List
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import Safe_Str__Hash, SIZE__VALUE_HASH
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                      import type_safe
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import bytes_sha256, timestamp_now
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request            import Schema__Cache__Store__Request
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Request         import Schema__Cache__Retrieve__Request

DEFAULT__CACHE__SERVICE__BUCKET_NAME        = "mgraph-ai-cache"
DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS  = 24

class Cache__Service(Type_Safe):                                                                    # Main cache service orchestrator
    cache_handlers   : Dict[Safe_Id, Cache__Handler]                                                # Multiple cache handlers by namespace
    default_bucket   : str                           = DEFAULT__CACHE__SERVICE__BUCKET_NAME         # Default S3 bucket
    default_ttl_hours: int                           = DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS   # Default TTL in hours

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
            cache_key = str(data)                                                       # todo: check the performance of this on very large data entries (for example when handing zip files)
        hash_value = bytes_sha256(cache_key.encode())[:SIZE__VALUE_HASH]
        return Safe_Str__Hash(hash_value)

    @type_safe
    def store(self, request   : Schema__Cache__Store__Request ,                         # Store data in cache
                    namespace : Safe_Id = None
              ) -> Schema__Cache__Store__Response:                                      # todo: check the performance of this on very large data entries (for example when handing zip files): question is there a good way to handle raw bytes for storage
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        if request.hash:                                                                # Use provided hash or generate one
            hash_value = request.hash                                                   # todo: see if it is a good idea to get the hash from the submitted (this could introduce some bugs)
        else:
            hash_value = self.generate_cache_hash(request.data)

        cache_id    = Random_Guid()
        file_id     = Safe_Id(cache_id)
        cache_entry = { 'data'         : request.data                             ,   # Build cache entry       # todo: this should be Type_Safe class
                        'content_type' : str(request.content_type)                ,
                        'metadata'     : request.metadata or {}                   ,
                        'tags'         : request.tags or []                       ,
                        'stored_at'    : timestamp_now()                          ,
                        'ttl_hours'    : handler.cache_ttl_hours                  ,
                        'namespace'    : str(namespace)                           ,
                        'hash'         : str(hash_value)                          }

        with handler.fs__latest_temporal.file__json(file_id) as file_fs:             # Store using Memory-FS
            paths = file_fs.create(cache_entry)

            if request.metadata:                                                       # Update metadata if provided
                metadata_dict = {Safe_Id(k): v for k, v in request.metadata.items()}
                file_fs.metadata__update(metadata_dict)

            return Schema__Cache__Store__Response( cache_id = cache_id                              ,
                                                   hash     = hash_value                            ,
                                                   path     = paths[0] if paths else ""             ,
                                                   size     = len(json_to_str(cache_entry).encode()))

    @type_safe
    def retrieve(self, request   : Schema__Cache__Retrieve__Request ,                 # Retrieve data from cache
                       namespace : Safe_Id = None
                 ) -> Optional[Dict[str, Any]]:                                         # todo: this needs to be Type_Safe class
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        if request.hash:                                                              # Determine cache ID from hash or use provided ID
            return dict(status='error', message='retrieval by hash not supported')     # todo: add a mode to convert cache_hash to cache id

        cache_id = request.cache_id
        file_id  = Safe_Id(cache_id)

        with handler.fs__latest_temporal.file__json(file_id) as file_fs:
            if not file_fs.exists():
                return dict(status='error', message='cache entry not found')

            result = {}

            if request.include_data:
                cache_entry = file_fs.content()
                result['data'] = cache_entry if cache_entry else None

            if request.include_metadata:
                metadata = file_fs.metadata()
                result['metadata'] = metadata.data if metadata else {}

            if request.include_config:
                config = file_fs.config()
                result['config'] = config.json() if config else {}

            return result                                                               # todo: this needs to be Type_Safe class

    @type_safe
    def list_namespaces(self) -> List[Safe_Id]:                                       # List all active namespaces
        return list(self.cache_handlers.keys())