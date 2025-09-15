import base64
import json
from typing                                                                        import Dict, Any, Literal
from fastapi                                                                       import Request, Response
from osbot_fast_api.api.routes.Fast_API__Routes                                    import Fast_API__Routes
from osbot_utils.utils.Json                                                        import str_to_json
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                   import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                 import Safe_Id
from mgraph_ai_service_cache.service.cache.Cache__Service                          import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response          import Schema__Cache__Store__Response

TAG__ROUTES_STORE                  = 'store'
ROUTES_PATHS__STORE                = [ f'/{TAG__ROUTES_STORE}' + '/store/binary/{strategy}/{namespace}'             ,
                                       f'/{TAG__ROUTES_STORE}' + '/store/json/{strategy}/{namespace}'               ,
                                       f'/{TAG__ROUTES_STORE}' + '/store/string/{strategy}/{namespace}'             ]

class Routes__Cache(Fast_API__Routes):                                             # FastAPI routes for cache operations
    tag           : str            = ROUTES_PATHS__STORE
    cache_service : Cache__Service

    def store__string__strategy__namespace(self, request   : Request,
                                                 strategy  : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                                 namespace : Safe_Id = None
                                            ) -> Schema__Cache__Store__Response:
        data = request.state.body.decode()

        cache_hash = self.cache_service.hash_from_string(data)
        cache_id   = Random_Guid()

        return self.cache_service.store_with_strategy(cache_key_data = data      ,
                                                      storage_data   = data      ,
                                                      cache_hash     = cache_hash,
                                                      cache_id       = cache_id  ,
                                                      strategy       = strategy  ,
                                                      namespace      = namespace )

    def store__json__strategy__namespace(self, request   : Request,
                                               #exclude_fields  : List[str] = None                         ,
                                               strategy        : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                               namespace       : Safe_Id = None
                   ) -> Schema__Cache__Store__Response:
        #exclude_fields = []
        # Calculate hash from filtered JSON
        #cache_key_json = {k: v for k, v in data.items() if k not in (exclude_fields or [])}
        data           = str_to_json(request.state.body.decode())
        cache_hash     = self.cache_service.hash_from_json(data)
        cache_id       = Random_Guid()

        # Store full JSON
        return self.cache_service.store_with_strategy(cache_key_data = data         ,       # todo: review this since the fact that we are storing the entire data could cause performance problems
                                                      storage_data   = data         ,
                                                      cache_hash     = cache_hash   ,
                                                      cache_id       = cache_id     ,
                                                      strategy       = strategy     ,
                                                      namespace      = namespace    )


    def store__binary__strategy__namespace(self, request: Request,
                                                 strategy: Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                                 namespace: Safe_Id = None
                                         ) -> Schema__Cache__Store__Response:               # Store raw binary data with hash calculation
        body = request.state.body

        # Check if compressed
        content_encoding = request.headers.get('content-encoding')
        if content_encoding == 'gzip':
            import gzip
            decompressed   = gzip.decompress(body)
            cache_hash     = self.cache_service.hash_from_bytes(decompressed)
            storage_data   = body                       # Store compressed
            cache_key_data = cache_hash                 # Use the hash of decompressed data as cache key
        else:
            cache_hash     = self.cache_service.hash_from_bytes(body)
            storage_data   = body
            cache_key_data = cache_hash                 # Store hash instead of raw binary

        cache_id = Random_Guid()

        return self.cache_service.store_with_strategy(cache_key_data   = str(cache_key_data),  # Convert hash to string for storage
                                                      storage_data     = storage_data,
                                                      cache_hash       = cache_hash,
                                                      cache_id         = cache_id,
                                                      strategy         = strategy,
                                                      namespace        = namespace,
                                                      content_encoding = content_encoding)


    def setup_routes(self):                                                         # Configure FastAPI routes
        self.add_route_post(self.store__string__strategy__namespace)                # String endpoints
        self.add_route_post(self.store__json__strategy__namespace)                  # JSON endpoints
        self.add_route_post(self.store__binary__strategy__namespace)                # Binary endpoints