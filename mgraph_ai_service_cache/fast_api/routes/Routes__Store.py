from typing                                                                        import Literal
from fastapi                                                                       import Request, Body
from osbot_fast_api.api.routes.Fast_API__Routes                                    import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                         import Safe_Str__Fast_API__Route__Tag
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                  import Safe_Id
from mgraph_ai_service_cache.service.cache.Cache__Service                          import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response          import Schema__Cache__Store__Response

TAG__ROUTES_STORE                  = 'store/{strategy}/{namespace}'
ROUTES_PATHS__STORE                = [ f'/{TAG__ROUTES_STORE}' + '/as/binary' ,
                                       f'/{TAG__ROUTES_STORE}' + '/as/json'   ,
                                       f'/{TAG__ROUTES_STORE}' + '/as/string' ]

class Routes__Store(Fast_API__Routes):                                             # FastAPI routes for cache operations
    tag           : Safe_Str__Fast_API__Route__Tag  = TAG__ROUTES_STORE
    cache_service : Cache__Service

    def as__string(self, data: str = Body(...)                                                                          ,
                                          strategy  : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                          namespace : Safe_Id = None
                                     ) -> Schema__Cache__Store__Response:

        cache_hash = self.cache_service.hash_from_string(data)
        cache_id   = Random_Guid()

        return self.cache_service.store_with_strategy(cache_key_data = data      ,
                                                      storage_data   = data      ,
                                                      cache_hash     = cache_hash,
                                                      cache_id       = cache_id  ,
                                                      strategy       = strategy  ,
                                                      namespace      = namespace )

    def as__json(self, data            : dict                                                                               ,
                                          strategy        : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                          namespace       : Safe_Id = None
                                     ) -> Schema__Cache__Store__Response:
        cache_hash     = self.cache_service.hash_from_json(data)
        cache_id       = Random_Guid()

        # Store full JSON
        return self.cache_service.store_with_strategy(cache_key_data = data         ,       # todo: review this since the fact that we are storing the entire data could cause performance problems
                                                      storage_data   = data         ,
                                                      cache_hash     = cache_hash   ,
                                                      cache_id       = cache_id     ,
                                                      strategy       = strategy     ,
                                                      namespace      = namespace    )


    def as__binary(self, request  : Request                                                    ,
                                            body     : bytes = Body(..., media_type="application/octet-stream")   ,
                                            strategy: Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                            namespace: Safe_Id = None
                                       ) -> Schema__Cache__Store__Response:               # Store raw binary data with hash calculation
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


    def setup_routes(self):                                                     # Configure FastAPI routes
        self.add_route_post(self.as__string )                                   # String endpoints
        self.add_route_post(self.as__json   )                                   # JSON endpoints
        self.add_route_post(self.as__binary )                                   # Binary endpoints