from typing                                                                       import Literal
from fastapi                                                                      import Request, Body
from osbot_fast_api.api.decorators.route_path                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                   import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                     import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                        import Safe_Str__Fast_API__Route__Tag
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                  import DEFAULT_CACHE__STORE__STRATEGY, DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy     import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                         import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response         import Schema__Cache__Store__Response

TAG__ROUTES_STORE                  = 'store'
PREFIX__ROUTES_STORE               = '/{namespace}/{strategy}'
ROUTES_PATHS__STORE                = [ f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'binary'                 ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'binary/{cache_key:path}',
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'json'                   ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'json/{cache_key:path}',
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'string'                 ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'string/{cache_key:path}']

class Routes__Store(Fast_API__Routes):                                             # FastAPI routes for cache operations
    tag           : Safe_Str__Fast_API__Route__Tag  = TAG__ROUTES_STORE
    prefix        : Safe_Str__Fast_API__Route__Prefix  = PREFIX__ROUTES_STORE
    cache_service : Cache__Service

    def store__string(self, data      : str = Body(...)                                        ,
                            strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,          # todo: see the best way to handle/document this, since on Swagger this default value is not shown (because it on a path, but will be good to double check if we can set it anyway)
                            namespace : Safe_Str__Id                      = None
                       ) -> Schema__Cache__Store__Response:

        cache_hash = self.cache_service.hash_from_string(data)
        cache_id   = Random_Guid()

        return self.cache_service.store_with_strategy(storage_data   = data      ,           # todo: refactor to Type_Safe class
                                                      cache_hash     = cache_hash,
                                                      cache_id       = cache_id  ,
                                                      strategy       = strategy  ,
                                                      namespace      = namespace )

    @route_path("/store/string/{cache_key:path}")
    def store__string__cache_key(self, data      : str = Body(...)                                              ,
                                       namespace : Safe_Str__Id                       = None                          ,
                                       strategy  : Enum__Cache__Store__Strategy  = DEFAULT_CACHE__STORE__STRATEGY,          # todo: see the best way to handle/document this, since on Swagger this default value is not shown (because it on a path, but will be good to double check if we can set it anyway)
                                       cache_key  : Safe_Str__File__Path         = None                          ,
                                       file_id    : Safe_Str__Id                 = None                                     # using Safe_Str__Id since it supports None (Safe_Str__Id will create a default value, this way this file_id, when None, will eventually be assigned the cache_id)
                                  ) -> Schema__Cache__Store__Response:

        cache_hash = self.cache_service.hash_from_string(cache_key)

        return self.cache_service.store_with_strategy(storage_data   = data      ,          # todo: refactor to Type_Safe class
                                                      cache_hash     = cache_hash,
                                                      file_id        = file_id   ,
                                                      cache_key      = cache_key ,
                                                      strategy       = strategy  ,
                                                      namespace      = namespace )


    def store__json(self, data      : dict                                                         ,
                          strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                          namespace : Safe_Str__Id                      = None
                     ) -> Schema__Cache__Store__Response:
        cache_hash     = self.cache_service.hash_from_json(data)
        cache_id       = Random_Guid()

        # Store full JSON
        return self.cache_service.store_with_strategy(storage_data   = data         ,
                                                      cache_hash     = cache_hash   ,
                                                      cache_id       = cache_id     ,
                                                      strategy       = strategy     ,
                                                      namespace      = namespace    )


    @route_path("/store/json/{cache_key:path}")
    def store__json__cache_key(self, data       : dict = Body(...)                                              ,
                                     namespace  : Safe_Str__Id                       = None                          ,
                                     strategy   : Enum__Cache__Store__Strategy  = DEFAULT_CACHE__STORE__STRATEGY,          # todo: see the best way to handle/document this, since on Swagger this default value is not shown (because it on a path, but will be good to double check if we can set it anyway)
                                     cache_key  : Safe_Str__File__Path         = None                          ,
                                     file_id    : Safe_Str__Id                 = None                                     # using Safe_Str__Id since it supports None (Safe_Str__Id will create a default value, this way this file_id, when None, will eventually be assigned the cache_id)
                               ) -> Schema__Cache__Store__Response:

        cache_hash = self.cache_service.hash_from_string(cache_key)

        return self.cache_service.store_with_strategy(storage_data   = data      ,          # todo: refactor to Type_Safe class
                                                      cache_hash     = cache_hash,
                                                      file_id        = file_id   ,
                                                      cache_key      = cache_key ,
                                                      strategy       = strategy  ,
                                                      namespace      = namespace )
    def store__binary(self, request  : Request                                                    ,
                            body     : bytes = Body(..., media_type="application/octet-stream")   ,
                            strategy: Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                            namespace: Safe_Str__Id = None
                       ) -> Schema__Cache__Store__Response:               # Store raw binary data with hash calculation
        # Check if compressed
        content_encoding = request.headers.get('content-encoding')
        if content_encoding == 'gzip':
            import gzip
            decompressed   = gzip.decompress(body)
            cache_hash     = self.cache_service.hash_from_bytes(decompressed)
            storage_data   = body                       # Store compressed
        else:
            cache_hash     = self.cache_service.hash_from_bytes(body)
            storage_data   = body

        cache_id = Random_Guid()

        return self.cache_service.store_with_strategy(storage_data     = storage_data,
                                                      cache_hash       = cache_hash,
                                                      cache_id         = cache_id,
                                                      strategy         = strategy,
                                                      namespace        = namespace,
                                                      content_encoding = content_encoding)

    @route_path("/store/binary/{cache_key:path}")
    def store__binary__cache_key(self, body       : bytes = Body(..., media_type="application/octet-stream")     ,
                                       namespace  : Safe_Str__Id                       = None                          ,
                                       strategy   : Enum__Cache__Store__Strategy  = DEFAULT_CACHE__STORE__STRATEGY,          # todo: see the best way to handle/document this, since on Swagger this default value is not shown (because it on a path, but will be good to double check if we can set it anyway)
                                       cache_key  : Safe_Str__File__Path         = None                          ,
                                       file_id    : Safe_Str__Id                 = None                                     # using Safe_Str__Id since it supports None (Safe_Str__Id will create a default value, this way this file_id, when None, will eventually be assigned the cache_id)
                                 ) -> Schema__Cache__Store__Response:

        cache_hash = self.cache_service.hash_from_string(cache_key)

        return self.cache_service.store_with_strategy(storage_data   = body      ,          # todo: refactor to Type_Safe class
                                                      cache_hash     = cache_hash,
                                                      file_id        = file_id   ,
                                                      cache_key      = cache_key ,
                                                      strategy       = strategy  ,
                                                      namespace      = namespace )
    def setup_routes(self):                                                     # Configure FastAPI routes
        self.add_route_post(self.store__string           )                      # String endpoints
        self.add_route_post(self.store__string__cache_key)
        self.add_route_post(self.store__json             )                      # JSON endpoints
        self.add_route_post(self.store__json__cache_key  )
        self.add_route_post(self.store__binary           )                      # Binary endpoints
        self.add_route_post(self.store__binary__cache_key)