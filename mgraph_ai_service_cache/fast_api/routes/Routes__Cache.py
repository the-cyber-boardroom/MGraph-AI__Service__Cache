from typing                                                                       import Dict, Any, Optional
from osbot_fast_api.api.routes.Fast_API__Routes                                   import Fast_API__Routes
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash import Safe_Str__Hash
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                import Safe_Id
from mgraph_ai_service_cache.service.cache.Cache__Service                         import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request          import Schema__Cache__Store__Request
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response         import Schema__Cache__Store__Response
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Request       import Schema__Cache__Retrieve__Request

TAG__ROUTES_CACHE                  = 'cache'
ROUTES_PATHS__CACHE                = [f'/{TAG__ROUTES_CACHE}/store'             ,
                                      f'/{TAG__ROUTES_CACHE}/stats'             ,
                                      f'/{TAG__ROUTES_CACHE}/retrieve'          ,
                                      f'/{TAG__ROUTES_CACHE}/retrieve-by-hash'  ,
                                      f'/{TAG__ROUTES_CACHE}/namespaces'        ]
class Routes__Cache(Fast_API__Routes):                                                 # FastAPI routes for cache operations
    tag           : str            = TAG__ROUTES_CACHE
    cache_service : Cache__Service

    def store(self, request   : Schema__Cache__Store__Request ,                       # Store data in cache
                    namespace : Safe_Id = None
              ) -> Schema__Cache__Store__Response:
        namespace = namespace or Safe_Id("default")
        return self.cache_service.store(request, namespace)

    def retrieve(self, cache_id  : Safe_Id               = None ,                           # Retrieve cache entry by ID or hash
                       hash      : Safe_Str__Hash = None ,
                       namespace : Safe_Id               = None
                 ) -> Dict[str, Any]:
        request = Schema__Cache__Retrieve__Request( cache_id         = cache_id ,
                                                    hash            = hash      ,
                                                    include_data    = True      ,
                                                    include_metadata= True      ,
                                                    include_config  = False     )

        result = self.cache_service.retrieve(request, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}
        return result

    def retrieve_by_hash(self, hash      : Safe_Str__Hash ,                           # Retrieve cache entry by hash
                               namespace : Safe_Id = None
                         ) -> Dict[str, Any]:
        return self.retrieve(cache_id=None, hash=hash, namespace=namespace)

    def namespaces(self) -> Dict[str, Any]:                                           # List all active namespaces
        namespaces = self.cache_service.list_namespaces()
        return {"namespaces": [str(ns) for ns in namespaces], "count": len(namespaces)}

    def stats(self, namespace: Safe_Id = None                                         # Get cache statistics for namespace
              ) -> Dict[str, Any]:
        namespace = namespace or Safe_Id("default")
        handler   = self.cache_service.get_or_create_handler(namespace)

        try:
            file_paths = handler.s3__storage.files__paths()
            total_files = len(file_paths)

            stats = { "namespace"    : str(namespace)                             ,
                      "total_entries": total_files                               ,
                      "s3_bucket"   : handler.s3__bucket                        ,
                      "s3_prefix"   : handler.s3__prefix                        ,
                      "ttl_hours"   : handler.cache_ttl_hours                   }

            return stats
        except Exception as e:
            return {"error": str(e), "namespace": str(namespace)}

    def setup_routes(self):                                                           # Configure FastAPI routes
        self.add_route_post(self.store             )
        self.add_route_get (self.retrieve          )
        self.add_route_get (self.retrieve_by_hash  )
        self.add_route_get (self.namespaces        )
        self.add_route_get (self.stats             )