from typing                                                                        import Dict, Any, List, Literal
from fastapi                                                                       import Request
from osbot_fast_api.api.routes.Fast_API__Routes                                    import Fast_API__Routes
from osbot_utils.utils.Json                                                        import str_to_json
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                   import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                 import Safe_Id
from mgraph_ai_service_cache.service.cache.Cache__Service                          import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response          import Schema__Cache__Store__Response

TAG__ROUTES_CACHE                  = 'cache'
ROUTES_PATHS__CACHE                = [  f'/{TAG__ROUTES_CACHE}' + '/delete/by-id/{cache_id}/{namespace}'    ,
                                        f'/{TAG__ROUTES_CACHE}' + '/exists/{cache_hash}/{namespace}'        ,
                                       f'/{TAG__ROUTES_CACHE}' + '/store/binary/{strategy}/{namespace}'     ,
                                       f'/{TAG__ROUTES_CACHE}' + '/store/json/{strategy}/{namespace}'       ,
                                       f'/{TAG__ROUTES_CACHE}' + '/store/string/{strategy}/{namespace}'     ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/by-hash/{cache_hash}/{namespace}' ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/by-id/{cache_id}/{namespace}'   ,

                                       f'/{TAG__ROUTES_CACHE}/stats'                                  ,
                                       #f'/{TAG__ROUTES_CACHE}/retrieve'                               ,
                                       #f'/{TAG__ROUTES_CACHE}/retrieve-by-hash'                       ,
                                       f'/{TAG__ROUTES_CACHE}/namespaces'                             ]

class Routes__Cache(Fast_API__Routes):                                             # FastAPI routes for cache operations
    tag           : str            = TAG__ROUTES_CACHE
    cache_service : Cache__Service

    def delete__by_id__cache_id__namespace(self, cache_id: Random_Guid,
                                                 namespace: Safe_Id = None) -> Dict[str, Any]:
        return self.cache_service.delete_by_id(cache_id, namespace)

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

    def store__binary__strategy__namespace(self, request   : Request                              ,  # Store raw binary data
                                                 strategy    : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
                                                 namespace   : Safe_Id = None
                                             ) -> Schema__Cache__Store__Response:
        body = request.state.body

        # Check if compressed
        content_encoding = request.headers.get('content-encoding')
        if content_encoding == 'gzip':
            import gzip
            decompressed = gzip.decompress(body)
            cache_hash   = self.cache_service.hash_from_bytes(decompressed)
            storage_data = body                                                    # Store compressed
        else:
            cache_hash   = self.cache_service.hash_from_bytes(body)
            storage_data = body

        cache_id = Random_Guid()
        return self.cache_service.store_with_strategy(
            #cache_key_data   = body if not content_encoding else decompressed,         # todo: on the topic of the cache_key_data, this one doesn't make sense at all (i.e. storing the binary data)
            cache_key_data   = None                                          ,          # todo: see side effects of setting this to None
            storage_data     = storage_data                                  ,
            cache_hash       = cache_hash                                    ,
            cache_id         = cache_id                                      ,
            strategy         = strategy                                      ,
            namespace        = namespace                                     ,
            content_encoding = content_encoding                              )

    # todo: this should not be done like this (we should be able to get from the headers the info if the json data is compressed
    # async def store_json_compressed(self, request        : Request                ,  # Store compressed JSON
    #                                      exclude_fields  : List[str] = None       ,
    #                                      strategy        : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"] = "temporal",
    #                                      namespace       : Safe_Id = None
    #                                 ) -> Schema__Cache__Store__Response:
    #     import gzip
    #     import json
    #
    #     body         = await request.body()
    #     decompressed = gzip.decompress(body)
    #     data         = json.loads(decompressed)
    #
    #     # Calculate hash from filtered, uncompressed JSON
    #     cache_key_json = {k: v for k, v in data.items() if k not in (exclude_fields or [])}
    #     cache_hash     = self.cache_service.hash_from_json(cache_key_json)
    #     cache_id       = Random_Guid()
    #
    #     # Store compressed version
    #     return self.cache_service.store_with_strategy(cache_key_data   = cache_key_json,
    #                                                   storage_data     = body          ,  # Store original compressed
    #                                                   cache_hash       = cache_hash    ,
    #                                                   cache_id         = cache_id      ,
    #                                                   strategy         = strategy      ,
    #                                                   namespace        = namespace     ,
    #                                                   content_encoding = 'gzip'         )

    def retrieve__by_hash__cache_hash__namespace(self, cache_hash      : Safe_Str__Cache_Hash                         ,  # Retrieve latest by hash
                                                       namespace  : Safe_Id = None
                                                  ) -> Dict[str, Any]:
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}
        return result

    def retrieve__by_id__cache_id__namespace(self, cache_id  : Random_Guid                              ,  # Retrieve by cache ID
                                                   namespace  : Safe_Id = None
                                              ) -> Dict[str, Any]:
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}
        return result

    def hash_calculate(self, data          : str = None                           ,  # Calculate hash from provided data
                             json_data      : dict = None                          ,
                             exclude_fields : List[str] = None
                        ) -> Dict[str, str]:
        if data:
            hash_value = self.cache_service.hash_from_string(data)
        elif json_data:
            hash_value = self.cache_service.hash_from_json(json_data, exclude_fields)
        else:
            return {"error": "No data provided"}

        return {"hash": str(hash_value)}

    def exists__cache_hash__namespace(self, cache_hash      : Safe_Str__Cache_Hash                                   ,  # Check if hash exists
                                            namespace  : Safe_Id = None
                                       ) -> Dict[str, bool]:
        namespace = namespace or Safe_Id("default")
        handler   = self.cache_service.get_or_create_handler(namespace)

        with handler.fs__refs_hash.file__json(Safe_Id(str(cache_hash))) as ref_fs:
            exists = ref_fs.exists()

        return {"exists": exists, "hash": str(cache_hash)}

    def namespaces(self) -> Dict[str, Any]:                                        # List all active namespaces
        namespaces = self.cache_service.list_namespaces()
        return {"namespaces": [str(ns) for ns in namespaces], "count": len(namespaces)}

    def stats(self, namespace: Safe_Id = None                                      # Get cache statistics
              ) -> Dict[str, Any]:
        namespace = namespace or Safe_Id("default")
        handler   = self.cache_service.get_or_create_handler(namespace)

        try:
            # Count files in each strategy
            stats = {"namespace"     : str(namespace)       ,
                    "s3_bucket"     : handler.s3__bucket   ,
                    "s3_prefix"     : handler.s3__prefix   ,
                    "ttl_hours"     : handler.cache_ttl_hours}

            # Add file counts for each strategy
            for strategy in ["direct", "temporal", "temporal_latest", "temporal_versioned"]:
                fs = handler.get_fs_for_strategy(strategy)
                if fs and fs.storage_fs:
                    file_count = len(fs.storage_fs.files__paths())
                    stats[f"{strategy}_files"] = file_count

            return stats
        except Exception as e:
            return {"error": str(e), "namespace": str(namespace)}

    def setup_routes(self):                                                        # Configure FastAPI routes
        # String endpoints
        self.add_route_post(self.store__string__strategy__namespace)

        # JSON endpoints
        self.add_route_post(self.store__json__strategy__namespace)
        #self.add_route_post(self.store_json_compressed, path="/cache/store/json/compressed")

        # Binary endpoints
        self.add_route_post(self.store__binary__strategy__namespace)

        # Retrieval endpoints
        self.add_route_get(self.retrieve__by_hash__cache_hash__namespace) #, path="/cache/retrieve/hash/{hash}")
        self.add_route_get(self.retrieve__by_id__cache_id__namespace    ) # , path="/cache/retrieve/id/{cache_id}")

        # Utility endpoints
        #self.add_route_post(self.hash_calculate, path="/cache/hash/calculate")
        self.add_route_get   (self.exists__cache_hash__namespace   )
        self.add_route_get   (self.namespaces                      )
        self.add_route_get   (self.stats                           )
        self.add_route_delete(self.delete__by_id__cache_id__namespace)

    # todo: remove this method when the next version of OSBot-Fast-API has been installed (which uses the code below)
    def add_route_delete(self, function):
        return self.add_route_with_body(function, methods=['DELETE'])