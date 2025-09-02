import base64
import json
from typing                                                                        import Dict, Any, Literal
from fastapi                                                                       import Request, Response
from osbot_fast_api.api.routes.Fast_API__Routes                                    import Fast_API__Routes
from osbot_utils.utils.Json                                                        import str_to_json
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                   import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                 import Safe_Id
from mgraph_ai_service_cache.service.cache.Cache__Service                          import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response          import Schema__Cache__Store__Response

TAG__ROUTES_CACHE                  = 'cache'
ROUTES_PATHS__CACHE                = [ f'/{TAG__ROUTES_CACHE}' + '/delete/by-id/{cache_id}/{namespace}'             ,
                                       f'/{TAG__ROUTES_CACHE}' + '/exists/{cache_hash}/{namespace}'                 ,
                                       f'/{TAG__ROUTES_CACHE}' + '/store/binary/{strategy}/{namespace}'             ,
                                       f'/{TAG__ROUTES_CACHE}' + '/store/json/{strategy}/{namespace}'               ,
                                       f'/{TAG__ROUTES_CACHE}' + '/store/string/{strategy}/{namespace}'             ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/by-hash/{cache_hash}/{namespace}'       ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/by-id/{cache_id}/{namespace}'           ,
                                       f'/{TAG__ROUTES_CACHE}' + '/stats/namespaces/{namespace}'                    ,
                                       f'/{TAG__ROUTES_CACHE}' + '/stats/namespaces'                                ,
                                       f'/{TAG__ROUTES_CACHE}' + '/namespaces'                                      ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/string/by-id/{cache_id}/{namespace}'    ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/json/by-id/{cache_id}/{namespace}'      ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/binary/by-id/{cache_id}/{namespace}'    ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/string/by-hash/{cache_hash}/{namespace}',
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/json/by-hash/{cache_hash}/{namespace}'  ,
                                       f'/{TAG__ROUTES_CACHE}' + '/retrieve/binary/by-hash/{cache_hash}/{namespace}']

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

    # def hash_calculate(self, data          : str = None                           ,  # Calculate hash from provided data
    #                          json_data      : dict = None                          ,
    #                          exclude_fields : List[str] = None
    #                     ) -> Dict[str, str]:
    #     if data:
    #         hash_value = self.cache_service.hash_from_string(data)
    #     elif json_data:
    #         hash_value = self.cache_service.hash_from_json(json_data, exclude_fields)
    #     else:
    #         return {"error": "No data provided"}
    #
    #     return {"hash": str(hash_value)}

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

    def stats__namespaces__namespace(self, namespace: Safe_Id = None) -> Dict[str, Any]:       # Get cache statistics
        namespace = namespace or Safe_Id("default")

        try:
            # Get file counts using shared method
            counts_data = self.cache_service.get_namespace_file_counts(namespace)
            handler = counts_data['handler']

            # Build stats response
            stats = { "namespace": str(namespace)         ,
                      "s3_bucket": handler.s3__bucket     ,
                      "s3_prefix": handler.s3__prefix     ,
                      "ttl_hours": handler.cache_ttl_hours,
                      **counts_data['file_counts'        ]}  # Spread all the file counts

            return stats
        except Exception as e:
            return {"error": str(e), "namespace": str(namespace)}

    def stats__namespaces(self):
        return self.cache_service.list_namespaces()

    def retrieve__by_hash__cache_hash__namespace(self, cache_hash : Safe_Str__Cache_Hash,
                                                       namespace  : Safe_Id = None
                                                  ) -> Dict[str, Any]:                          # Retrieve latest by hash with type information"""
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        if result.get("data_type") == "binary":                                                 # If data is binary, don't return it in JSON - direct to binary endpoint
            return { "status"      : "binary_data"                                                          ,
                     "message"     : "Binary data cannot be returned in JSON response"                      ,
                     "data_type"   : "binary"                                                               ,
                     "size"        : len(result.get("data", b""))                                           ,
                     "metadata"    : result.get("metadata", {})                                             ,
                     "binary_url"  : f"/cache/retrieve/binary/by-hash/{cache_hash}/{namespace or 'default'}"}

        return result

    def retrieve__by_id__cache_id__namespace(self, cache_id  : Random_Guid,
                                                   namespace : Safe_Id = None
                                              ) -> Dict[str, Any]:              # Retrieve by cache ID with type information
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}


        if result.get("data_type") == "binary":                                 # If data is binary, don't return it in JSON - direct to binary endpoint
            return { "status"      : "binary_data"                                                      ,
                     "message"     : "Binary data cannot be returned in JSON response"                  ,
                     "data_type"   : "binary"                                                           ,
                     "size"        : len(result.get("data", b""))                                       ,
                     "metadata"    : result.get("metadata", {})                                         ,
                     "binary_url"  : f"/cache/retrieve/binary/by-id/{cache_id}/{namespace or 'default'}" }

        return result

    # New type-specific retrieval methods
    def retrieve__string__by_id__cache_id__namespace(self, cache_id: Random_Guid,
                                                    namespace: Safe_Id = None
                                                    ) -> str:
        """Retrieve as string by cache ID"""
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return Response(content="Not found", status_code=404)

        data = result.get("data")
        data_type = result.get("data_type")

        if data_type == "string":
            return Response(content=data, media_type="text/plain")
        elif data_type == "json":
            import json
            return Response(content=json.dumps(data), media_type="text/plain")
        elif data_type == "binary":
            # Convert binary to string (might not be ideal for all binary data)
            try:
                return Response(content=data.decode('utf-8'), media_type="text/plain")
            except:
                # If can't decode, return base64 encoded
                return Response(content=base64.b64encode(data).decode('utf-8'),
                              media_type="text/plain")

        return Response(content=str(data), media_type="text/plain")

    def retrieve__json__by_id__cache_id__namespace(self, cache_id: Random_Guid,
                                                  namespace: Safe_Id = None
                                                  ) -> Dict[str, Any]:
        """Retrieve as JSON by cache ID"""
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        data = result.get("data")
        data_type = result.get("data_type")

        if data_type == "json":
            return data
        elif data_type == "string":
            # Try to parse string as JSON
            try:
                import json
                return json.loads(data)
            except:
                return {"error": "Data is not valid JSON", "data": data}
        elif data_type == "binary":
            # Return base64 encoded binary in JSON wrapper
            return {
                "data_type": "binary",
                "encoding": "base64",
                "data": base64.b64encode(data).decode('utf-8')
            }

        return {"data": data, "data_type": data_type}

    def retrieve__binary__by_id__cache_id__namespace(self, cache_id: Random_Guid,
                                                    namespace: Safe_Id = None):     # Retrieve as binary by cache ID
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return Response(content="Not found", status_code=404)

        data      = result.get("data")
        data_type = result.get("data_type")

        if data_type == "binary":
            return Response(content=data,
                            media_type="application/octet-stream")          # Return raw binary data (already decompressed by service)
        elif data_type == "string":

            return Response(content=data.encode('utf-8'),
                            media_type="application/octet-stream")          # Convert string to bytes
        elif data_type == "json":

            return Response(content=json.dumps(data).encode('utf-8'),
                            media_type="application/octet-stream")          # Convert JSON to bytes

        return Response(content=str(data).encode('utf-8'),
                      media_type="application/octet-stream")                # Fallback

    def retrieve__string__by_hash__cache_hash__namespace(self, cache_hash: Safe_Str__Cache_Hash,
                                                        namespace: Safe_Id = None):
        """Retrieve as string by hash"""
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return Response(content="Not found", status_code=404)

        data = result.get("data")
        data_type = result.get("data_type")

        if data_type == "string":
            return Response(content=data, media_type="text/plain")
        elif data_type == "json":
            import json
            return Response(content=json.dumps(data), media_type="text/plain")
        elif data_type == "binary":
            try:
                return Response(content=data.decode('utf-8'), media_type="text/plain")
            except:
                return Response(content=base64.b64encode(data).decode('utf-8'),
                              media_type="text/plain")

        return Response(content=str(data), media_type="text/plain")

    def retrieve__json__by_hash__cache_hash__namespace(self, cache_hash: Safe_Str__Cache_Hash,
                                                      namespace: Safe_Id = None
                                                      ) -> Dict[str, Any]:
        """Retrieve as JSON by hash"""
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        data = result.get("data")
        data_type = result.get("data_type")

        if data_type == "json":
            return data
        elif data_type == "string":
            try:
                import json
                return json.loads(data)
            except:
                return {"error": "Data is not valid JSON", "data": data}
        elif data_type == "binary":
            return {
                "data_type": "binary",
                "encoding": "base64",
                "data": base64.b64encode(data).decode('utf-8')
            }

        return {"data": data, "data_type": data_type}

    def retrieve__binary__by_hash__cache_hash__namespace(self, cache_hash: Safe_Str__Cache_Hash,
                                                        namespace: Safe_Id = None):
        """Retrieve as binary by hash"""
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return Response(content="Not found", status_code=404)

        data = result.get("data")
        data_type = result.get("data_type")
        content_encoding = result.get("content_encoding")

        if data_type == "binary":
            headers = {}
            if content_encoding:
                headers["Content-Encoding"] = content_encoding
            return Response(content=data, media_type="application/octet-stream",
                          headers=headers)
        elif data_type == "string":
            return Response(content=data.encode('utf-8'),
                          media_type="application/octet-stream")
        elif data_type == "json":
            import json
            return Response(content=json.dumps(data).encode('utf-8'),
                          media_type="application/octet-stream")

        return Response(content=str(data).encode('utf-8'),
                      media_type="application/octet-stream")
    def setup_routes(self):                                                        # Configure FastAPI routes
        # String endpoints
        self.add_route_post(self.store__string__strategy__namespace)

        # JSON endpoints
        self.add_route_post(self.store__json__strategy__namespace)

        # Binary endpoints
        self.add_route_post(self.store__binary__strategy__namespace)

        # Generic retrieval endpoints (return with metadata and type info)
        self.add_route_get(self.retrieve__by_hash__cache_hash__namespace)
        self.add_route_get(self.retrieve__by_id__cache_id__namespace)

        # Type-specific retrieval by ID
        self.add_route_get(self.retrieve__string__by_id__cache_id__namespace)
        self.add_route_get(self.retrieve__json__by_id__cache_id__namespace)
        self.add_route_get(self.retrieve__binary__by_id__cache_id__namespace)

        # Type-specific retrieval by hash
        self.add_route_get(self.retrieve__string__by_hash__cache_hash__namespace)
        self.add_route_get(self.retrieve__json__by_hash__cache_hash__namespace)
        self.add_route_get(self.retrieve__binary__by_hash__cache_hash__namespace)

        # Utility endpoints
        self.add_route_get(self.exists__cache_hash__namespace)
        self.add_route_get(self.namespaces)
        self.add_route_get(self.stats__namespaces__namespace)
        self.add_route_get(self.stats__namespaces)
        self.add_route_delete(self.delete__by_id__cache_id__namespace)

    # # todo: remove this method when the next version of OSBot-Fast-API has been installed (which uses the code below)
    # def add_route_delete(self, function):
    #     return self.add_route_with_body(function, methods=['DELETE'])