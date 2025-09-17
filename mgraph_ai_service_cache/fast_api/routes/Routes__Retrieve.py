import base64
import json
from typing                                                            import Dict, Any
from fastapi                                                           import Response
from osbot_fast_api.api.routes.Fast_API__Routes                        import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix          import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag             import Safe_Str__Fast_API__Route__Tag
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid  import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id      import Safe_Id
from osbot_utils.utils.Http                                            import url_join_safe
from memory_fs.schemas.Safe_Str__Cache_Hash                            import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service              import Cache__Service

TAG__ROUTES_RETRIEVE                  = 'retrieve'
PREFIX__ROUTES_RETRIEVE               = '/{namespace}'
BASE_PATH__ROUTES_RETRIEVE            = f'{PREFIX__ROUTES_RETRIEVE}/{TAG__ROUTES_RETRIEVE}/'
ROUTES_PATHS__RETRIEVE                = [ BASE_PATH__ROUTES_RETRIEVE + '{cache_id}'               ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/binary'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/json'          ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/string'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/binary' ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/json'   ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/string' ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'details/{cache_id}'       ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'details/all/{cache_id}'   ]


# todo: refactor this logic into a Service__Retrieve class

class Routes__Retrieve(Fast_API__Routes):                                             # FastAPI routes for cache operations
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_RETRIEVE
    prefix        : Safe_Str__Fast_API__Route__Prefix  =  PREFIX__ROUTES_RETRIEVE
    cache_service : Cache__Service

    def retrieve__hash__cache_hash(self, cache_hash : Safe_Str__Cache_Hash,
                                         namespace  : Safe_Id = None
                                    ) -> Dict[str, Any]:                          # Retrieve latest by hash with type information"""
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        namespace = namespace or "default"                                      # todo: change to static config variable
        binary_url = (BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/binary').format(cache_hash=cache_hash, namespace=namespace)

        if result.get("data_type") == "binary":                                                 # If data is binary, don't return it in JSON - direct to binary endpoint
            return { "status"      : "binary_data"                                                          ,           # todo: convert to Type_Safe class
                     "message"     : "Binary data cannot be returned in JSON response"                      ,
                     "data_type"   : "binary"                                                               ,
                     "size"        : len(result.get("data", b""))                                           ,
                     "metadata"    : result.get("metadata", {})                                             ,
                     "namespace"   : namespace                                                              ,
                     "binary_url"  : binary_url                                                             }

        return result

    def retrieve__cache_id(self, cache_id  : Random_Guid,
                                 namespace : Safe_Id = None
                            ) -> Dict[str, Any]:              # Retrieve by cache ID with type information
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        namespace = namespace or "default"                                      # todo: change to static config variable
        binary_url = (BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/binary').format(cache_id=cache_id, namespace=namespace)
        if result.get("data_type") == "binary":                                 # If data is binary, don't return it in JSON - direct to binary endpoint
            return { "status"      : "binary_data"                                                      ,
                     "message"     : "Binary data cannot be returned in JSON response"                  ,
                     "data_type"   : "binary"                                                           ,
                     "size"        : len(result.get("data", b""))                                       ,
                     "metadata"    : result.get("metadata", {})                                         ,
                     "binary_url"  : binary_url                                                         }
                     #"binary_url"  : f"/cache/retrieve/binary/by-id/{cache_id}/{namespace or 'default'}" }

        return result

    # New type-specific retrieval methods
    def retrieve__cache_id__string(self, cache_id: Random_Guid,
                                         namespace: Safe_Id = None
                                         ) -> str:
        """Retrieve as string by cache ID"""
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return Response(content="Not found", status_code=404)

        data      = result.get("data")
        data_type = result.get("data_type")

        if data_type == "string":
            return Response(content=data, media_type="text/plain")
        elif data_type == "json":
            return Response(content=json.dumps(data), media_type="text/plain")
        elif data_type == "binary":                                                 # Convert binary to string (might not be ideal for all binary data)
            try:
                return Response(content=data.decode('utf-8'), media_type="text/plain")
            except:
                return Response(content=base64.b64encode(data).decode('utf-8'),     # If can't decode, return base64 encoded
                                media_type="text/plain")

        return Response(content=str(data), media_type="text/plain")

    def retrieve__cache_id__json(self, cache_id  : Random_Guid,
                                       namespace: Safe_Id = None
                                  ) -> Dict[str, Any]:               # Retrieve as JSON by cache ID
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        data = result.get("data")
        data_type = result.get("data_type")

        if data_type == "json":
            return data
        elif data_type == "string":                             # Try to parse string as JSON
            try:
                return json.loads(data)
            except:
                return {"error": "Data is not valid JSON", "data": data}
        elif data_type == "binary":                                         # Return base64 encoded binary in JSON wrapper
            return { "data_type": "binary",
                     "encoding": "base64",
                     "data"    : base64.b64encode(data).decode('utf-8')}

        return {"data": data, "data_type": data_type}

    def retrieve__cache_id__binary(self, cache_id: Random_Guid,
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

    def retrieve__hash__cache_hash__string(self, cache_hash: Safe_Str__Cache_Hash,
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
            return Response(content=json.dumps(data), media_type="text/plain")
        elif data_type == "binary":
            try:
                return Response(content=data.decode('utf-8'), media_type="text/plain")
            except:
                return Response(content=base64.b64encode(data).decode('utf-8'),
                              media_type="text/plain")

        return Response(content=str(data), media_type="text/plain")

    def retrieve__hash__cache_hash__json(self, cache_hash: Safe_Str__Cache_Hash,
                                               namespace: Safe_Id = None
                                               ) -> Dict[str, Any]:     # Retrieve as JSON by hash
        result = self.cache_service.retrieve_by_hash(cache_hash, namespace)
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}

        data      = result.get("data")
        data_type = result.get("data_type")

        if data_type == "json":
            return data
        elif data_type == "string":
            try:
                return json.loads(data)
            except:
                return {"error": "Data is not valid JSON", "data": data}
        elif data_type == "binary":                                                 # this base64 convertion should be useful for some web clients that want to get the base64 encoding data of a file
            return {    "data_type": "binary",
                        "encoding": "base64",
                        "data": base64.b64encode(data).decode('utf-8')}

        return {"data": data, "data_type": data_type}

    def retrieve__hash__cache_hash__binary(self, cache_hash: Safe_Str__Cache_Hash,
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
            return Response(content=json.dumps(data).encode('utf-8'),
                            media_type="application/octet-stream")

        return Response(content=str(data).encode('utf-8'),
                        media_type="application/octet-stream")

    def retrieve__details__cache_id(self, cache_id: Random_Guid, namespace: Safe_Id = None):
        details = self.cache_service.retrieve_by_id__config(cache_id=cache_id, namespace=namespace)
        if details:
            return { 'details': details }
        return {'error': 'Not found', 'message': f'Cache entry id not found: {cache_id} in namespace: {namespace}'}

    def retrieve__details__all__cache_id(self, cache_id: Random_Guid, namespace: Safe_Id = None):
        result = self.retrieve__details__cache_id(cache_id=cache_id, namespace=namespace)
        details = result.get('details')
        if not details:
            return result

        all_details   = {}
        content_paths = details.get("content_paths")            # capture this one, since we don't want to show it
        storage_fs    = self.cache_service.storage_fs()
        for file_type, file_paths in details.get('all_paths').items():
            for file_path in file_paths:
                if file_path not in content_paths:
                    full_file_path         = url_join_safe(str(namespace), file_path)
                    if full_file_path:
                        file_contents          = storage_fs.file__json(full_file_path)          # all these files are json files
                        all_details[file_path] = file_contents
        return dict(by_id   =  details   ,
                    details = all_details)


    def setup_routes(self):

        self.add_route_get(self.retrieve__cache_id)
        self.add_route_get(self.retrieve__cache_id__string    )                   # Type-specific retrieval by ID and Hash
        self.add_route_get(self.retrieve__cache_id__json      )
        self.add_route_get(self.retrieve__cache_id__binary    )


        self.add_route_get(self.retrieve__hash__cache_hash        )               # Generic retrieval endpoints (return with metadata and type info)
        self.add_route_get(self.retrieve__hash__cache_hash__string)
        self.add_route_get(self.retrieve__hash__cache_hash__json  )
        self.add_route_get(self.retrieve__hash__cache_hash__binary)

        self.add_route_get(self.retrieve__details__all__cache_id)
        self.add_route_get(self.retrieve__details__cache_id     )


