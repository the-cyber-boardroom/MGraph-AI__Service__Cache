import base64
import json
from typing                                                                              import Union, Dict
from fastapi                                                                             import HTTPException, Response, Path
from memory_fs.schemas.Schema__Memory_FS__File__Metadata                                 import Schema__Memory_FS__File__Metadata
from memory_fs.schemas.Schema__Memory_FS__File__Config                                   import Schema__Memory_FS__File__Config
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                           import type_safe
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__File__Refs         import Schema__Cache__File__Refs
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from osbot_fast_api.api.decorators.route_path                                            import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix               import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                  import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                        import cache_on_self
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Binary__Reference       import Schema__Cache__Binary__Reference
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type           import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                       import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve             import Cache__Service__Retrieve

TAG__ROUTES_RETRIEVE                  = 'retrieve'
PREFIX__ROUTES_RETRIEVE               = '/{namespace}'
BASE_PATH__ROUTES_RETRIEVE            = f'{PREFIX__ROUTES_RETRIEVE}/{TAG__ROUTES_RETRIEVE}/'
ROUTES_PATHS__RETRIEVE                = [ BASE_PATH__ROUTES_RETRIEVE + '{cache_id}'               ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/config'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/refs'          ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/refs/all'      ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/metadata'      ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/binary'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/json'          ,
                                          BASE_PATH__ROUTES_RETRIEVE + '{cache_id}/string'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}'        ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/binary' ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/json'   ,
                                          BASE_PATH__ROUTES_RETRIEVE + 'hash/{cache_hash}/string' ]

class Routes__File__Retrieve(Fast_API__Routes):                                             # FastAPI routes for cache retrieval operations
    tag            : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_RETRIEVE
    prefix         : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_RETRIEVE
    cache_service  : Cache__Service                                                                          # get cache_service via Dependency Injection
    
    @cache_on_self
    def retrieve_service(self):                                                                             # Service layer for business logic
        return Cache__Service__Retrieve(cache_service=self.cache_service)                                   # create Cache__Service__Retrieve object (once, using the shared Cache_Service)

    @type_safe
    def handle_not_found(self, result        : Union[Type_Safe, Dict] = None,                                 # Base method for 404 handling
                               cache_id      : Random_Guid            = None,
                               cache_hash    : Safe_Str__Cache_Hash   = None,
                               namespace     : Safe_Str__Id           = None):
        if result is None:
            error = self.retrieve_service().get_not_found_error(cache_id   = cache_id  ,
                                                                cache_hash = cache_hash,
                                                                namespace  = namespace )
            raise HTTPException(status_code=404, detail=error.json())
        return result

    def retrieve__cache_id(self, cache_id  : Random_Guid,
                                 namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                            ): # todo union is not supported by Fast_API. Refactor to one class -> Union[Schema__Cache__Retrieve__Success, Schema__Cache__Binary__Reference]:             # Retrieve by cache ID with metadata
                
        result = self.retrieve_service().retrieve_by_id(cache_id, namespace)                                              # Use service layer
        
        if result:
            # todo: this logic should not be happeining in this routes class, this should be handled by retrieve_by_id
            if result.data_type == Enum__Cache__Data_Type.BINARY:                           # Handle binary data that can't be returned in JSON
                binary_url = f"/{namespace}/retrieve/{cache_id}/binary"
                return Schema__Cache__Binary__Reference(message      = "Binary data requires separate endpoint"    ,        # todo: refactor this to use the Schema__Cache__Retrieve__Success
                                                        data_type    = Enum__Cache__Data_Type.BINARY               ,        #       which is quite compatible with this logic
                                                        size         = result.metadata.content_size                ,        #       since this is still a retrieve success, but it just the
                                                        cache_hash   = result.metadata.cache_hash                  ,        #       edge case where we don't return a binary here
                                                        cache_id     = result.metadata.cache_id                    ,        #       (to avoid having to convert bytes into base64)
                                                        namespace    = namespace                                   ,
                                                        binary_url   = binary_url                                  ,
                                                        metadata     = result.metadata                             )
        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)

    def retrieve__cache_id__config(self, cache_id  : Random_Guid,
                                         namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                    ) -> Schema__Memory_FS__File__Config:                                           # Retrieve by cache ID with metadata

        response_data = self.retrieve_service().retrieve_by_id__config(cache_id, namespace)
        return self.handle_not_found(response_data, cache_id=cache_id, namespace=namespace)

    def retrieve__cache_id__refs(self, cache_id : Random_Guid,
                                       namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                  ) -> Schema__Cache__File__Refs:                                                    # Get cache entry details

        result = self.retrieve_service().retrieve_by_id__refs(cache_id, namespace)
        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)

    def retrieve__cache_id__metadata(self, cache_id : Random_Guid,
                                           namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                      ) -> Schema__Memory_FS__File__Metadata:                                                       # Get cache entry details
        result = self.retrieve_service().retrieve_by_id__metadata(cache_id, namespace)                                              # todo: this class should return Schema__Cache__File__Refs
        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)


    def retrieve__cache_id__refs__all(self, cache_id: Random_Guid,
                                            namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                      ) -> Dict:
        result = self.retrieve_service().get_entry_details__all(cache_id, namespace)                                              # todo: this class should return Schema__Cache__File__Refs
        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)

    @route_path("/retrieve/hash/{cache_hash}")
    def retrieve__hash__cache_hash(self, cache_hash : Safe_Str__Cache_Hash,
                                         namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                    ): # todo union is not supported by Fast_API. Refactor to one class  -> Union[Schema__Cache__Retrieve__Success, Schema__Cache__Binary__Reference]:     # Retrieve by hash
                
        result = self.retrieve_service().retrieve_by_hash(cache_hash, namespace)                                          # Use service layer
        
        if result is None:
            # Return 404 Not Found
            error = self.retrieve_service().get_not_found_error(cache_hash=cache_hash, namespace=namespace)
            raise HTTPException(status_code=404, detail=error.json())
        
        # Handle binary data redirect
        if result.data_type == Enum__Cache__Data_Type.BINARY:
            binary_url = f"/{namespace}/retrieve/hash/{cache_hash}/binary"
            return Schema__Cache__Binary__Reference(
                message      = "Binary data requires separate endpoint"    ,
                data_type    = Enum__Cache__Data_Type.BINARY              ,
                size         = result.metadata.content_size               ,
                cache_hash   = result.metadata.cache_hash                 ,
                cache_id     = result.metadata.cache_id                   ,
                namespace    = namespace                                  ,
                binary_url   = binary_url                                  ,
                metadata     = result.metadata                             )
        
        return result
    
    @route_path("/retrieve/{cache_id}/string")
    def retrieve__cache_id__string(self, cache_id : Random_Guid,
                                         namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                    ) -> Response:                                                          # Retrieve as string format
        
        result = self.retrieve_service().retrieve_by_id(cache_id, namespace)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Cache entry not found")
        
        # Convert data to string format
        if result.data_type == Enum__Cache__Data_Type.STRING:
            content = result.data
        elif result.data_type == Enum__Cache__Data_Type.JSON:
            content = json.dumps(result.data)
        elif result.data_type == Enum__Cache__Data_Type.BINARY:
            content = base64.b64encode(result.data).decode('utf-8')         # if it is binary encode as base64
        else:
            content = str(result.data)
        
        return Response(content=content, media_type="text/plain")
    
    @route_path("/retrieve/{cache_id}/json")
    def retrieve__cache_id__json(self, cache_id  : Random_Guid,
                                       namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                  ) -> dict:                                                                    # Retrieve as JSON format
        namespace = namespace or Safe_Str__Id("default")
        
        result = self.retrieve_service().retrieve_by_id(cache_id, namespace)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Cache entry not found")
        
        # Return data based on type
        if result.data_type == Enum__Cache__Data_Type.JSON:
            return result.data
        elif result.data_type == Enum__Cache__Data_Type.STRING:
            # Try to parse as JSON
            try:
                return json.loads(result.data)
            except json.JSONDecodeError:
                # Return 415 Unsupported Media Type
                raise HTTPException(status_code=415, 
                                  detail=f"Data is string, not JSON: {result.data[:100]}")
        elif result.data_type == Enum__Cache__Data_Type.BINARY:
            # Return base64 encoded in JSON wrapper
            return {
                "data_type": "binary",
                "encoding": "base64",
                "data": base64.b64encode(result.data).decode('utf-8')
            }
        
        return {"data": result.data, "data_type": str(result.data_type)}                                                    # todo: look at how to handle this scenario that shouldn't happen
    
    @route_path("/retrieve/{cache_id}/binary")
    def retrieve__cache_id__binary(self, cache_id : Random_Guid,
                                         namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                    ) -> Response:                                                                          # Retrieve as binary format
        
        result = self.retrieve_service().retrieve_by_id(cache_id, namespace)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Cache entry not found")
        
        # Convert to binary format
        if result.data_type == Enum__Cache__Data_Type.BINARY:                                                               # todo: refactor to helper method since this is quite a common pattern (something like, convert_to_bytes)
            content = result.data
        elif result.data_type == Enum__Cache__Data_Type.STRING:
            content = result.data.encode('utf-8')
        elif result.data_type == Enum__Cache__Data_Type.JSON:
            content = json.dumps(result.data).encode('utf-8')
        else:
            content = str(result.data).encode('utf-8')
        
        return Response(content=content, media_type="application/octet-stream")
    
    @route_path("/retrieve/hash/{cache_hash}/string")
    def retrieve__hash__cache_hash__string(self, cache_hash : Safe_Str__Cache_Hash,
                                                 namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                            ) -> Response:                                                                  # Retrieve string by hash
        
        result = self.retrieve_service().retrieve_by_hash(cache_hash, namespace)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Cache entry not found")

        if result.data_type == Enum__Cache__Data_Type.STRING:                                                               # Same conversion logic as retrieve__cache_id__string
            content = result.data
        elif result.data_type == Enum__Cache__Data_Type.JSON:                                                               # todo: refactor to common conver method
            content = json.dumps(result.data)
        elif result.data_type == Enum__Cache__Data_Type.BINARY:
            try:
                content = result.data.decode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                content = base64.b64encode(result.data).decode('utf-8')
        else:
            content = str(result.data)                                                                                      # todo: look at how to handle this scenario that shouldn't happen
        
        return Response(content=content, media_type="text/plain")
    
    @route_path("/retrieve/hash/{cache_hash}/json")
    def retrieve__hash__cache_hash__json(self, cache_hash : Safe_Str__Cache_Hash,
                                               namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                          ) -> Dict:                                   # Retrieve JSON by hash

        
        result = self.retrieve_service().retrieve_by_hash(cache_hash, namespace)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Cache entry not found")

        if result.data_type == Enum__Cache__Data_Type.JSON:                                                                 # Same logic as retrieve__cache_id__json
            return result.data
        elif result.data_type == Enum__Cache__Data_Type.STRING:                                                             # todo: refactor to common convert method
            try:
                return json.loads(result.data)
            except json.JSONDecodeError:
                raise HTTPException(status_code=415, 
                                    detail=f"Data is string, not JSON")
        elif result.data_type == Enum__Cache__Data_Type.BINARY:
            return { "data_type" : "binary",                                                                                # todo: convert to Type_Safe class
                     "encoding"  : "base64",
                     "data"      : base64.b64encode(result.data).decode('utf-8')  }

    
    @route_path("/retrieve/hash/{cache_hash}/binary")
    def retrieve__hash__cache_hash__binary(self, cache_hash : Safe_Str__Cache_Hash,
                                                 namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                            ) -> Response:                             # Retrieve binary by hash
        
        result = self.retrieve_service().retrieve_by_hash(cache_hash, namespace)
        
        if result is None:
            raise HTTPException(status_code=404, detail="Cache entry not found")

        if result.data_type == Enum__Cache__Data_Type.BINARY:                                                               # Same conversion as retrieve__cache_id__binary
            content = result.data
        elif result.data_type == Enum__Cache__Data_Type.STRING:                                                             # todo: refactor to common convert method
            content = result.data.encode('utf-8')
        elif result.data_type == Enum__Cache__Data_Type.JSON:
            content = json.dumps(result.data).encode('utf-8')
        else:
            content = str(result.data).encode('utf-8')                                                                      # todo: look at how to handle this scenario that shouldn't happen
        
        return Response(content=content, media_type="application/octet-stream")
    


    def setup_routes(self):                                                                                                 # Configure all routes
        self.add_route_get(self.retrieve__cache_id                  )               # Generic retrieval (with metadata)
        self.add_route_get(self.retrieve__cache_id__config          )
        self.add_route_get(self.retrieve__cache_id__metadata        )
        self.add_route_get(self.retrieve__cache_id__refs            )
        self.add_route_get(self.retrieve__cache_id__refs__all       )

        self.add_route_get(self.retrieve__cache_id__string          )               # Type-specific retrieval
        self.add_route_get(self.retrieve__cache_id__json            )
        self.add_route_get(self.retrieve__cache_id__binary          )

        self.add_route_get(self.retrieve__hash__cache_hash          )
        self.add_route_get(self.retrieve__hash__cache_hash__string  )
        self.add_route_get(self.retrieve__hash__cache_hash__json    )
        self.add_route_get(self.retrieve__hash__cache_hash__binary  )