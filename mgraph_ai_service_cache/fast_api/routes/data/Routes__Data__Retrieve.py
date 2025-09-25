from typing                                                                                import List
from fastapi                                                                               import HTTPException, Response
from osbot_fast_api.api.decorators.route_path                                              import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                            import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                              import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                                 import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                          import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path          import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                      import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id            import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request     import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                    import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                  import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve             import Cache__Service__Data__Retrieve

TAG__ROUTES_RETRIEVE__DATA       = Safe_Str__Fast_API__Route__Tag('data')
PREFIX__ROUTES_RETRIEVE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_RETRIEVE__DATA = f'{PREFIX__ROUTES_RETRIEVE__DATA}/{TAG__ROUTES_RETRIEVE__DATA}/'

ROUTES_PATHS__RETRIEVE__DATA = [ BASE_PATH__ROUTES_RETRIEVE__DATA + 'json'   ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'string' ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'binary' ]


class Routes__Data__Retrieve(Fast_API__Routes):                                                 # FastAPI routes for retrieving data files from cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_RETRIEVE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_RETRIEVE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def retrieve_service(self) -> Cache__Service__Data__Retrieve:                               # Service layer for data retrieval logic
        return Cache__Service__Data__Retrieve(cache_service=self.cache_service)

    @route_path("/data/json")
    def data__json(self, cache_id     : Random_Guid          = None                          ,
                         namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                         data_key     : Safe_Str__File__Path = None                          ,
                         data_file_id : Safe_Str__Id         = None
                   ) -> dict:                                                                    # Retrieve data as JSON format
        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id              ,
                                                         data_type    = Enum__Cache__Data_Type.JSON,
                                                         data_key     = data_key              ,
                                                         data_file_id = data_file_id          ,
                                                         namespace    = namespace             )

        result = self.retrieve_service().retrieve_data(request)

        if not result or not result.found:
            raise HTTPException(status_code = 404,
                               detail   = { "error_type"   : "NOT_FOUND"                     ,
                                            "message"      : "Data file not found"                   ,
                                            "cache_id"     : str(cache_id)                           ,
                                            "data_file_id" : str(data_file_id) if data_file_id else None})

        if result.data_type != Enum__Cache__Data_Type.JSON:
            raise HTTPException(status_code = 415,
                               detail   = { "error_type"  : "UNSUPPORTED_MEDIA_TYPE"        ,
                                            "message"     : f"Data is {result.data_type}, not JSON" ,
                                            "actual_type" : str(result.data_type)                   })

        return result.data

    @route_path("/data/string")
    def data__string(self, cache_id     : Random_Guid          = None                          ,
                          namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                          data_key     : Safe_Str__File__Path = None                          ,
                          data_file_id : Safe_Str__Id         = None
                    ) -> Response:                                                             # Retrieve data as plain text string
        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id                ,
                                                         data_type    = Enum__Cache__Data_Type.STRING,
                                                         data_key     = data_key                ,
                                                         data_file_id = data_file_id            ,
                                                         namespace    = namespace               )

        result = self.retrieve_service().retrieve_data(request)

        if not result or not result.found:
            return Response(content="Not found", status_code=404)

        return Response(content=result.data, media_type="text/plain")                           # Service returns string data, just pass it through

    @route_path("/data/binary")
    def data__binary(self, cache_id     : Random_Guid          = None                          ,
                          namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                          data_key     : Safe_Str__File__Path = None                          ,
                          data_file_id : Safe_Str__Id         = None
                    ) -> Response:                                                             # Retrieve data as binary octet-stream
        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id                 ,
                                                         data_type    = Enum__Cache__Data_Type.BINARY,
                                                         data_key     = data_key                 ,
                                                         data_file_id = data_file_id             ,
                                                         namespace    = namespace                )

        result = self.retrieve_service().retrieve_data(request)

        if not result or not result.found:
            return Response(content=b"Not found", status_code=404)

        # Service returns binary data, just pass it through
        return Response(content=result.data, media_type="application/octet-stream")

    def setup_routes(self) -> 'Routes__Data__Retrieve':                                      # Configure all data retrieval routes
        self.add_route_get(self.data__json  )
        self.add_route_get(self.data__string)
        self.add_route_get(self.data__binary)
        return self