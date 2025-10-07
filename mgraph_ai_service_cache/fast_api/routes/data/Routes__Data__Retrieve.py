from typing                                                                                       import Union, Dict
from fastapi                                                                                      import HTTPException, Response, Body
from osbot_fast_api.api.decorators.route_path                                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                   import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                        import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                           import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                                 import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                 import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                   import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                                    import type_safe
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Request     import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Response    import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                    import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                         import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve                    import Cache__Service__Data__Retrieve

TAG__ROUTES_RETRIEVE__DATA       = Safe_Str__Fast_API__Route__Tag('data')
PREFIX__ROUTES_RETRIEVE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_RETRIEVE__DATA = f'{PREFIX__ROUTES_RETRIEVE__DATA}/{TAG__ROUTES_RETRIEVE__DATA}/'

ROUTES_PATHS__RETRIEVE__DATA = [ BASE_PATH__ROUTES_RETRIEVE__DATA + 'json/{data_file_id}'                  ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'json/{data_key:path}/{data_file_id}'  ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'string/{data_file_id}'                ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'string/{data_key:path}/{data_file_id}',
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'binary/{data_file_id}'                ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'binary/{data_key:path}/{data_file_id}']


class Routes__Data__Retrieve(Fast_API__Routes):                                                 # FastAPI routes for retrieving data files from cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_RETRIEVE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_RETRIEVE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def retrieve_service(self) -> Cache__Service__Data__Retrieve:                               # Service layer for data retrieval logic
        return Cache__Service__Data__Retrieve(cache_service=self.cache_service)


    @route_path("/data/json/{data_file_id}")
    def data__json__with__id(self, cache_id     : Random_Guid          = None                          ,
                                   namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                   data_file_id : Safe_Str__Id         = None
                             ) -> dict:
        return self.data__json__with__id_and_key(cache_id     = cache_id    ,
                                                 namespace    = namespace   ,
                                                 data_key     = ''          ,
                                                 data_file_id = data_file_id)

    @route_path("/data/json/{data_key:path}/{data_file_id}")
    def data__json__with__id_and_key(self, cache_id     : Random_Guid          = None                          ,
                                           namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                           data_key     : Safe_Str__File__Path = None                          ,
                                           data_file_id : Safe_Str__Id         = None
                                     ) -> dict:                                                                 # Retrieve data as JSON format
        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id              ,
                                                         data_type    = Enum__Cache__Data_Type.JSON,
                                                         data_key     = data_key              ,
                                                         data_file_id = data_file_id          ,
                                                         namespace    = namespace             )

        result = self.retrieve_service().retrieve_data(request)

        return self.handle_json_result(result, cache_id=cache_id, data_file_id=data_file_id)

    @route_path("/data/string/{data_file_id}")
    def data__string__with__id(self, cache_id     : Random_Guid          = None                          ,
                                     namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                     data_file_id : Safe_Str__Id         = None
                               ) -> Response:
        return self.data__string__with__id_and_key(cache_id     = cache_id    ,
                                                   namespace    = namespace   ,
                                                   data_key     = ''          ,
                                                   data_file_id = data_file_id)

    @route_path("/data/string/{data_key:path}/{data_file_id}")
    def data__string__with__id_and_key(self, cache_id     : Random_Guid          = None                          ,
                                             namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                             data_key     : Safe_Str__File__Path = None                          ,
                                             data_file_id : Safe_Str__Id         = None
                                       ) -> Response:                                                           # Retrieve data as plain text string
        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id                ,
                                                         data_type    = Enum__Cache__Data_Type.STRING,
                                                         data_key     = data_key                ,
                                                         data_file_id = data_file_id            ,
                                                         namespace    = namespace               )

        result = self.retrieve_service().retrieve_data(request)

        return self.handle_string_result(result)

    @route_path("/data/binary/{data_file_id}")
    def data__binary__with__id(self, cache_id     : Random_Guid          = None                          ,
                                     namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                     data_file_id : Safe_Str__Id         = None
                               ) -> Response:
        return self.data__binary__with__id_and_key(cache_id     = cache_id    ,
                                                   namespace    = namespace   ,
                                                   data_key     = ''          ,
                                                   data_file_id = data_file_id)

    @route_path("/data/binary/{data_key:path}/{data_file_id}")
    def data__binary__with__id_and_key(self, cache_id     : Random_Guid          = None                          ,
                                             namespace    : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                             data_key     : Safe_Str__File__Path = None                          ,
                                             data_file_id : Safe_Str__Id         = None
                                       ) -> Response:                                                           # Retrieve data as binary octet-stream
        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id                 ,
                                                         data_type    = Enum__Cache__Data_Type.BINARY,
                                                         data_key     = data_key                 ,
                                                         data_file_id = data_file_id             ,
                                                         namespace    = namespace                )

        result = self.retrieve_service().retrieve_data(request)

        return self.handle_binary_result(result)

    @type_safe
    def handle_not_found(self, result        : Union[Type_Safe, Dict] = None,                                  # Base method for 404 handling
                               cache_id      : Random_Guid            = None,
                               data_file_id  : Safe_Str__Id           = None):
        if result is None or (hasattr(result, 'found') and not result.found):
            error_detail = { "error_type"   : "NOT_FOUND"                                                     ,
                             "message"      : "Data file not found"                                            ,
                             "cache_id"     : str(cache_id) if cache_id else None                              ,
                             "data_file_id" : str(data_file_id) if data_file_id else None                      }
            raise HTTPException(status_code=404, detail=error_detail)
        return result

    def handle_json_result(self, result: Schema__Cache__Data__Retrieve__Response,                              # Handle JSON-specific result processing
                                 cache_id: Random_Guid = None,
                                 data_file_id: Safe_Str__Id = None) -> dict:
        self.handle_not_found(result, cache_id=cache_id, data_file_id=data_file_id)
        return result.data

    def handle_string_result(self, result: Schema__Cache__Data__Retrieve__Response) -> Response:               # Handle string-specific result processing
        if not result or not result.found:
            return Response(content="Not found", status_code=404)

        return Response(content=result.data, media_type="text/plain")

    def handle_binary_result(self, result: Schema__Cache__Data__Retrieve__Response) -> Response:               # Handle binary-specific result processing
        if not result or not result.found:
            return Response(content=b"Not found", status_code=404)

        return Response(content=result.data, media_type="application/octet-stream")

    def setup_routes(self):                                                        # Configure all data retrieval routes
        self.add_route_get(self.data__json__with__id           )                                               # json endpoints
        self.add_route_get(self.data__json__with__id_and_key   )

        self.add_route_get(self.data__string__with__id         )                                                # string endpoints
        self.add_route_get(self.data__string__with__id_and_key )

        self.add_route_get(self.data__binary__with__id         )                                               # binary endpoints
        self.add_route_get(self.data__binary__with__id_and_key )