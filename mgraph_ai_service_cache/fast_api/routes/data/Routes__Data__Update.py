from typing                                                                                       import Union, Dict
from fastapi                                                                                      import HTTPException, Response, Body
from osbot_fast_api.api.decorators.route_path                                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                   import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                        import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                           import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                                 import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                 import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                                import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                   import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                                    import type_safe
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Update__Request       import Schema__Cache__Data__Update__Request
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Update__Response      import Schema__Cache__Data__Update__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                    import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                         import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Update                      import Cache__Service__Data__Update

TAG__ROUTES_UPDATE__DATA       = Safe_Str__Fast_API__Route__Tag('data/update')
PREFIX__ROUTES_UPDATE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_UPDATE__DATA = f'{PREFIX__ROUTES_UPDATE__DATA}/{TAG__ROUTES_UPDATE__DATA}/'

ROUTES_PATHS__UPDATE__DATA = [ BASE_PATH__ROUTES_UPDATE__DATA + 'string/{data_file_id}'                ,
                               BASE_PATH__ROUTES_UPDATE__DATA + 'string/{data_key:path}/{data_file_id}',
                               BASE_PATH__ROUTES_UPDATE__DATA + 'json/{data_file_id}'                  ,
                               BASE_PATH__ROUTES_UPDATE__DATA + 'json/{data_key:path}/{data_file_id}'  ,
                               BASE_PATH__ROUTES_UPDATE__DATA + 'binary/{data_file_id}'                ,
                               BASE_PATH__ROUTES_UPDATE__DATA + 'binary/{data_key:path}/{data_file_id}']


class Routes__Data__Update(Fast_API__Routes):                                                   # FastAPI routes for updating data files in cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_UPDATE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_UPDATE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def update_service(self) -> Cache__Service__Data__Update:                                   # Service layer for update operations
        return Cache__Service__Data__Update(cache_service=self.cache_service)

    # String update endpoints

    @route_path("/data/update/string/{data_file_id}")
    def data__update_string__with__id(self, data         : str          = Body(...)                     ,
                                            cache_id     : Cache_Id     = None                          ,
                                            namespace    : Safe_Str__Id = FAST_API__PARAM__NAMESPACE    ,
                                            data_file_id : Safe_Str__Id = None
                                      ) -> Schema__Cache__Data__Update__Response:
        return self.data__update_string__with__id_and_key(data         = data        ,
                                                          cache_id     = cache_id    ,
                                                          namespace    = namespace   ,
                                                          data_key     = ''          ,
                                                          data_file_id = data_file_id)

    @route_path("/data/update/string/{data_key:path}/{data_file_id}")
    def data__update_string__with__id_and_key(self, data         : str                    = Body(...)                     ,
                                                    cache_id     : Cache_Id               = None                          ,
                                                    namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                                    data_key     : Safe_Str__File__Path   = None                          ,
                                                    data_file_id : Safe_Str__Id           = None
                                              ) -> Schema__Cache__Data__Update__Response:       # Update string data file

        if not data:
            error_detail = { "error_type" : "INVALID_INPUT"             ,
                            "message"    : "String data cannot be empty",
                            "field_name" : "data"                       }
            raise HTTPException(status_code=400, detail=error_detail)

        request = Schema__Cache__Data__Update__Request(cache_id     = cache_id                ,
                                                       data         = data                    ,
                                                       data_type    = Enum__Cache__Data_Type.STRING,
                                                       data_key     = data_key                ,
                                                       data_file_id = data_file_id            ,
                                                       namespace    = namespace               )

        result = self.update_service().update_data(request)
        return self.handle_not_found(result, cache_id=cache_id, data_file_id=data_file_id)

    # JSON update endpoints

    @route_path("/data/update/json/{data_file_id}")
    def data__update_json__with__id(self, data         : dict         = Body(...)                     ,
                                          cache_id     : Cache_Id     = None                          ,
                                          namespace    : Safe_Str__Id = FAST_API__PARAM__NAMESPACE    ,
                                          data_file_id : Safe_Str__Id = None
                                    ) -> Schema__Cache__Data__Update__Response:
        return self.data__update_json__with__id_and_key(data         = data        ,
                                                        cache_id     = cache_id    ,
                                                        namespace    = namespace   ,
                                                        data_key     = ''          ,
                                                        data_file_id = data_file_id)

    @route_path("/data/update/json/{data_key:path}/{data_file_id}")
    def data__update_json__with__id_and_key(self, data         : dict                   = Body(...)                     ,
                                                  cache_id     : Cache_Id               = None                          ,
                                                  namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                                  data_key     : Safe_Str__File__Path   = None                          ,
                                                  data_file_id : Safe_Str__Id           = None
                                            ) -> Schema__Cache__Data__Update__Response:         # Update JSON data file

        request = Schema__Cache__Data__Update__Request(cache_id     = cache_id              ,
                                                       data         = data                  ,
                                                       data_type    = Enum__Cache__Data_Type.JSON,
                                                       data_key     = data_key              ,
                                                       data_file_id = data_file_id          ,
                                                       namespace    = namespace             )

        result = self.update_service().update_data(request)
        return self.handle_not_found(result, cache_id=cache_id, data_file_id=data_file_id)

    # Binary update endpoints

    @route_path("/data/update/binary/{data_file_id}")
    def data__update_binary__with__id(self, body         : bytes        = Body(..., media_type="application/octet-stream"),
                                            cache_id     : Cache_Id     = None                          ,
                                            namespace    : Safe_Str__Id = FAST_API__PARAM__NAMESPACE    ,
                                            data_file_id : Safe_Str__Id = None
                                      ) -> Schema__Cache__Data__Update__Response:
        return self.data__update_binary__with__id_and_key(body         = body        ,
                                                          cache_id     = cache_id    ,
                                                          namespace    = namespace   ,
                                                          data_key     = ''          ,
                                                          data_file_id = data_file_id)

    @route_path("/data/update/binary/{data_key:path}/{data_file_id}")
    def data__update_binary__with__id_and_key(self, body         : bytes                  = Body(..., media_type="application/octet-stream"),
                                                    cache_id     : Cache_Id               = None                          ,
                                                    namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                                    data_key     : Safe_Str__File__Path   = None                          ,
                                                    data_file_id : Safe_Str__Id           = None
                                              ) -> Schema__Cache__Data__Update__Response:       # Update binary data file

        if not body:
            error_detail = { "error_type" : "INVALID_INPUT"             ,
                            "message"    : "Binary data cannot be empty",
                            "field_name" : "body"                       }
            raise HTTPException(status_code=400, detail=error_detail)

        request = Schema__Cache__Data__Update__Request(cache_id     = cache_id                 ,
                                                       data         = body                     ,
                                                       data_type    = Enum__Cache__Data_Type.BINARY,
                                                       data_key     = data_key                 ,
                                                       data_file_id = data_file_id             ,
                                                       namespace    = namespace                )

        result = self.update_service().update_data(request)
        return self.handle_not_found(result, cache_id=cache_id, data_file_id=data_file_id)

    @type_safe
    def handle_not_found(self, result        : Union[Type_Safe, Dict] = None,                   # Base method for 404 handling
                               cache_id      : Cache_Id               = None,
                               data_file_id  : Safe_Str__Id           = None):
        if result is None:
            error_detail = { "error_type"   : "NOT_FOUND"                                      ,
                             "message"      : f"Data file '{data_file_id}' not found in cache entry '{cache_id}'",
                             "cache_id"     : str(cache_id)                                    ,
                             "data_file_id" : str(data_file_id)                                }
            raise HTTPException(status_code=404, detail=error_detail)
        return result

    def setup_routes(self):                                                                     # Configure all data update routes
        self.add_route_post(self.data__update_string__with__id        )                         # string endpoints
        self.add_route_post(self.data__update_string__with__id_and_key)

        self.add_route_post(self.data__update_json__with__id          )                         # json endpoints
        self.add_route_post(self.data__update_json__with__id_and_key  )

        self.add_route_post(self.data__update_binary__with__id        )                         # binary endpoints
        self.add_route_post(self.data__update_binary__with__id_and_key)