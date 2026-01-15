from fastapi                                                                                      import HTTPException
from osbot_fast_api.api.decorators.route_path                                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                   import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                        import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                           import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                                 import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                 import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                                import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                   import Safe_Str__Id
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                    import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                         import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Exists                      import Cache__Service__Data__Exists
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Exists__Response      import Schema__Cache__Data__Exists__Response

TAG__ROUTES_EXISTS__DATA       = Safe_Str__Fast_API__Route__Tag('data')
PREFIX__ROUTES_EXISTS__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_EXISTS__DATA = f'{PREFIX__ROUTES_EXISTS__DATA}/{TAG__ROUTES_EXISTS__DATA}/'

ROUTES_PATHS__EXISTS__DATA = [ BASE_PATH__ROUTES_EXISTS__DATA + 'exists/{data_type}/{data_file_id}'                  ,
                               BASE_PATH__ROUTES_EXISTS__DATA + 'exists/{data_type}/{data_key:path}/{data_file_id}' ]


class Routes__Data__Exists(Fast_API__Routes):                                                   # FastAPI routes for checking if data files exist in cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_EXISTS__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_EXISTS__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def exists_service(self) -> Cache__Service__Data__Exists:                                   # Service layer for exists operations
        return Cache__Service__Data__Exists(cache_service=self.cache_service)

    @route_path("/data/exists/{data_type}/{data_file_id}")
    def data__exists__with__id(self, cache_id     : Cache_Id               = None                          ,
                                     namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                     data_type    : Enum__Cache__Data_Type = None                          ,
                                     data_file_id : Safe_Str__Id           = None
                               ) -> Schema__Cache__Data__Exists__Response:
        return self.data__exists__with__id_and_key(cache_id     = cache_id    ,
                                                   namespace    = namespace   ,
                                                   data_type    = data_type   ,
                                                   data_key     = ''          ,
                                                   data_file_id = data_file_id)

    @route_path("/data/exists/{data_type}/{data_key:path}/{data_file_id}")
    def data__exists__with__id_and_key(self, cache_id     : Cache_Id               = None                          ,
                                             namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                             data_type    : Enum__Cache__Data_Type = None                          ,
                                             data_key     : Safe_Str__File__Path   = None                          ,
                                             data_file_id : Safe_Str__Id           = None
                                       ) -> Schema__Cache__Data__Exists__Response:                                  # Check if specific data file exists

        if not data_file_id:
            error_detail = { "error_type" : "INVALID_INPUT",
                            "message"    : "data_file_id is required to check existence"}
            raise HTTPException(status_code=400, detail=error_detail)

        if not data_type:
            error_detail = { "error_type" : "INVALID_INPUT",
                            "message"    : "data_type is required to check existence"}
            raise HTTPException(status_code=400, detail=error_detail)

        exists = self.exists_service().data_file_exists(cache_id     = cache_id    ,
                                                        namespace    = namespace   ,
                                                        data_type    = data_type   ,
                                                        data_key     = data_key    ,
                                                        data_file_id = data_file_id)

        return Schema__Cache__Data__Exists__Response(exists       = exists      ,
                                                     cache_id     = cache_id    ,
                                                     namespace    = namespace   ,
                                                     data_type    = data_type   ,
                                                     data_key     = data_key    ,
                                                     data_file_id = data_file_id)

    def setup_routes(self):                                                                     # Configure all data exists routes
        self.add_route_get(self.data__exists__with__id        )
        self.add_route_get(self.data__exists__with__id_and_key)