from typing                                                                                       import List
from fastapi                                                                                      import HTTPException
from osbot_fast_api.api.decorators.route_path                                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                   import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                        import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                           import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                                 import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                 import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                                import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                   import Safe_Str__Id
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                         import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__List                        import Cache__Service__Data__List
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__List__Response        import Schema__Cache__Data__List__Response

TAG__ROUTES_LIST__DATA       = Safe_Str__Fast_API__Route__Tag('data')
PREFIX__ROUTES_LIST__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_LIST__DATA = f'{PREFIX__ROUTES_LIST__DATA}/{TAG__ROUTES_LIST__DATA}/'

ROUTES_PATHS__LIST__DATA = [ BASE_PATH__ROUTES_LIST__DATA + 'list'                ,
                             BASE_PATH__ROUTES_LIST__DATA + 'list/{data_key:path}']


class Routes__Data__List(Fast_API__Routes):                                                     # FastAPI routes for listing data files in cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_LIST__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_LIST__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def list_service(self) -> Cache__Service__Data__List:                                       # Service layer for list operations
        return Cache__Service__Data__List(cache_service=self.cache_service)

    @route_path("/data/list")
    def data__list(self, cache_id  : Cache_Id     = None                          ,
                         namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE    ,
                         recursive : bool         = True
                   ) -> Schema__Cache__Data__List__Response:
        return self.data__list__with__key(cache_id  = cache_id  ,
                                          namespace = namespace ,
                                          data_key  = ''        ,
                                          recursive = recursive )

    @route_path("/data/list/{data_key:path}")
    def data__list__with__key(self, cache_id  : Cache_Id             = None                          ,
                                    namespace : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                    data_key  : Safe_Str__File__Path = None                          ,
                                    recursive : bool                 = True
                              ) -> Schema__Cache__Data__List__Response:                         # List all data files under cache entry

        result = self.list_service().list_data_files(cache_id  = cache_id  ,
                                                     namespace = namespace ,
                                                     data_key  = data_key  ,
                                                     recursive = recursive )

        if result is None:
            error_detail = { "error_type" : "NOT_FOUND"                                        ,
                             "message"    : f"Cache entry '{cache_id}' not found"              ,
                             "cache_id"   : str(cache_id)                                      }
            raise HTTPException(status_code=404, detail=error_detail)

        return result

    def setup_routes(self):                                                                     # Configure all data list routes
        self.add_route_get(self.data__list         )
        self.add_route_get(self.data__list__with__key)