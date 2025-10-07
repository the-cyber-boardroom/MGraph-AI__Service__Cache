from typing                                                                                 import Dict, Union
from fastapi                                                                                import HTTPException, Body
from osbot_fast_api.api.decorators.route_path                                               import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                             import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                  import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                     import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                           import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                              import type_safe
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Store__Request  import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Store__Response import Schema__Cache__Data__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type              import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                          import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store                 import Cache__Service__Data__Store


TAG__ROUTES_STORE__DATA       = Safe_Str__Fast_API__Route__Tag('data/store')
PREFIX__ROUTES_STORE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_STORE__DATA = f'{PREFIX__ROUTES_STORE__DATA}/{TAG__ROUTES_STORE__DATA}/'

ROUTES_PATHS__STORE__DATA = [ BASE_PATH__ROUTES_STORE__DATA + 'string'                               ,
                              BASE_PATH__ROUTES_STORE__DATA + 'string/{data_file_id}'                ,
                              BASE_PATH__ROUTES_STORE__DATA + 'string/{data_key:path}/{data_file_id}',
                              BASE_PATH__ROUTES_STORE__DATA + 'json'                                 ,
                              BASE_PATH__ROUTES_STORE__DATA + 'json/{data_file_id}'                  ,
                              BASE_PATH__ROUTES_STORE__DATA + 'json/{data_key:path}/{data_file_id}'  ,
                              BASE_PATH__ROUTES_STORE__DATA + 'binary'                               ,
                              BASE_PATH__ROUTES_STORE__DATA + 'binary/{data_file_id}'                ,
                              BASE_PATH__ROUTES_STORE__DATA + 'binary/{data_key:path}/{data_file_id}']


class Routes__Data__Store(Fast_API__Routes):                                            # FastAPI routes for storing data files as cache entry children
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_STORE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_STORE__DATA
    cache_service : Cache__Service                                                      # Dependency injection for cache service

    @cache_on_self
    def store_service(self) -> Cache__Service__Data__Store:                             # Service layer for data storage logic
        return Cache__Service__Data__Store(cache_service=self.cache_service)

    @route_path("/data/store/binary")
    def data__store_binary(self, body: bytes         = Body(...)                  ,
                                 cache_id     : Random_Guid   = None                       ,
                                 namespace    : Safe_Str__Id  = FAST_API__PARAM__NAMESPACE ,
                            ) -> Schema__Cache__Data__Store__Response:
        return self.data__store_binary__with__id_and_key(body         = body        ,
                                                         cache_id     = cache_id    ,
                                                         namespace    = namespace   ,
                                                         data_key     = ''          ,
                                                         data_file_id = ''          )

    @route_path("/data/store/binary/{data_file_id}")
    def data__store_binary__with__id(self, body         : bytes         = Body(...)                     ,
                                           cache_id     : Random_Guid   = None                          ,
                                           namespace    : Safe_Str__Id  = FAST_API__PARAM__NAMESPACE    ,
                                           data_file_id : Safe_Str__Id  = None
                                    ) -> Schema__Cache__Data__Store__Response:
        return self.data__store_binary__with__id_and_key(body         = body        ,
                                                         cache_id     = cache_id    ,
                                                         namespace    = namespace   ,
                                                         data_key     = ''          ,
                                                         data_file_id = data_file_id)

    @route_path("/data/store/binary/{data_key:path}/{data_file_id}")
    def data__store_binary__with__id_and_key(self, body         : bytes                  = Body(..., media_type="application/octet-stream"),
                                                   cache_id     : Random_Guid            = None                          ,
                                                   namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                                   data_key     : Safe_Str__File__Path   = None                          ,
                                                   data_file_id : Safe_Str__Id           = None
                                           ) -> Schema__Cache__Data__Store__Response:                                    # Store binary data file under cache entry

        request = Schema__Cache__Data__Store__Request(cache_id     = cache_id                     ,
                                                      data         = body                         ,
                                                      data_type    = Enum__Cache__Data_Type.BINARY,
                                                      data_key     = data_key                     ,
                                                      data_file_id = data_file_id                 ,
                                                      namespace    = namespace                    )

        result = self.store_service().store_data(request)
        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)

    @route_path("/data/store/json")
    def data__store_json(self, data         : dict         = Body(...)                  ,
                               cache_id     : Random_Guid  = None                       ,
                               namespace    : Safe_Str__Id = FAST_API__PARAM__NAMESPACE ,
                        ) -> Schema__Cache__Data__Store__Response:
        return self.data__store_json__with__id_and_key(data         = data      ,
                                                       cache_id     = cache_id  ,
                                                       namespace    = namespace ,
                                                       data_key     = ''        ,
                                                       data_file_id = ''        )

    @route_path("/data/store/json/{data_file_id}")
    def data__store_json__with__id(self, data         : dict         = Body(...)                  ,
                                         cache_id     : Random_Guid  = None                       ,
                                         namespace    : Safe_Str__Id = FAST_API__PARAM__NAMESPACE ,
                                         data_file_id : Safe_Str__Id = None
                                  ) -> Schema__Cache__Data__Store__Response:
        return self.data__store_json__with__id_and_key(data           = data        ,
                                                      cache_id     = cache_id      ,
                                                      namespace    = namespace     ,
                                                      data_key     = ''            ,
                                                      data_file_id = data_file_id  )

    @route_path("/data/store/json/{data_key:path}/{data_file_id}")
    def data__store_json__with__id_and_key(self, data         : dict                   = Body(...)                     ,
                                                cache_id     : Random_Guid            = None                          ,
                                                namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                                data_key     : Safe_Str__File__Path   = None                          ,
                                                data_file_id : Safe_Str__Id           = None
                                         ) -> Schema__Cache__Data__Store__Response:                                     # Store JSON data file under cache entry
        request = Schema__Cache__Data__Store__Request(cache_id     = cache_id              ,
                                                      data         = data                  ,
                                                      data_type    = Enum__Cache__Data_Type.JSON,
                                                      data_key     = data_key              ,
                                                      data_file_id = data_file_id          ,
                                                      namespace    = namespace             )

        result = self.store_service().store_data(request)

        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)

    def test_404(self, cache_id     : Random_Guid            = None                          ,
                       namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,):

        error_detail = { "error_type" : "NOT_FOUND"                                                      ,
                             "message"    : f"Cache entry '{cache_id}' in namespace '{namespace}' not found" ,
                             "cache_id"   : str(cache_id)                                                    }
        raise HTTPException(status_code=404, detail=error_detail)

    @route_path("/data/store/string")
    def data__store_string(self, data         : str                    = Body(...)                     ,
                                  cache_id     : Random_Guid            = None                          ,
                                  namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                          ) -> Schema__Cache__Data__Store__Response:

        return self.data__store_string__with__id_and_key(data         = data        ,
                                                         cache_id     = cache_id    ,
                                                         namespace    = namespace   ,
                                                         data_key     = ''          ,
                                                         data_file_id = ''          )

    @route_path("/data/store/string/{data_file_id}")
    def data__store_string__with__id(self, data         : str                    = Body(...)                     ,
                                           cache_id     : Random_Guid            = None                          ,
                                           namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                           data_file_id : Safe_Str__Id           = None
                                    ) -> Schema__Cache__Data__Store__Response:
        return self.data__store_string__with__id_and_key(data         = data        ,
                                                         cache_id     = cache_id    ,
                                                         namespace    = namespace   ,
                                                         data_key     = ''          ,
                                                         data_file_id = data_file_id)

    @route_path("/data/store/string/{data_key:path}/{data_file_id}")
    def data__store_string__with__id_and_key(self, data         : str                    = Body(...)                     ,
                                           cache_id     : Random_Guid            = None                          ,
                                           namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                           data_key     : Safe_Str__File__Path   = None                          ,
                                           data_file_id : Safe_Str__Id           = None
                                    ) -> Schema__Cache__Data__Store__Response:                                   # Store string data file under cache entry

        request = Schema__Cache__Data__Store__Request(cache_id     = cache_id                ,
                                                      data         = data                    ,
                                                      data_type    = Enum__Cache__Data_Type.STRING,
                                                      data_key     = data_key                ,
                                                      data_file_id = data_file_id            ,
                                                      namespace    = namespace               )
        result = self.store_service().store_data(request)

        return self.handle_not_found(result, cache_id=cache_id, namespace=namespace)

    @type_safe
    def handle_not_found(self, result        : Union[Type_Safe, Dict] = None,                                 # Base method for 404 handling
                               cache_id      : Random_Guid            = None,
                               namespace     : Safe_Str__Id           = None):
        if result is None:
            error_detail = { "error_type" : "NOT_FOUND"                                                      ,
                             "message"    : f"Cache entry '{cache_id}' in namespace '{namespace}' not found" ,
                             "cache_id"   : str(cache_id)                                                    }
            raise HTTPException(status_code=404, detail=error_detail)
        return result

    def setup_routes(self):                                                             # Configure all data storage routes
        self.add_route_post(self.data__store_binary                   )                 # binary endpoints
        self.add_route_post(self.data__store_binary__with__id         )
        self.add_route_post(self.data__store_binary__with__id_and_key )

        self.add_route_post(self.data__store_json                     )                 # json endpoints
        self.add_route_post(self.data__store_json__with__id           )
        self.add_route_post(self.data__store_json__with__id_and_key   )

        self.add_route_post(self.data__store_string                   )                 # string endpoints
        self.add_route_post(self.data__store_string__with__id         )
        self.add_route_post(self.data__store_string__with__id_and_key )