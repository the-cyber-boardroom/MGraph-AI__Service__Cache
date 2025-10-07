from typing                                                                     import Dict, Any
from osbot_fast_api.api.routes.Fast_API__Routes                                 import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix      import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag         import Safe_Str__Fast_API__Route__Tag
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid           import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id import Safe_Str__Id
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API              import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                       import Cache__Service

TAG__ROUTES_DELETE                  = 'delete'
PREFIX__ROUTES_DELETE               = '/{namespace}'
BASE_PATH__ROUTES_DELETE            = f'{PREFIX__ROUTES_DELETE}/{TAG__ROUTES_DELETE}/'
ROUTES_PATHS__DELETE                = [ BASE_PATH__ROUTES_DELETE + '{cache_id}']


class Routes__File__Delete(Fast_API__Routes):
    tag           : Safe_Str__Fast_API__Route__Tag     = TAG__ROUTES_DELETE
    prefix        : Safe_Str__Fast_API__Route__Prefix  =  PREFIX__ROUTES_DELETE
    cache_service : Cache__Service

    # todo: this should return a Type_Safe class
    def delete__cache_id(self, cache_id  : Random_Guid,
                               namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                          ) -> Dict[str, Any]:
        return self.cache_service.delete_by_id(cache_id, namespace)


    def setup_routes(self):
        self.add_route_delete(self.delete__cache_id)