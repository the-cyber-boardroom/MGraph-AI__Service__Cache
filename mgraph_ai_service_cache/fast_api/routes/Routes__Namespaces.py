from osbot_fast_api.api.routes.Fast_API__Routes                                 import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix      import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag         import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                               import cache_on_self
from mgraph_ai_service_cache.service.cache.Cache__Service                       import Cache__Service

TAG__ROUTES_NAMESPACES      = 'namespaces'
PREFIX__ROUTES_NAMESPACES   = '/{namespaces}'
ROUTES_PATHS__NAMESPACES    = [ PREFIX__ROUTES_NAMESPACES + '/list']


class Routes__Namespaces(Fast_API__Routes):
    tag           : Safe_Str__Fast_API__Route__Tag     = TAG__ROUTES_NAMESPACES
    prefix        : Safe_Str__Fast_API__Route__Prefix  = PREFIX__ROUTES_NAMESPACES
    cache_service : Cache__Service

    @cache_on_self
    def storage_fs(self):
        return self.cache_service.storage_fs()

    # todo, move this to a namespaces service
    def list(self):
        namespaces = self.storage_fs().folder__folders(parent_folder='/', return_full_path=False)       # the list of namespaces is the list of root folders
        return namespaces

    def setup_routes(self):
        self.add_route_get(self.list)