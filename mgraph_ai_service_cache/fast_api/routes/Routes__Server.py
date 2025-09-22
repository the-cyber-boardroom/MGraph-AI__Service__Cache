from osbot_fast_api.api.routes.Fast_API__Routes           import Fast_API__Routes
from mgraph_ai_service_cache.service.cache.Cache__Service import Cache__Service

TAG__ROUTES_SERVER                  = 'server'
ROUTES_PATHS__SERVER                = [f'/{TAG__ROUTES_SERVER}' + '/storage/info'                                                            ]

class Routes__Server(Fast_API__Routes):
    tag         : str          = TAG__ROUTES_SERVER
    cache_service : Cache__Service

    def storage__info(self) -> dict:                                                                     # Get current storage backend information
        return self.cache_service.get_storage_info()


    def setup_routes(self):
        self.add_route_get(self.storage__info)

