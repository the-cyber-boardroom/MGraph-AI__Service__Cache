from osbot_fast_api.api.routes.Fast_API__Routes           import Fast_API__Routes
from mgraph_ai_service_cache.service.cache.Cache__Service import Cache__Service

TAG__ROUTES_SERVER                  = 'server'
ROUTES_PATHS__SERVER                = [ f'/{TAG__ROUTES_SERVER}/namespaces' ]

class Routes__Server(Fast_API__Routes):
    tag         : str          = TAG__ROUTES_SERVER
    cache_service : Cache__Service

    # # todo: fix the logic, since this will only return the active namespaces in memory (and not the ones from the cloud storage
    # def namespaces(self) -> Dict[str, Any]:                                        # List all active namespaces
    #     namespaces = self.cache_service.list_namespaces()
    #     return {"namespaces": [str(ns) for ns in namespaces], "count": len(namespaces)}

    def setup_routes(self):
        #self.add_route_get(self.namespaces)
        pass

