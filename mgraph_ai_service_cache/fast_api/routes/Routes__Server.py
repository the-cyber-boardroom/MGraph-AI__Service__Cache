from osbot_fast_api.api.routes.Fast_API__Routes                     import Fast_API__Routes
from mgraph_ai_service_cache_client.schemas.cache.consts__Cache_Service    import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service           import Cache__Service
from mgraph_ai_service_cache.utils.testing.Cache__Test__Fixtures    import Cache__Test__Fixtures

TAG__ROUTES_SERVER                  = 'server'
ROUTES_PATHS__SERVER                = [f'/{TAG__ROUTES_SERVER}' + '/storage/info' ,
                                       f'/{TAG__ROUTES_SERVER}' + '/create/test-fixtures' ]

class Routes__Server(Fast_API__Routes):
    tag         : str          = TAG__ROUTES_SERVER
    cache_service : Cache__Service

    def storage__info(self) -> dict:                                                                     # Get current storage backend information
        return self.cache_service.get_storage_info()

    def create__test_fixtures(self):
        with Cache__Test__Fixtures(cache_service=self.cache_service     ,
                                   namespace  = DEFAULT_CACHE__NAMESPACE) as _:
            _.setup()
            return _.load_manifest__data()

    def setup_routes(self):
        self.add_route_get(self.storage__info        )
        self.add_route_get(self.create__test_fixtures)

