from osbot_fast_api.api.decorators.route_path import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                  import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix       import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag          import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                import cache_on_self
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id  import Safe_Str__Id
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API               import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                        import Cache__Service
from mgraph_ai_service_cache.service.cache.test_data.Cache__Service__Test_Data   import Cache__Service__Test_Data
from mgraph_ai_service_cache.schemas.test_data.Schema__Test_Data                 import Schema__Test_Data__Create__Response
from mgraph_ai_service_cache.schemas.test_data.Schema__Test_Data                 import Schema__Test_Data__Clear__Response

TAG__ROUTES_TEST_DATA    = Safe_Str__Fast_API__Route__Tag('test-data')
PREFIX__ROUTES_TEST_DATA = Safe_Str__Fast_API__Route__Prefix('/test-data')
ROUTES_PATHS__TEST_DATA  = [ f'/{TAG__ROUTES_TEST_DATA}/create/in/default-namespaces/comprehensive',
                             f'/{TAG__ROUTES_TEST_DATA}/create/in/default-namespaces/minimal'      ,
                             f'/{TAG__ROUTES_TEST_DATA}/create/in/new-namespace/{{namespace}}'     ,
                             f'/{TAG__ROUTES_TEST_DATA}/clear/{{namespace}}'                       ]


class Routes__Test_Data(Fast_API__Routes):                                       # FastAPI routes for generating test data for the web console
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_TEST_DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_TEST_DATA
    cache_service : Cache__Service

    @cache_on_self
    def test_data_service(self) -> Cache__Service__Test_Data:                    # Service layer for test data operations
        return Cache__Service__Test_Data(cache_service=self.cache_service)

    @route_path('/create/in/default-namespaces/comprehensive')
    def create__comprehensive(self) -> Schema__Test_Data__Create__Response:      # Create comprehensive test data across all strategies and namespaces
        return self.test_data_service().create_comprehensive()

    @route_path('/create/in/new-namespace/{namespace}')
    def create__namespace(self, namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                          ) -> Schema__Test_Data__Create__Response:              # Create test data for a specific namespace
        return self.test_data_service().create_for_namespace(namespace)

    @route_path('/create/in/default-namespaces/minimal')
    def create__minimal(self) -> Schema__Test_Data__Create__Response:            # Create minimal test data (quick setup)
        return self.test_data_service().create_minimal()

    def clear__namespace(self, namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                         ) -> Schema__Test_Data__Clear__Response:                # Clear all data from a specific namespace
        return self.test_data_service().clear_namespace(namespace)

    def setup_routes(self):                                                      # Configure all test data routes
        self.add_route_post  (self.create__comprehensive)
        self.add_route_post  (self.create__minimal      )
        self.add_route_post  (self.create__namespace    )
        self.add_route_delete(self.clear__namespace     )