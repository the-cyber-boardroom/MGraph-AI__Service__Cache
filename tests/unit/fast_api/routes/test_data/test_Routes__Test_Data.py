from unittest                                                                        import TestCase
from osbot_fast_api.api.routes.Fast_API__Routes                                      import Fast_API__Routes
from osbot_fast_api_serverless.utils.testing.skip_tests                              import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                          import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                 import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id      import Safe_Str__Id
from osbot_utils.utils.Objects                                                       import base_classes
from mgraph_ai_service_cache.fast_api.routes.test_data.Routes__Test_Data             import Routes__Test_Data
from mgraph_ai_service_cache.fast_api.routes.test_data.Routes__Test_Data             import TAG__ROUTES_TEST_DATA
from mgraph_ai_service_cache.fast_api.routes.test_data.Routes__Test_Data             import PREFIX__ROUTES_TEST_DATA
from mgraph_ai_service_cache.fast_api.routes.test_data.Routes__Test_Data             import ROUTES_PATHS__TEST_DATA
from mgraph_ai_service_cache.service.cache.Cache__Service                            import Cache__Service
from mgraph_ai_service_cache.service.cache.test_data.Cache__Service__Test_Data       import Cache__Service__Test_Data
from mgraph_ai_service_cache.schemas.test_data.Schema__Test_Data                     import Schema__Test_Data__Create__Response
from mgraph_ai_service_cache.schemas.test_data.Schema__Test_Data                     import Schema__Test_Data__Clear__Response
from tests.unit.Service__Cache__Test_Objs                                            import setup__service__cache__test_objs


class test_Routes__Test_Data(TestCase):

    @classmethod
    def setUpClass(cls):                                                             # ONE-TIME expensive setup
        skip__if_not__in_github_actions()
        cls.test_objs         = setup__service__cache__test_objs()
        cls.cache_fixtures    = cls.test_objs.cache_fixtures
        cls.cache_service     = cls.cache_fixtures.cache_service
        cls.routes_test_data  = Routes__Test_Data(cache_service=cls.cache_service)

        cls.test_namespace = Safe_Str__Id("routes-test-data")

    def test__init__(self):                                                          # Test initialization and structure
        with Routes__Test_Data() as _:
            assert type(_)                     is Routes__Test_Data
            assert base_classes(_)             == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                       == TAG__ROUTES_TEST_DATA
            assert _.prefix                    == PREFIX__ROUTES_TEST_DATA
            assert type(_.cache_service)       is Cache__Service
            assert type(_.test_data_service()) is Cache__Service__Test_Data

    def test__class_constants(self):                                                 # Test module-level constants
        assert TAG__ROUTES_TEST_DATA        == 'test-data'
        assert PREFIX__ROUTES_TEST_DATA     == '/test-data'
        assert len(ROUTES_PATHS__TEST_DATA) == 4
        assert ROUTES_PATHS__TEST_DATA[0]   == '/test-data/create/comprehensive'
        assert ROUTES_PATHS__TEST_DATA[1]   == '/test-data/create/namespace/{namespace}'
        assert ROUTES_PATHS__TEST_DATA[2]   == '/test-data/create/minimal'
        assert ROUTES_PATHS__TEST_DATA[3]   == '/test-data/clear/namespace/{namespace}'

    def test_test_data_service(self):                                                # Test service caching
        with self.routes_test_data as _:
            service1 = _.test_data_service()
            service2 = _.test_data_service()

            assert service1 is service2                                              # Same instance (cached)
            assert type(service1) is Cache__Service__Test_Data

    def test_create__minimal(self):                                                  # Test minimal data creation endpoint
        with self.routes_test_data as _:
            response = _.create__minimal()

            assert type(response)          is Schema__Test_Data__Create__Response
            assert response.success        is True
            assert response.entries_created == 3
            assert response.namespaces     == ['default']
            assert response.strategies_used == ['direct']
            assert len(response.entries)   == 3
            assert response.message        == 'Minimal test data created'

    def test_create__namespace(self):                                                # Test namespace-specific creation
        with self.routes_test_data as _:
            response = _.create__namespace(namespace=self.test_namespace)

            assert type(response)          is Schema__Test_Data__Create__Response
            assert response.success        is True
            assert response.entries_created > 0
            assert response.namespaces     == [str(self.test_namespace)]
            assert len(response.strategies_used) == 4

            for entry in response.entries:
                assert entry['namespace'] == str(self.test_namespace)

    def test_create__namespace__different_namespaces(self):                          # Test with various namespaces
        with self.routes_test_data as _:
            namespaces = ['ns-alpha', 'ns-beta', 'ns-gamma']

            for ns in namespaces:
                response = _.create__namespace(namespace=Safe_Str__Id(ns))
                assert response.success    is True
                assert response.namespaces == [ns]

    def test_create__comprehensive(self):                                            # Test comprehensive data creation
        with self.routes_test_data as _:
            response = _.create__comprehensive()

            assert type(response)                is Schema__Test_Data__Create__Response
            assert response.success              is True
            assert response.entries_created      > 20
            assert len(response.namespaces)      == 4
            assert 'default'                     in response.namespaces
            assert 'testing'                     in response.namespaces
            assert 'demo'                        in response.namespaces
            assert 'analytics'                   in response.namespaces
            assert len(response.strategies_used) == 4
            assert response.message              == 'Comprehensive test data created'

    def test_create__comprehensive__response_structure(self):                        # Verify response structure
        with self.routes_test_data as _:
            response = _.create__comprehensive()

            for entry in response.entries:                                           # Each entry has all expected fields
                assert 'cache_id'    in entry
                assert 'cache_hash'  in entry
                assert 'namespace'   in entry
                assert 'strategy'    in entry
                assert 'data_type'   in entry
                assert 'description' in entry

    def test_create__comprehensive__all_strategies_used(self):                       # Verify all strategies are present
        with self.routes_test_data as _:
            response = _.create__comprehensive()

            strategies_in_entries = set(e['strategy'] for e in response.entries)
            assert 'direct'          in strategies_in_entries
            assert 'temporal'        in strategies_in_entries
            assert 'temporal_latest' in strategies_in_entries
            assert 'key_based'       in strategies_in_entries

    def test_create__comprehensive__all_namespaces_have_entries(self):               # All namespaces populated
        with self.routes_test_data as _:
            response = _.create__comprehensive()

            namespaces_in_entries = set(e['namespace'] for e in response.entries)
            assert 'test-data'   in namespaces_in_entries
            assert 'test-demo'      in namespaces_in_entries
            assert 'test-analytics' in namespaces_in_entries

    def test_clear__namespace(self):                                                 # Test namespace clearing
        with self.routes_test_data as _:
            clear_ns = Safe_Str__Id('routes-clear-test')

            create_response = _.create__namespace(namespace=clear_ns)                                  # Create data first
            assert create_response.obj() == __(success          = True      ,
                                               timestamp        = __SKIP__  ,
                                               entries_created  = 37        ,
                                               namespaces       = ['routes-clear-test'],
                                               strategies_used  = ['direct', 'temporal', 'temporal_latest', 'key_based'],
                                               entries          =__SKIP__,
                                               message          = 'Test data created for routes-clear-test')

            response = _.clear__namespace(namespace=clear_ns)                        # Then clear

            assert type(response)       is Schema__Test_Data__Clear__Response
            assert response.obj()       == __(success=True,
                                              namespace='routes-clear-test',
                                              files_deleted=185,
                                              message='Cleared 185 files from routes-clear-test',
                                              error='')
            assert response.success       is True
            assert response.namespace     == clear_ns
            assert response.files_deleted  > 0
            assert response.error         == ''

    def test_clear__namespace__empty(self):                                          # Test clearing empty namespace
        with self.routes_test_data as _:
            response = _.clear__namespace(namespace=Safe_Str__Id('never-existed-ns'))

            assert response.success       is True
            assert response.files_deleted == 0

    def test_clear__namespace__response_structure(self):                             # Verify clear response structure
        with self.routes_test_data as _:
            response = _.clear__namespace(namespace=Safe_Str__Id('struct-test-ns'))

            assert hasattr(response, 'success')
            assert hasattr(response, 'namespace')
            assert hasattr(response, 'files_deleted')
            assert hasattr(response, 'message')
            assert hasattr(response, 'error')

    def test__integration__create_verify_clear(self):                                # Full integration cycle
        with self.routes_test_data as _:
            ns = Safe_Str__Id('full-integration-test')

            create_response = _.create__namespace(namespace=ns)                      # Create
            assert create_response.success        is True
            assert create_response.entries_created > 0
            initial_count = create_response.entries_created

            clear_response = _.clear__namespace(namespace=ns)                        # Clear
            assert clear_response.success is True

    def test__integration__minimal_creates_valid_entries(self):                      # Verify minimal entries are valid
        with self.routes_test_data as _:
            response = _.create__minimal()

            for entry in response.entries:                                           # All entries have non-empty IDs
                assert entry['cache_id']   != ''
                assert entry['cache_hash'] != ''

    def test__integration__multiple_calls_additive(self):                            # Multiple creates are additive
        with self.routes_test_data as _:
            ns = Safe_Str__Id('additive-test')

            response1 = _.create__namespace(namespace=ns)                            # First create
            count1    = response1.entries_created

            response2 = _.create__namespace(namespace=ns)                            # Second create - adds more
            count2    = response2.entries_created

            assert count2 == count1                                                  # Same amount each time

    def test__routes_delegate_to_service(self):                                      # Verify routes are thin wrappers
        with self.routes_test_data as _:
            service_response = _.test_data_service().create_minimal()                # Call service directly
            route_response   = _.create__minimal()                                   # Call via route

            assert type(service_response) == type(route_response)                    # Same response types
            assert service_response.success == route_response.success
            assert service_response.namespaces == route_response.namespaces
            assert service_response.strategies_used == route_response.strategies_used

    def test__setup_routes(self):                                                    # Test route registration
        with Routes__Test_Data() as _:
            _.setup_routes()                                                         # Should not raise

            assert _.tag    == 'test-data'                                           # Routes should be configured
            assert _.prefix == '/test-data'