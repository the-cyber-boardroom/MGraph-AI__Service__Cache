from unittest                                                               import TestCase
from fastapi                                                                import FastAPI
from osbot_fast_api.api.Fast_API                                            import ENV_VAR__FAST_API__AUTH__API_KEY__NAME, ENV_VAR__FAST_API__AUTH__API_KEY__VALUE
from osbot_fast_api.api.schemas.consts.consts__Fast_API                     import EXPECTED_ROUTES__SET_COOKIE
from osbot_utils.utils.Env                                                  import get_env
from starlette.testclient                                                   import TestClient
from mgraph_ai_service_cache.config                                         import ROUTES_PATHS__WEB_CONSOLE
from mgraph_ai_service_cache.fast_api.Cache_Service__Fast_API               import Cache_Service__Fast_API
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespaces             import ROUTES_PATHS__NAMESPACES
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Delete      import ROUTES_PATHS__DELETE__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Retrieve    import ROUTES_PATHS__RETRIEVE__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store       import ROUTES_PATHS__STORE__DATA
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Delete      import ROUTES_PATHS__DELETE
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Exists      import ROUTES_PATHS__EXISTS
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Retrieve    import ROUTES_PATHS__RETRIEVE
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store       import ROUTES_PATHS__STORE
from mgraph_ai_service_cache.fast_api.routes.Routes__Info                   import ROUTES_PATHS__INFO, ROUTES_INFO__HEALTH__RETURN_VALUE
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespace              import ROUTES_PATHS__NAMESPACE
from mgraph_ai_service_cache.fast_api.routes.Routes__Server                 import ROUTES_PATHS__SERVER
from mgraph_ai_service_cache.fast_api.routes.admin.Routes__Admin__Storage   import ROUTES_PATHS__STORAGE
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Update      import ROUTES_PATHS__UPDATE
from mgraph_ai_service_cache.fast_api.routes.test_data.Routes__Test_Data    import ROUTES_PATHS__TEST_DATA
from mgraph_ai_service_cache.fast_api.routes.zip.Routes__Zip                import ROUTES_PATHS__ZIP
from tests.unit.Service__Cache__Test_Objs                                   import setup__service__cache__test_objs, Service__Cache__Test_Objs, TEST_API_KEY__NAME


class test_Service__Fast_API__client(TestCase):

    @classmethod
    def setUpClass(cls):
        with setup__service__cache__test_objs() as _:
            cls.service_fast_api_test_objs         = _
            cls.fast_api                           = cls.service_fast_api_test_objs.fast_api
            cls.client                             = cls.service_fast_api_test_objs.fast_api__client
            cls.client.headers[TEST_API_KEY__NAME] = ''

    def test__init__(self):
        with self.service_fast_api_test_objs as _:
            assert type(_) is Service__Cache__Test_Objs
            assert type(_.fast_api        ) is Cache_Service__Fast_API
            assert type(_.fast_api__app   ) is FastAPI
            assert type(_.fast_api__client) is TestClient
            assert self.fast_api            == _.fast_api
            assert self.client              == _.fast_api__client

    def test__client__auth(self):
        path                = '/info/health'
        auth_key_name       = get_env(ENV_VAR__FAST_API__AUTH__API_KEY__NAME )
        auth_key_value      = get_env(ENV_VAR__FAST_API__AUTH__API_KEY__VALUE)
        headers             = {auth_key_name: auth_key_value}

        response__no_auth   = self.client.get(url=path, headers={})
        response__with_auth = self.client.get(url=path, headers=headers)

        assert response__no_auth.status_code == 401
        assert response__no_auth.json()      == { 'data'   : None,
                                                  'error'  : None,
                                                  'message': 'Client API key is missing, you need to set it on a header or cookie',
                                                  'status' : 'error'}

        assert auth_key_name                 is not None
        assert auth_key_value                is not None
        assert response__with_auth.json()    == ROUTES_INFO__HEALTH__RETURN_VALUE

    def test__config_fast_api_routes(self):
        #routes_paths   = []
        fast_api_paths = []
        raw_paths      = (ROUTES_PATHS__INFO           +
                          EXPECTED_ROUTES__SET_COOKIE  +
                          ROUTES_PATHS__STORE          +
                          ROUTES_PATHS__RETRIEVE       +
                          ROUTES_PATHS__EXISTS         +
                          ROUTES_PATHS__UPDATE         +
                          ROUTES_PATHS__DELETE         +
                          ROUTES_PATHS__NAMESPACE      +
                          ROUTES_PATHS__NAMESPACES     +
                          ROUTES_PATHS__SERVER         +
                          ROUTES_PATHS__STORAGE        +
                          ROUTES_PATHS__STORE__DATA    +
                          ROUTES_PATHS__RETRIEVE__DATA +
                          ROUTES_PATHS__DELETE__DATA   +
                          ROUTES_PATHS__ZIP            +
                          ROUTES_PATHS__WEB_CONSOLE    +
                          ROUTES_PATHS__TEST_DATA      )
        # for raw_path in raw_paths:
        #     routes_paths.append(raw_path)
        for fast_api_path in self.fast_api.routes_paths():
            fast_api_paths.append(str(fast_api_path))               # cast to str to make it easier compare

        assert sorted(fast_api_paths)       == sorted(raw_paths)                        # this creates a better diff
        assert self.fast_api.routes_paths() == sorted(raw_paths   )                     # but this also works :)