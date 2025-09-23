from fastapi                                                                    import FastAPI
from mgraph_ai_service_cache.service.cache.Cache__Service                       import Cache__Service
from osbot_fast_api.api.Fast_API                                                import ENV_VAR__FAST_API__AUTH__API_KEY__NAME, ENV_VAR__FAST_API__AUTH__API_KEY__VALUE
from osbot_utils.helpers.duration.decorators.capture_duration                   import capture_duration
from osbot_utils.type_safe.Type_Safe                                            import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_Float                           import Safe_Float
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid           import Random_Guid
from osbot_utils.utils.Env                                                      import set_env
from mgraph_ai_service_cache.fast_api.Service__Fast_API                         import Service__Fast_API
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                     import  CACHE__TEST__FIXTURES__NAMESPACE
from mgraph_ai_service_cache.utils.testing.Cache__Test__Fixtures                import Cache__Test__Fixtures

TEST_API_KEY__NAME = 'key-used-in-pytest'
TEST_API_KEY__VALUE = Random_Guid()

class Service__Cache__Test_Objs(Type_Safe):
    fast_api        : Service__Fast_API     = None
    fast_api__app   : FastAPI               = None
    cache_fixtures  : Cache__Test__Fixtures = None
    cache_service   : Cache__Service        = None
    setup_completed : bool                  = False
    duration        : Safe_Float            = None

service_fast_api_test_objs = Service__Cache__Test_Objs()

def setup_cache_fixtures(cache_service: Cache__Service):
    cache_fixtures = Cache__Test__Fixtures(cache_service      = cache_service                                    ,
                                           namespace         = CACHE__TEST__FIXTURES__NAMESPACE                  ,
                                           manifest_cache_id = Random_Guid("00000000-0000-0000-0000-000000000001")) # Predictable
    return cache_fixtures.setup()

def setup__service__cache__test_objs():
        with service_fast_api_test_objs as _:
            if _.setup_completed is False:

                set_env(ENV_VAR__FAST_API__AUTH__API_KEY__NAME  , TEST_API_KEY__NAME                )
                set_env(ENV_VAR__FAST_API__AUTH__API_KEY__VALUE , TEST_API_KEY__VALUE               )

                with capture_duration() as load_duration:
                    _.fast_api         = Service__Fast_API().setup()
                    _.fast_api__app    = _.fast_api.app()
                    _.fast_api__client = _.fast_api.client()
                    #_.local_stack      = setup_local_stack()
                    _.cache_service    = _.fast_api.cache_service
                    _.cache_fixtures   = setup_cache_fixtures(cache_service = _.cache_service)
                    _.setup_completed  = True

                _.duration = load_duration.seconds

            return _