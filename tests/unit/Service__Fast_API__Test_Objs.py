from fastapi                                                                    import FastAPI
from osbot_aws.testing.Temp__Random__AWS_Credentials                            import Temp_AWS_Credentials
from osbot_fast_api.api.Fast_API                                                import ENV_VAR__FAST_API__AUTH__API_KEY__NAME, ENV_VAR__FAST_API__AUTH__API_KEY__VALUE
from osbot_local_stack.local_stack.Local_Stack                                  import Local_Stack
from osbot_utils.helpers.duration.decorators.capture_duration                   import capture_duration
from osbot_utils.type_safe.Type_Safe                                            import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_Float                           import Safe_Float
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid           import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id import Safe_Str__Id
from osbot_utils.utils.Env                                                      import set_env
from starlette.testclient                                                       import TestClient
from mgraph_ai_service_cache.fast_api.Service__Fast_API                         import Service__Fast_API
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                     import ENV_VAR__CACHE__SERVICE__BUCKET_NAME, CACHE__TEST__FIXTURES__BUCKET_NAME
from mgraph_ai_service_cache.utils.testing.Cache__Test__Fixtures                import Cache__Test__Fixtures

TEST_API_KEY__NAME = 'key-used-in-pytest'
TEST_API_KEY__VALUE = Random_Guid()

class Service__Fast_API__Test_Objs(Type_Safe):
    fast_api        : Service__Fast_API     = None
    fast_api__app   : FastAPI               = None
    #fast_api__client: TestClient            = None
    cache_fixtures  : Cache__Test__Fixtures = None
    local_stack     : Local_Stack           = None
    setup_completed : bool                  = False
    duration        : Safe_Float            = None

service_fast_api_test_objs = Service__Fast_API__Test_Objs()

# todo: move this to the integration tests (since the unit tests should run all in memory)
# def setup_local_stack() -> Local_Stack:
#     Temp_AWS_Credentials().with_localstack_credentials()
#     local_stack = Local_Stack().activate()
#     return local_stack

def setup_cache_fixtures():
    cache_fixtures = Cache__Test__Fixtures(namespace         = Safe_Str__Id("test-fixtures")                       ,
                                           manifest_cache_id = Random_Guid("00000000-0000-0000-0000-000000000001")) # Predictable
    if False:               # todo: find better way to reset the db
        cache_fixtures.setup()
        cache_fixtures.cleanup_all()
    return cache_fixtures.setup()

def setup__service_fast_api_test_objs():
        with service_fast_api_test_objs as _:
            if service_fast_api_test_objs.setup_completed is False:

                set_env(ENV_VAR__FAST_API__AUTH__API_KEY__NAME  , TEST_API_KEY__NAME                )
                set_env(ENV_VAR__FAST_API__AUTH__API_KEY__VALUE , TEST_API_KEY__VALUE               )
                #set_env(ENV_VAR__CACHE__SERVICE__BUCKET_NAME    , CACHE__TEST__FIXTURES__BUCKET_NAME)

                with capture_duration() as load_duration:
                    _.fast_api         = Service__Fast_API().setup()
                    _.fast_api__app    = _.fast_api.app()
                    _.fast_api__client = _.fast_api.client()
                    #_.local_stack      = setup_local_stack()
                    _.cache_fixtures   = setup_cache_fixtures()
                    _.setup_completed  = True



                _.duration = load_duration.seconds


        return service_fast_api_test_objs