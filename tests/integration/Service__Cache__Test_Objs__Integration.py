from osbot_aws.testing.Temp__Random__AWS_Credentials                        import Temp_AWS_Credentials
from osbot_local_stack.local_stack.Local_Stack                              import Local_Stack
from osbot_utils.utils.Env                                                  import set_env
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Storage_Mode  import Enum__Cache__Storage_Mode
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                 import ENV_VAR__CACHE__SERVICE__BUCKET_NAME, CACHE__TEST__FIXTURES__BUCKET_NAME
from mgraph_ai_service_cache.schemas.consts.const__Storage                  import ENV_VAR__CACHE__SERVICE__STORAGE_MODE
from tests.unit.Service__Cache__Test_Objs                                   import Service__Cache__Test_Objs


class Service__Cache__Test_Objs__Integration(Service__Cache__Test_Objs):
    local_stack     : Local_Stack           = None

def setup_local_stack() -> Local_Stack:
    Temp_AWS_Credentials().with_localstack_credentials()
    local_stack = Local_Stack().activate()
    return local_stack

def set_up_env_vars():
    set_env(ENV_VAR__CACHE__SERVICE__BUCKET_NAME , CACHE__TEST__FIXTURES__BUCKET_NAME)
    set_env(ENV_VAR__CACHE__SERVICE__STORAGE_MODE, Enum__Cache__Storage_Mode.S3.value)


service_fast_api_test_objs__integration = Service__Cache__Test_Objs__Integration()

def setup__service__cache__test_objs__integration():
    with service_fast_api_test_objs__integration as _:
        if _.setup_completed is False:
            set_up_env_vars()
            _.local_stack = setup_local_stack()
        return _
