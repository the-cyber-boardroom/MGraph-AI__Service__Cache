from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Version import Safe_Str__Version
from osbot_utils.type_safe.Type_Safe                                 import Type_Safe
from osbot_utils.utils.Version                                       import Version as Version__OSBot_Utils
from osbot_aws.utils.Version                                         import Version as Version__OSBot_AWS
from osbot_fast_api.utils.Version                                    import version__osbot_fast_api
from osbot_fast_api_serverless.utils.Version                         import version__osbot_fast_api_serverless
from mgraph_ai_service_cache.utils.Version                            import version__mgraph_ai_service_cache


class Schema__Server__Versions(Type_Safe):
    mgraph_ai_service_cache   : Safe_Str__Version       = version__mgraph_ai_service_cache
    osbot_utils              : Safe_Str__Version       = Safe_Str__Version(Version__OSBot_Utils().value()    )
    osbot_aws                : Safe_Str__Version       = Safe_Str__Version(Version__OSBot_AWS  ().value()    )
    osbot_fast_api           : Safe_Str__Version       = Safe_Str__Version(version__osbot_fast_api           )
    osbot_fast_api_serverless: Safe_Str__Version        =Safe_Str__Version(version__osbot_fast_api_serverless)
