from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                    import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type             import Enum__Cache__Data_Type


class Schema__Cache__Data__Retrieve__Request(Type_Safe):
    cache_id     : Random_Guid               = None
    data_type    : Enum__Cache__Data_Type    = None  # Required: 'string', 'json', or 'binary'
    data_key     : Safe_Str__File__Path      = None
    data_file_id : Safe_Str__Id              = None
    namespace    : Safe_Str__Id              = DEFAULT_CACHE__NAMESPACE