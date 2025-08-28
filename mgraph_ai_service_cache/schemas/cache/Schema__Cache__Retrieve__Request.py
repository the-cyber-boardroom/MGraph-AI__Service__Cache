from osbot_utils.type_safe.Type_Safe                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id  import Safe_Id
from osbot_utils.type_safe.primitives.safe_uint.Safe_UInt           import Safe_UInt
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__SHA1__Short   import Safe_Str__SHA1__Short


class Schema__Cache__Retrieve__Request(Type_Safe):  # Request schema for retrieving cache data
    hash            : Safe_Str__SHA1__Short = None
    cache_id        : Safe_Id               = None
    version         : Safe_UInt             = None          # todo: see if we can have this here
    include_data    : bool                  = True
    include_metadata: bool                  = True
    include_config  : bool                  = True