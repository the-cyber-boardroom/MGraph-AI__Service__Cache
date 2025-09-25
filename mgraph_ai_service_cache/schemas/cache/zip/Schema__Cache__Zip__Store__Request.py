from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                    import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy       import Enum__Cache__Store__Strategy


class Schema__Cache__Zip__Store__Request(Type_Safe):                                    # Request schema for storing zip files
    zip_bytes   : bytes                                                                 # Raw zip file content
    cache_key   : Safe_Str__File__Path         = None                                   # Optional semantic key for the zip
    file_id     : Safe_Str__Id                 = None                                   # Optional file ID (defaults to cache_id)
    namespace   : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE               # Namespace for isolation
    strategy    : Enum__Cache__Store__Strategy = Enum__Cache__Store__Strategy.TEMPORAL  # Storage strategy