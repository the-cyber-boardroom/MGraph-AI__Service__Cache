from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                  import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.zip.enums.Enum__Cache__Zip__Operation  import Enum__Cache__Zip__Operation


class Schema__Cache__Zip__Operation__Request(Type_Safe):                # Request for single zip operation
    cache_id    : Random_Guid                                           # ID of the zip file to operate on
    operation   : Enum__Cache__Zip__Operation                           # Operation type
    file_path   : Safe_Str__File__Path      = None                      # Path within zip (for get/add/remove/replace)
    file_content: bytes                     = None                      # Content for add/replace operations
    namespace   : Safe_Str__Id              = DEFAULT_CACHE__NAMESPACE  # Namespace for isolation