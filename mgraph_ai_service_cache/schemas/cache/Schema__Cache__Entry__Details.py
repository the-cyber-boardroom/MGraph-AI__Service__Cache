from typing                                                                       import Dict, List
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash                                       import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type           import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy     import Enum__Cache__Store__Strategy

# Response for cache entry details
# todo: rename to Schema__Cache__Entry__Refs (and see if 'Entry' is the best term to use here)
class Schema__Cache__Entry__Details(Type_Safe):                                       # Detailed cache entry info
    cache_id       : Random_Guid                                                      # Cache ID
    cache_hash     : Safe_Str__Cache_Hash                                             # Content hash
    namespace      : Safe_Str__Id                                                     # Namespace
    strategy       : Enum__Cache__Store__Strategy                                     # Storage strategy
    all_paths      : Dict[Safe_Str__Id, List[Safe_Str__File__Path]]                   # All file paths
    content_paths  : List[Safe_Str__File__Path]                                       # Content file paths
    file_type      : Enum__Cache__Data_Type                                           # Type of data
    timestamp      : Safe_Str__Text                                                   # When stored