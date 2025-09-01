from typing                                                                        import List
from osbot_utils.type_safe.Type_Safe                                               import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path     import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid             import Random_Guid
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                   import Safe_Str__Cache_Hash


class Schema__Cache__Store__Response(Type_Safe):                                   # Response schema for store operations
    cache_id : Random_Guid                                                         # Unique ID for this cache entry
    hash     : Safe_Str__Cache_Hash           = None                               # Content hash
    paths    : List[Safe_Str__File__Path]                                          # Storage paths
    size     : int                                                                 # Size in bytes