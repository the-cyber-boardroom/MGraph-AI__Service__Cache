from typing                                                                                 import Union, Dict
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash    import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now            import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.store.Schema__Cache__Store__Metadata             import Schema__Cache__Store__Metadata
from mgraph_ai_service_cache.schemas.cache.store.Schema__Cache__Store__Paths                import Schema__Cache__Store__Paths
from mgraph_ai_service_cache.service.cache.Cache__Handler                                   import Cache__Handler


class Schema__Store__Context(Type_Safe):                                                     # Context object to pass data between store operations
    storage_data     : Union[str, Dict, bytes]                                               # Data to be stored (string, dict, or bytes)
    cache_hash       : Safe_Str__Cache_Hash         = None                                   # Hash of the data or cache key
    cache_id         : Random_Guid                  = None                                   # Unique ID for this cache entry
    cache_key        : Safe_Str__File__Path         = None                                   # Optional semantic cache key
    file_id          : Safe_Str__Id                 = None                                   # Optional file ID (defaults to cache_id)
    namespace        : Safe_Str__Id                 = None                                   # Namespace for isolation
    strategy         : Enum__Cache__Store__Strategy = None                                   # Storage strategy to use
    content_encoding : Safe_Str__Id                 = None                                   # Optional encoding (e.g., 'gzip')
    handler          : Cache__Handler               = None                                   # Cache handler for the namespace

    # Computed during storage process (now using Type_Safe classes)
    file_type        : str                              = None                               # Determined type: 'json' or 'binary'
    file_size        : int                              = None                               # Size of stored data in bytes
    all_paths        : Schema__Cache__Store__Paths      = None                               # Paths organized by type
    content_paths    : list                             = None                               # Paths to actual content files (will be List[Safe_Str__File__Path])
    timestamp        : Timestamp_Now                    = None                               # When the entry was stored
    metadata         : Schema__Cache__Store__Metadata   = None