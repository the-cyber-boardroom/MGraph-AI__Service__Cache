from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now  import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from memory_fs.schemas.Safe_Str__Cache_Hash                                       import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type           import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy     import Enum__Cache__Store__Strategy


# Base response metadata that all responses share
class Schema__Cache__Metadata(Type_Safe):                                             # Metadata about cached entry
    cache_id         : Random_Guid                  = None                            # Unique ID of this cache entry
    cache_hash       : Safe_Str__Cache_Hash         = None                            # Content hash
    cache_key        : Safe_Str__Text               = None                            # Optional semantic key
    file_id          : Safe_Str__Id                 = None                            # Optional file ID
    namespace        : Safe_Str__Id                 = None                            # Namespace for isolation
    strategy         : Enum__Cache__Store__Strategy = None                            # Storage strategy used
    stored_at        : Timestamp_Now                = None                            # When stored (timestamp)
    file_type        : Enum__Cache__Data_Type       = None                            # Type of data
    content_encoding : Safe_Str__Id                 = None                            # e.g., gzip
    content_size     : Safe_UInt                    = None                            # Size in bytes









