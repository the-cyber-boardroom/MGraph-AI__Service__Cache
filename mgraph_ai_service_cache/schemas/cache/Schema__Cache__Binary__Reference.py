from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from memory_fs.schemas.Safe_Str__Cache_Hash                                       import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Metadata                import Schema__Cache__Metadata
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type           import Enum__Cache__Data_Type

# Response when binary data can't be included in JSON
class Schema__Cache__Binary__Reference(Type_Safe):                                    # Reference to binary data
    message      : Safe_Str__Text                                                     # Explanation message
    data_type    : Enum__Cache__Data_Type                                             # Always BINARY
    size         : Safe_UInt                                                          # Size of binary data
    cache_hash   : Safe_Str__Cache_Hash                                               # Hash of the data
    cache_id     : Random_Guid                                                        # ID of the entry
    namespace    : Safe_Str__Id                                                            # Namespace
    binary_url   : Safe_Str__Text                                                     # URL to retrieve binary data
    metadata     : Schema__Cache__Metadata                                            # Full metadata


