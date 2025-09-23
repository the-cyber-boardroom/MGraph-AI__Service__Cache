from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text             import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now         import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__File_Type                  import Enum__Cache__File_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy


class Schema__Cache__Store__Metadata(Type_Safe):                                             # Metadata for stored cache entries
    cache_hash       : Safe_Str__Cache_Hash             = None                               # Hash of the data
    cache_key        : Safe_Str__Text                   = None                               # Optional semantic key
    cache_id         : Random_Guid                      = None                               # Cache entry ID
    content_encoding : Safe_Str__Id                     = None                               # Optional encoding
    file_id          : Safe_Str__Id                     = None                               # File ID used for storage
    stored_at        : Timestamp_Now                    = None                               # When stored
    strategy         : Enum__Cache__Store__Strategy     = None                               # Storage strategy used
    namespace        : Safe_Str__Id                     = None                               # Namespace
    file_type        : Enum__Cache__File_Type           = None                               # Type: 'json' or 'binary'