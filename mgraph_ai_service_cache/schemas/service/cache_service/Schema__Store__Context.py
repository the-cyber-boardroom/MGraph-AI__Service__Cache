from typing                                                                                     import Union, Dict
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__File_Type                  import Enum__Cache__File_Type
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Cache__File__Cache_Hash    import Safe_Str__Cache__File__Cache_Hash
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Cache__File__Cache_Key     import Safe_Str__Cache__File__Cache_Key
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Cache__File__File_Id       import Safe_Str__Cache__File__File_Id
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Cache__Namespace           import Safe_Str__Cache__Namespace
from osbot_utils.type_safe.Type_Safe                                                            import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                                            import Safe_UInt
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                              import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now                import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                 import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Json__Field_Path   import Safe_Str__Json__Field_Path
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__File__Paths               import Schema__Cache__File__Paths
from mgraph_ai_service_cache_client.schemas.cache.store.Schema__Cache__Store__Metadata          import Schema__Cache__Store__Metadata
from mgraph_ai_service_cache_client.schemas.cache.store.Schema__Cache__Store__Paths             import Schema__Cache__Store__Paths
from mgraph_ai_service_cache.service.cache.Cache__Handler                                       import Cache__Handler

# todo: review the use of Cache__Handler since it is this dependency that makes this entire schema to need to be placed in the mgraph_ai_service_cache project (instead of the mgraph_ai_service_cache__client project)


class Schema__Store__Context(Type_Safe):                                                        # Context object to pass data between store operations
    cache_hash        : Safe_Str__Cache__File__Cache_Hash   = None                              # Hash of the data or cache key
    cache_id          : Cache_Id                            = None                              # Unique ID for this cache entry
    content_encoding  : Safe_Str__Id                        = None                              # Optional encoding (e.g., 'gzip')
    cache_key         : Safe_Str__Cache__File__Cache_Key    = None                              # Optional semantic cache key
    file_id           : Safe_Str__Cache__File__File_Id      = None                              # Optional file ID (defaults to cache_id)
    handler           : Cache__Handler                      = None                              # Cache handler for the namespace
    json_field_path   : Safe_Str__Json__Field_Path          = None                              # Field Path used to calculate the hash of a Json object
    namespace         : Safe_Str__Cache__Namespace          = None                              # Namespace for isolation
    storage_data      : Union[str, Dict, bytes]                                                 # Data to be stored (string, dict, or bytes)
    strategy          : Enum__Cache__Store__Strategy        = None                              # Storage strategy to use


    # Computed during storage process (now using Type_Safe classes)
    file_type          : Enum__Cache__File_Type                       = None                    # Determined type: 'json' or 'binary'
    file_size          : Safe_UInt                          = None                              # Size of stored data in bytes
    all_paths          : Schema__Cache__Store__Paths        = None                              # Paths organized by type
    file_paths         : Schema__Cache__File__Paths                                             # Paths to actual content files and data folders
    timestamp          : Timestamp_Now                      = None                              # When the entry was stored
    metadata           : Schema__Cache__Store__Metadata     = None