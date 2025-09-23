from typing                                                                                 import List
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now            import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__File_Type                     import Enum__Cache__File_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.store.Schema__Cache__Store__Paths                import Schema__Cache__Store__Paths

class Schema__Cache__File__Refs(Type_Safe):             # ID-to-hash reference with content paths
    cache_id      : Random_Guid                         # Cache ID
    cache_hash    : Safe_Str__Cache_Hash                # Hash value
    namespace     : Safe_Str__Id                        # Namespace
    strategy      : Enum__Cache__Store__Strategy        # Storage strategy
    all_paths     : Schema__Cache__Store__Paths         # All file paths created
    content_paths : List[Safe_Str__File__Path]          # Paths to content files
    file_type     : Enum__Cache__File_Type              # Type of stored data
    timestamp     : Timestamp_Now                       # When created