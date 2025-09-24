from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now            import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__File_Type                     import Enum__Cache__File_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy               import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.file.Schema__Cache__File__Paths                  import Schema__Cache__File__Paths
from mgraph_ai_service_cache.schemas.cache.store.Schema__Cache__Store__Paths                import Schema__Cache__Store__Paths

# todo: review the field name 'all_paths' (in light of the new file_paths field)
class Schema__Cache__File__Refs(Type_Safe):                # ID-to-hash reference with content paths
    all_paths        : Schema__Cache__Store__Paths         # All file paths created
    cache_id         : Random_Guid                         # Cache ID
    cache_hash       : Safe_Str__Cache_Hash                # Hash value
    file_type        : Enum__Cache__File_Type              # Type of stored data
    namespace        : Safe_Str__Id                        # Namespace
    file_paths       : Schema__Cache__File__Paths          # Paths to content and data files
    strategy         : Enum__Cache__Store__Strategy        # Storage strategy
    timestamp        : Timestamp_Now                       # When created