from typing                                                                                      import List
from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                                             import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                            import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now                 import Timestamp_Now
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Zip__Operation__Result                    import Schema__Zip__Operation__Result
from mgraph_ai_service_cache.schemas.cache.zip.safe_str.Safe_Str__Cache__Zip__Operation__Message import Safe_Str__Cache__Zip__Operation__Message


class Schema__Cache__Zip__Batch__Response(Type_Safe):                                   # Response from batch operations
    success              : bool                                                         # Overall success status
    cache_id             : Random_Guid              = None                              # Original zip ID
    original_cache_id    : Random_Guid              = None                              # Original ID for provenance
    operations_applied   : Safe_UInt                                                    # Number of successful operations
    operations_failed    : Safe_UInt                                                    # Number of failed operations
    operation_results    : List[Schema__Zip__Operation__Result]                         # Individual results
    files_added          : List[Safe_Str__File__Path]                                   # Files that were added
    files_removed        : List[Safe_Str__File__Path]                                   # Files that were removed
    files_modified       : List[Safe_Str__File__Path]                                   # Files that were modified
    new_file_count       : Safe_UInt                                                    # Total files after operations
    new_size             : Safe_UInt                                                    # Total size after operations
    completed_at         : Timestamp_Now                                                # When batch completed
    rollback_performed   : bool                                       = False           # Whether rollback occurred
    error_message        : Safe_Str__Cache__Zip__Operation__Message                     # Overall error if atomic failed