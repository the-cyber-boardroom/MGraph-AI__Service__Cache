from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                import Safe_Str__File__Path
from mgraph_ai_service_cache.schemas.cache.zip.enums.Enum__Cache__Zip__Condition                 import Enum__Cache__Zip__Condition
from mgraph_ai_service_cache.schemas.cache.zip.enums.Enum__Cache__Zip__Operation                 import Enum__Cache__Zip__Operation
from mgraph_ai_service_cache.schemas.cache.zip.safe_str.Safe_Str__Cache__Zip__Operation__Pattern import Safe_Str__Cache__Zip__Operation__Pattern


class Schema__Zip__Batch__Operation(Type_Safe):                                         # Individual operation in batch
    action       : Enum__Cache__Zip__Operation                                          # Operation type
    path         : Safe_Str__File__Path                                                 # File path in zip
    content      : bytes                                    = None                                   # Content for add/replace
    new_path     : Safe_Str__File__Path                     = None                                   # New path for rename/move
    condition    : Enum__Cache__Zip__Condition                        = Enum__Cache__Zip__Condition.ALWAYS     # Conditional execution
    pattern      : Safe_Str__Cache__Zip__Operation__Pattern = None                                   # Pattern for bulk operations (e.g., "*.tmp")
