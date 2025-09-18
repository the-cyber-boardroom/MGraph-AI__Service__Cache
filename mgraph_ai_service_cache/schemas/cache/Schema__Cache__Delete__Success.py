from typing                                                                       import List
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id

# Response for delete operations
class Schema__Cache__Delete__Success(Type_Safe):                                      # Successful deletion
    cache_id       : Random_Guid                                                      # ID that was deleted
    namespace      : Safe_Str__Id                                                     # Namespace it was in
    deleted_count  : Safe_UInt                                                        # Number of files deleted
    deleted_paths  : List[Safe_Str__File__Path]                                       # Paths that were deleted
    message        : Safe_Str__Text    = "Cache entry successfully deleted"           # Success message
