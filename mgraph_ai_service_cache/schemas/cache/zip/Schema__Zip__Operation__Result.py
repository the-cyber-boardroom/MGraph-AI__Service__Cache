from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text                     import Safe_Str__Text
from mgraph_ai_service_cache.schemas.cache.zip.safe_str.Safe_Str__Cache__Zip__Operation__Message import Safe_Str__Cache__Zip__Operation__Message


class Schema__Zip__Operation__Result(Type_Safe):                              # Result of individual operation
    action       : Safe_Str__Text                                             # Operation performed
    path         : Safe_Str__File__Path                                       # File affected
    success      : bool                                                       # Whether it succeeded
    error        : Safe_Str__Cache__Zip__Operation__Message          = None   # Error if failed
