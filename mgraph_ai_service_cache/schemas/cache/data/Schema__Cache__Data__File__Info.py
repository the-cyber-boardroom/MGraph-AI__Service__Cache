from typing                                                                         import Any
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text        import Safe_Str__Text
from osbot_utils.type_safe.primitives.core.Safe_UInt                                import Safe_UInt
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type             import Enum__Cache__Data_Type


class Schema__Cache__Data__File__Info(Type_Safe):                                                      # Information about a data file
    data_file_id : Safe_Str__Id                                                                 # Data file identifier
    data_key     : Safe_Str__File__Path                                                         # Path within data folder
    full_path    : Safe_Str__File__Path                                                         # Full file path
    data_type    : Enum__Cache__Data_Type                                                       # Data type: json, string, binary
    extension    : Safe_Str__Text                                                               # File extension
    size         : Safe_UInt