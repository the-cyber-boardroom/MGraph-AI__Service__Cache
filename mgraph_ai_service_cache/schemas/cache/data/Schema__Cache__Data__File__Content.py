from typing                                                                         import Any
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.type_safe.primitives.core.Safe_UInt                                import Safe_UInt
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type             import Enum__Cache__Data_Type


class Schema__Cache__Data__File__Content(Type_Safe):                                                   # Data file with its content
    data         : Any                                                                          # Actual file content
    data_type    : Enum__Cache__Data_Type                                                       # Data type: json, string, binary
    data_file_id : Safe_Str__Id                                                                 # Data file identifier
    data_key     : Safe_Str__File__Path                                                         # Path within data folder
    full_path    : Safe_Str__File__Path                                                         # Full file path
    size         : Safe_UInt                                                                    # File size in bytes
