from typing                                                                         import Union
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.type_safe.primitives.core.Safe_UInt                                import Safe_UInt
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type             import Enum__Cache__Data_Type


class Schema__Cache__Data__Retrieve__Response(Type_Safe):                   # Data file with its content
    data         : Union[str, dict, bytes]                  = None          # Actual file content
    data_type    : Enum__Cache__Data_Type                   = None          # Data type: json, string, binary
    data_file_id : Safe_Str__Id                             = None          # Data file identifier
    data_key     : Safe_Str__File__Path                     = None          # Path within data folder
    full_path    : Safe_Str__File__Path                     = None          # Full file path
    size         : Safe_UInt                                = None          # File size in bytes
    found        : bool                                     = False         # Indicates if file was found
