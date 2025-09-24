from typing                                                                                 import List, Dict
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                                        import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id

from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type import Enum__Cache__Data_Type


class Schema__Cache__Store__Data__Response(Type_Safe):
    cache_id           : Random_Guid                            = None
    data_files_created : List[Safe_Str__File__Path]             = None
    data_key           : Safe_Str__File__Path                   = None
    data_type          : Enum__Cache__Data_Type                 = None          # Type: 'string', 'json', or 'binary'
    extension          : Safe_Str__Id                           = None
    file_id            : Safe_Str__Id                           = None
    file_size          : Safe_UInt                              = None          # Size in bytes
    namespace          : Safe_Str__Id                           = None
    timestamp          : Timestamp_Now
