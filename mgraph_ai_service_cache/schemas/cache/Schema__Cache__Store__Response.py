from typing                                                                       import List, Dict
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                 import Safe_Id
from memory_fs.schemas.Safe_Str__Cache_Hash                  import Safe_Str__Cache_Hash


class Schema__Cache__Store__Response(Type_Safe):
    cache_id  : Random_Guid
    hash      : Safe_Str__Cache_Hash
    namespace : Safe_Id
    paths     : Dict[str,List[Safe_Str__File__Path]]                     # Structured paths
    size      : int                                                      # Size in bytes