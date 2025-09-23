from typing                                                                                 import List, Dict
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                                        import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash    import Safe_Str__Cache_Hash


class Schema__Cache__Store__Response(Type_Safe):
    cache_id   : Random_Guid
    cache_hash : Safe_Str__Cache_Hash
    namespace  : Safe_Str__Id
    paths      : Dict[str,List[Safe_Str__File__Path]]                     # Structured paths
    size       : Safe_UInt                                                # Size in bytes