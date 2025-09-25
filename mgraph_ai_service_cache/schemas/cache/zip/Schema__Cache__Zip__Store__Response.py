from typing                                                                              import List, Dict
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                                     import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now         import Timestamp_Now


class Schema__Cache__Zip__Store__Response(Type_Safe):                                   # Response after storing zip file
    cache_id         : Random_Guid                                                      # Generated ID for this zip entry
    cache_hash       : Safe_Str__Cache_Hash                                             # Hash of the zip content
    namespace        : Safe_Str__Id                                                     # Namespace where stored
    # todo: review the use of str in this 'paths' var
    paths            : Dict[str, List[Safe_Str__File__Path]]                            # Storage paths by type
    size             : Safe_UInt                                                        # Size in bytes
    file_count       : Safe_UInt                                                        # Number of files in zip
    stored_at        : Timestamp_Now                                                    # When stored
    # todo: review the use of str in this 'compression_info' var
    compression_info : Dict[str, Safe_UInt]                                             # Compression stats