from typing                                                                                      import List, Dict, Optional
from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                                             import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                            import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                  import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash         import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now                 import Timestamp_Now
from mgraph_ai_service_cache.schemas.cache.zip.safe_str.Safe_Str__Cache__Zip__Operation__Message import Safe_Str__Cache__Zip__Operation__Message

class Schema__Cache__Zip__Store__Response(Type_Safe):                                   # Response after storing zip file
    cache_id         : Optional[Random_Guid]                               = None       # Generated ID for this zip entry
    cache_hash       : Optional[Safe_Str__Cache_Hash]                      = None       # Hash of the zip content
    namespace        : Safe_Str__Id                                                     # Namespace where stored
    paths            : Dict[Safe_Str__Id, List[Safe_Str__File__Path]]                   # Storage paths by type
    size             : Safe_UInt                                                        # Size in bytes
    file_count       : Safe_UInt                                                        # Number of files in zip
    stored_at        : Timestamp_Now                                                    # When stored
    error_type       : Optional[Safe_Str__Id]                              = None       # Error classification
    error_message    : Optional[Safe_Str__Cache__Zip__Operation__Message]  = None       # Human-readable error
    success          : bool                                                             # Operation success status



