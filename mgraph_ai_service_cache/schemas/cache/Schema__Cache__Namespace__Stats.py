from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id

# Response for namespace statistics
class Schema__Cache__Namespace__Stats(Type_Safe):                                        # Namespace statistics
    namespace                : Safe_Str__Id                                              # Namespace name
    s3_bucket                : Safe_Str__Id                                              # S3 bucket name
    s3_prefix                : Safe_Str__Text                                            # S3 prefix path
    ttl_hours                : Safe_UInt                                                 # TTL configuration
    direct_files             : Safe_UInt                                                 # Files in direct strategy
    temporal_files           : Safe_UInt                                                 # Files in temporal strategy
    temporal_latest_files    : Safe_UInt                                                 # Files in temporal_latest
    temporal_versioned_files : Safe_UInt                                                 # Files in temporal_versioned
    refs_hash_files          : Safe_UInt                                                 # Hash reference files
    refs_id_files            : Safe_UInt                                                 # ID reference files
    total_files              : Safe_UInt                                                 # Total file count



