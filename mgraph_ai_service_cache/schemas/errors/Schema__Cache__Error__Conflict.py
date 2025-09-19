from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash                                       import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Base            import Schema__Cache__Error__Base


class Schema__Cache__Error__Conflict(Schema__Cache__Error__Base):                    # 409 Conflict errors
    conflict_type : Safe_Str__Id                                                     # Type of conflict (duplicate, locked, etc.)
    existing_id   : Random_Guid           = None                                     # Existing entry that conflicts
    cache_hash    : Safe_Str__Cache_Hash = None                                      # Hash involved in conflict
