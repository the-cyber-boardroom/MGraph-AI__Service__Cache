from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash                                       import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Base            import Schema__Cache__Error__Base


class Schema__Cache__Error__Not_Found(Schema__Cache__Error__Base):  # 404 Not Found errors
    resource_type : Safe_Str__Id                                    # Type of resource (cache_entry, namespace, etc.)
    resource_id   : Safe_Str__Id         = None                     # ID that wasn't found
    cache_hash    : Safe_Str__Cache_Hash = None                     # Hash that wasn't found (if applicable)
    cache_id      : Random_Guid          = None                     # Cache ID that wasn't found (if applicable)
    namespace     : Safe_Str__Id         = None                     # Namespace where search occurred
