from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Base            import Schema__Cache__Error__Base


class Schema__Cache__Error__Gone(Schema__Cache__Error__Base):                        # 410 Gone errors (expired entries)
    cache_id    : Random_Guid                                                        # ID that expired
    expired_at  : Safe_Str__Text                                                     # When it expired
    ttl_hours   : Safe_UInt                                                          # What the TTL was
    namespace   : Safe_Str__Id               = None                                       # Namespace of expired entry
