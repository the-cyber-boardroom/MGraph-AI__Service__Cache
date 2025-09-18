from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Base            import Schema__Cache__Error__Base


class Schema__Cache__Error__Service_Unavailable(Schema__Cache__Error__Base):         # 503 Service Unavailable errors
    service_name : Safe_Str__Id                                                       # Which service is unavailable (s3, cache, etc.)
    retry_after  : int                  = None                                        # Seconds to wait before retry
    details      : Safe_Str__Text      = None                                         # Additional details about the issue