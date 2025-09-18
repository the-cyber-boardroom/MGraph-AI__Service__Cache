from typing                                                                       import Dict, Any, Optional
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Base            import Schema__Cache__Error__Base

class Schema__Cache__Error__Invalid_Input(Schema__Cache__Error__Base):                # 400 Bad Request errors
    field_name    : Safe_Str__Id                                                      # Which field had invalid input
    field_value   : Safe_Str__Text     = None                                         # The invalid value (if safe to include)
    expected_type : Safe_Str__Text                                                    # What type/format was expected
    constraints   : Dict[str, Any]     = None                                         # Any constraints that were violated
