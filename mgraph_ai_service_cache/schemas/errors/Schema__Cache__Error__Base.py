from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now           import Timestamp_Now


# todo: should we add here the status_code value?
class Schema__Cache__Error__Base(Type_Safe):           # Base error response schema that all error responses inherit from
    error_type  : Safe_Str__Id                         # Type of error (e.g., NOT_FOUND, INVALID_INPUT)
    message     : Safe_Str__Text                       # Human-readable error message
    timestamp   : Timestamp_Now                        # When the error occurred
    request_id  : Random_Guid                          # Unique ID for this request (for tracing)