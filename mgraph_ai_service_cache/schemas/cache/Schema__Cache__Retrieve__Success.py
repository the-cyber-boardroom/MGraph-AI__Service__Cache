from typing                                                                       import Dict, Any, Optional, Union
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Metadata                import Schema__Cache__Metadata
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type           import Enum__Cache__Data_Type

# Success response for retrieving data
class Schema__Cache__Retrieve__Success(Type_Safe):                                    # Successful retrieval with data
    data      : Union[dict, str, bytes]                                               # Actual cached content
    metadata  : Schema__Cache__Metadata                                               # Cache metadata
    data_type : Enum__Cache__Data_Type                                                # Type of data returned
