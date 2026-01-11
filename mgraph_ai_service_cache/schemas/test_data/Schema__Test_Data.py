from typing                                                                                 import List, Dict, Any
from osbot_utils.type_safe.Type_Safe                                                        import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text                import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash    import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                          import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now            import Timestamp_Now
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type              import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy        import Enum__Cache__Store__Strategy


class Schema__Test_Data__Entry(Type_Safe):                                       # Schema for individual created cache entry
    cache_id    : Cache_Id
    cache_hash  : Safe_Str__Cache_Hash
    namespace   : Safe_Str__Id
    strategy    : Enum__Cache__Store__Strategy
    data_type   : Enum__Cache__Data_Type
    description : Safe_Str__Text
    cache_key   : Safe_Str__File__Path
    children    : List[Dict[str, Any]]                                           # Optional: for entries with child data


class Schema__Test_Data__Create__Response(Type_Safe):                            # Response schema for test data creation
    success         : bool
    timestamp       : Timestamp_Now
    entries_created : int
    namespaces      : List[Safe_Str__Id]
    strategies_used : List[str]
    entries         : List[Dict[str, Any]]
    message         : Safe_Str__Text


class Schema__Test_Data__Clear__Response(Type_Safe):                             # Response schema for clearing namespace
    success       : bool
    namespace     : Safe_Str__Id
    files_deleted : int
    message       : Safe_Str__Text
    error         : Safe_Str__Text                                                 # Only populated on failure


class Schema__Test_Data__Sample_Json(Type_Safe):                                 # Schema for sample JSON test data
    type        : Safe_Str__Id
    description : Safe_Str__Text


class Schema__Test_Data__Key_Based_Item(Type_Safe):                              # Schema for key-based test data item
    cache_key : Safe_Str__Id
    file_id   : Safe_Str__Id
    data      : Dict[str, Any]


class Schema__Test_Data__Child_Item(Type_Safe):                                  # Schema for child data item
    data_key     : Safe_Str__Id
    data_file_id : Safe_Str__Id
    data         : Dict[str, Any]


class Schema__Test_Data__Zip_File(Type_Safe):                                    # Schema for ZIP file content
    path    : Safe_Str__Id
    content : bytes