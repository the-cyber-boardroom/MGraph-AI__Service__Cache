from typing                                                         import Optional, Dict, Any, List
from osbot_utils.helpers.Safe_Id                                    import Safe_Id
from osbot_utils.helpers.safe_str.Safe_Str__Text                    import Safe_Str__Text
from osbot_utils.helpers.safe_str.http.Safe_Str__Http__Content_Type import Safe_Str__Http__Content_Type
from osbot_utils.type_safe.Type_Safe                                import Type_Safe
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__SHA1__Short   import Safe_Str__SHA1__Short


class Schema__Cache__Store__Request(Type_Safe):          # Request schema for storing cache data"""
    data        : str                                    # Base64 encoded for binary data   # todo: see what type of schema we should use here
    content_type: Safe_Str__Http__Content_Type           # MIME type                        # todo: see if we need this here
    metadata    : Dict[Safe_Id, Safe_Str__Text] = None   # Domain-specific metadata
    tags        : List[Safe_Id]                 = None   # Tags for categorization
    hash        : Safe_Str__SHA1__Short         = None   # Optional: provide custom hash
