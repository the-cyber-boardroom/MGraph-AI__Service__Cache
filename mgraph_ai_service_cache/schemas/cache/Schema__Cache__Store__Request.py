from typing                                                                              import Dict, List
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.http.safe_str.Safe_Str__Http__Content_Type import Safe_Str__Http__Content_Type
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text             import Safe_Str__Text
from memory_fs.schemas.Safe_Str__Cache_Hash                                              import Safe_Str__Cache_Hash


class Schema__Cache__Store__Request(Type_Safe):          # Request schema for storing cache data"""
    data        : str                                    # Base64 encoded for binary data   # todo: see what type of schema we should use here
    content_type: Safe_Str__Http__Content_Type           # MIME type                        # todo: see if we need this here
    metadata    : Dict[Safe_Str__Id, Safe_Str__Text]          # Domain-specific metadata
    tags        : List[Safe_Str__Id]                          # Tags for categorization
    hash        : Safe_Str__Cache_Hash                         # provide custom hash              #todo: see if it is a good idea to ask the called to provide the hash
