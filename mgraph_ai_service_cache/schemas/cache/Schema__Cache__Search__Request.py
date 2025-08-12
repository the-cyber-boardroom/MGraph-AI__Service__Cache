from typing                                                         import Dict, List, Any
from osbot_utils.helpers.Safe_Id                                    import Safe_Id
from osbot_utils.type_safe.Type_Safe                                import Type_Safe
from osbot_utils.helpers.safe_str.http.Safe_Str__Http__Content_Type import Safe_Str__Http__Content_Type

# todo: see if need this
class Schema__Cache__Search__Request(Type_Safe):                # Request schema for searching cache entries
    filters         : Dict[str, Any]
    tags            : List[Safe_Id ]
    content_type    : Safe_Str__Http__Content_Type  = None
    date_from       : str                           = None
    date_to         : str                           = None
    limit           : int                           = 100
    offset          : int                           = 0
    include_data    : bool                          = False