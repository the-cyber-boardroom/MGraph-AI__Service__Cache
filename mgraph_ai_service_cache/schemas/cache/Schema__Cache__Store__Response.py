from osbot_utils.helpers.Safe_Id                        import Safe_Id
from osbot_utils.helpers.safe_str.Safe_Str__File__Path  import Safe_Str__File__Path
from osbot_utils.helpers.safe_str.Safe_Str__Hash        import Safe_Str__Hash
from osbot_utils.type_safe.Type_Safe                    import Type_Safe


class Schema__Cache__Store__Response(Type_Safe): # Response schema for store operations"""
    cache_id: Safe_Id
    hash    : Safe_Str__Hash
    version : int                                 # Version number if hash existed # todo: see if we should be storing the version here
    path    : Safe_Str__File__Path                # Storage path
    size    : int                                 # Size in bytes