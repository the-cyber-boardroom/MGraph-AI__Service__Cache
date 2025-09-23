from typing                                                                              import List
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path


class Schema__Cache__Store__Paths(Type_Safe):       # Tracks all paths created during storage
    data    : List[Safe_Str__File__Path]            # Paths to actual data files
    by_hash : List[Safe_Str__File__Path]            # Paths to hash reference files
    by_id   : List[Safe_Str__File__Path]            # Paths to ID reference files