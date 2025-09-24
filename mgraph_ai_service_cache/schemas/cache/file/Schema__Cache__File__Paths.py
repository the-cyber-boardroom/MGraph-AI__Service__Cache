from typing                                                                       import List
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path

class Schema__Cache__File__Paths(Type_Safe):
    content_files: List[Safe_Str__File__Path]          # Paths to content files
    data_folders  : List[Safe_Str__File__Path]          # Paths to data files