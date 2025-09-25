from typing                                                                       import List
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path

class Schema__Cache__Data__Delete__All_Files__Response(Type_Safe):
    deleted_count : Safe_UInt
    deleted_files : List[Safe_Str__File__Path]