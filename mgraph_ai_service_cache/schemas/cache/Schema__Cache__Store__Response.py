from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import Safe_Str__Hash
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path      import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid


class Schema__Cache__Store__Response(Type_Safe): # Response schema for store operations"""
    cache_id: Random_Guid
    hash    : Safe_Str__Hash            = None
    path    : Safe_Str__File__Path                # Storage path
    size    : int                                 # Size in bytes