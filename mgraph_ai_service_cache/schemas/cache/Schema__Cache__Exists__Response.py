from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from memory_fs.schemas.Safe_Str__Cache_Hash                                       import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Metadata                import Schema__Cache__Metadata

# todo: see refactoring opportunity for creating some base classes that have common fields like cache_id, cache_hash and metadata
# Response for checking if cache entry exists
class Schema__Cache__Exists__Response(Type_Safe):                                     # Response for exists check
    exists       : bool                                                               # Whether entry exists
    cache_hash   : Safe_Str__Cache_Hash                                               # Hash that was checked
    namespace    : Safe_Str__Id                                                       # Namespace checked
    cache_id     : Random_Guid             = None                                     # ID if it exists
    metadata     : Schema__Cache__Metadata = None                                     # Metadata if it exists
