from memory_fs.Memory_FS                                                                import Memory_FS
from memory_fs.helpers.Memory_FS__Temporal                                              import Memory_FS__Temporal
from memory_fs.helpers.Memory_FS__Latest_Temporal                                       import Memory_FS__Latest_Temporal
from memory_fs.path_handlers.Path__Handler__Hash_Sharded                                import Path__Handler__Hash_Sharded
from memory_fs.path_handlers.Path__Handler__Key_Based                                   import Path__Handler__Key_Based
from memory_fs.storage_fs.Storage_FS                                                    import Storage_FS
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path       import Safe_Str__File__Path
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy           import Enum__Cache__Store__Strategy
from osbot_utils.utils.Http                                                             import url_join_safe

# Constants for all prefix paths
CACHE__HANDLER__PREFIX_PATH__FS__REFS_ID                            = 'refs/by-id'
CACHE__HANDLER__PREFIX_PATH__FS__REFS_HASH                          = 'refs/by-hash'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT                        = 'data/direct'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_KEY_BASED                     = 'data/key-based'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL                      = 'data/temporal'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_LATEST               = 'data/temporal-latest'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED            = 'data/temporal-versioned'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED_VERSIONS   = 'data/temporal-versioned/versions'

# Default shard depth for hash sharding
CACHE__HANDLER__SHARD_DEPTH_DEFAULT = 2

class Cache__Handler(Type_Safe):                                                                        # Single handler supporting all storage strategies
    storage_backend         : Storage_FS                  = None                                        # Storage backend instance (from Cache__Config)
    namespace               : str                         = ""                                          # Namespace prefix for isolation
    cache_ttl_hours         : int                         = 24

    # All Memory_FS instances for different strategies
    fs__data_direct             : Memory_FS                   = None                                    # Direct to hash location
    fs__data_temporal           : Memory_FS__Temporal         = None                                    # Temporal organization
    fs__data_temporal_latest    : Memory_FS__Latest_Temporal  = None                                    # Temporal + latest
    fs__data_temporal_versioned : Memory_FS                   = None                                    # Temporal + latest + versions
    fs__data_key_based      : Memory_FS                   = None                                    # For Semantic Files

    # Reference stores (always needed)
    fs__refs_hash           : Memory_FS                   = None                                        # Hash->ID mappings
    fs__refs_id             : Memory_FS                   = None                                        # ID->Hash mappings

    def build_namespaced_path(self, base_path: str) -> Safe_Str__File__Path:                           # Helper to build namespace-prefixed paths
        if self.namespace:                                                                              # Build a path with namespace prefix if namespace is set
            return url_join_safe(self.namespace , Safe_Str__File__Path(base_path))
        return Safe_Str__File__Path(base_path)

    def setup(self) -> 'Cache__Handler':                                                                # Setup all storage strategies
        if not self.storage_backend:
            raise ValueError("Storage backend must be provided before setup")

        storage = self.storage_backend                                                                  # Use the storage backend directly - namespace is handled via path prefixes

        # todo: refactor the creation of these stores into separate methods (one method per handler)
        # Reference stores (always needed) - with namespace prefix
        with Memory_FS() as _:                                                                          # Hash->ID mappings
            kwargs__fs__refs_hash = dict(prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__REFS_HASH),
                                         shard_depth = CACHE__HANDLER__SHARD_DEPTH_DEFAULT)
            _.storage_fs = storage
            _.add_handler(Path__Handler__Hash_Sharded(**kwargs__fs__refs_hash))
            self.fs__refs_hash = _

        with Memory_FS() as _:                                                                          # ID->Hash mappings
            kwargs__fs__refs_id = dict(prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__REFS_ID),
                                       shard_depth = CACHE__HANDLER__SHARD_DEPTH_DEFAULT)
            _.storage_fs = storage
            _.add_handler(Path__Handler__Hash_Sharded(**kwargs__fs__refs_id))
            self.fs__refs_id = _

        with Memory_FS() as _:                                                                          # Direct strategy
            kwargs__fs__data_direct = dict(prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT),
                                           shard_depth = CACHE__HANDLER__SHARD_DEPTH_DEFAULT)
            _.storage_fs = storage
            _.add_handler(Path__Handler__Hash_Sharded(**kwargs__fs__data_direct))
            self.fs__data_direct = _


        prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL)       # Temporal only - with namespace prefix
        with Memory_FS__Temporal(storage_fs=storage, prefix_path=prefix_path) as _:
            self.fs__data_temporal = _

        # Temporal + Latest - with namespace prefix
        prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_LATEST)
        with Memory_FS__Latest_Temporal(storage_fs=storage, prefix_path=prefix_path) as _:
            self.fs__data_temporal_latest = _

        # Temporal + Latest + Versioned - with namespace prefixes
        with Memory_FS() as _:
            _.storage_fs = storage
            _.add_handler__temporal (prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED))
            _.add_handler__latest   (prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED))
            _.add_handler__versioned(prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED_VERSIONS))
            self.fs__data_temporal_versioned = _

        # Semantic File - with namespace prefix
        with Memory_FS() as _:
            kwargs__fs__data_semantic = dict(prefix_path = self.build_namespaced_path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_KEY_BASED))
            _.storage_fs = storage
            _.add_handler(Path__Handler__Key_Based(**kwargs__fs__data_semantic))
            self.fs__data_key_based = _

        return self

    def get_fs_for_strategy(self, strategy: Enum__Cache__Store__Strategy
                             ) -> Memory_FS:                                                                         # Return appropriate Memory_FS for strategy
        if strategy == "direct":
            return self.fs__data_direct
        elif strategy == "temporal":
            return self.fs__data_temporal
        elif strategy == "temporal_latest":
            return self.fs__data_temporal_latest
        elif strategy == "temporal_versioned":
            return self.fs__data_temporal_versioned
        elif strategy == "key_based":
            return self.fs__data_key_based
        else:
            raise ValueError(f"Unknown strategy: {strategy}")