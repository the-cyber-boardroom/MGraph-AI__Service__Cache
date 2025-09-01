from typing                                                                       import Literal
from memory_fs.Memory_FS                                                          import Memory_FS
from memory_fs.helpers.Memory_FS__Temporal                                        import Memory_FS__Temporal
from memory_fs.helpers.Memory_FS__Latest_Temporal                                 import Memory_FS__Latest_Temporal
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path    import Safe_Str__File__Path
from mgraph_ai_service_cache.service.storage.Storage_FS__S3                       import Storage_FS__S3
from mgraph_ai_service_cache.service.storage.Path__Handler__Hash_Sharded          import Path__Handler__Hash_Sharded


# Constants for all prefix paths
CACHE__HANDLER__PREFIX_PATH__FS__REFS_ID                            = 'refs/by-id'
CACHE__HANDLER__PREFIX_PATH__FS__REFS_HASH                          = 'refs/by-hash'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT                        = 'data/direct'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL                      = 'data/temporal'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_LATEST               = 'data/temporal-latest'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED            = 'data/temporal-versioned'
CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED_VERSIONS   = 'data/temporal-versioned/versions'

# Default shard depth for hash sharding
CACHE__HANDLER__SHARD_DEPTH_DEFAULT = 2

class Cache__Handler(Type_Safe):                                                   # Single handler supporting all storage strategies
    s3__bucket              : str
    s3__prefix              : str                         = ""
    cache_ttl_hours         : int                         = 24

    # Storage backend
    s3__storage             : Storage_FS__S3              = None

    # All Memory_FS instances for different strategies
    fs__data_direct             : Memory_FS                   = None                   # Direct to hash location
    fs__data_temporal           : Memory_FS__Temporal         = None                   # Temporal organization
    fs__data_temporal_latest    : Memory_FS__Latest_Temporal  = None                   # Temporal + latest
    fs__data_temporal_versioned : Memory_FS                = None                   # Temporal + latest + versions

    # Reference stores (always needed)
    fs__refs_hash           : Memory_FS                   = None                    # Hash->ID mappings
    fs__refs_id             : Memory_FS                   = None                    # ID->Hash mappings

    def setup(self) -> 'Cache__Handler':                                            # Setup all storage strategies
        self.s3__storage = Storage_FS__S3(s3_bucket=self.s3__bucket,
                                          s3_prefix=self.s3__prefix).setup()

        # Reference stores (always needed)
        with Memory_FS() as _:                                                                          # Hash->ID mappings
            kwargs__fs__refs_hash = dict(prefix_path = CACHE__HANDLER__PREFIX_PATH__FS__REFS_HASH,
                                         shard_depth = CACHE__HANDLER__SHARD_DEPTH_DEFAULT)
            _.storage_fs = self.s3__storage
            _.add_handler(Path__Handler__Hash_Sharded(**kwargs__fs__refs_hash))
            self.fs__refs_hash = _

        with Memory_FS() as _:                                                                          # ID->Hash mappings
            kwargs__fs__refs_id = dict(prefix_path = CACHE__HANDLER__PREFIX_PATH__FS__REFS_ID,
                                       shard_depth = CACHE__HANDLER__SHARD_DEPTH_DEFAULT)
            _.storage_fs = self.s3__storage
            _.add_handler(Path__Handler__Hash_Sharded(**kwargs__fs__refs_id))
            self.fs__refs_id = _

        with Memory_FS() as _:                                                                          # Direct strategy
            kwargs__fs__data_direct = dict(prefix_path = CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT,
                                           shard_depth = CACHE__HANDLER__SHARD_DEPTH_DEFAULT)
            _.storage_fs = self.s3__storage
            _.add_handler(Path__Handler__Hash_Sharded(**kwargs__fs__data_direct))
            self.fs__data_direct = _

        prefix_path = Safe_Str__File__Path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL)              # Temporal only
        with Memory_FS__Temporal(storage_fs=self.s3__storage, prefix_path=prefix_path) as _:
            self.fs__data_temporal = _

        prefix_path = Safe_Str__File__Path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_LATEST)       # Temporal + Latest
        with Memory_FS__Latest_Temporal(storage_fs=self.s3__storage, prefix_path=prefix_path) as _:
            self.fs__data_temporal_latest = _

        # Temporal + Latest + Versioned
        with Memory_FS() as _:
            _.storage_fs = self.s3__storage
            _.add_handler__temporal (prefix_path = Safe_Str__File__Path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED))
            _.add_handler__latest   (prefix_path = Safe_Str__File__Path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED))
            _.add_handler__versioned(prefix_path = Safe_Str__File__Path(CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED_VERSIONS))
            self.fs__data_temporal_versioned = _

        return self

    def get_fs_for_strategy(self, strategy: Literal["direct", "temporal", "temporal_latest", "temporal_versioned"]
                            ) -> Memory_FS:                                                                         # Return appropriate Memory_FS for strategy
        if strategy == "direct":
            return self.fs__data_direct
        elif strategy == "temporal":
            return self.fs__data_temporal
        elif strategy == "temporal_latest":
            return self.fs__data_temporal_latest
        elif strategy == "temporal_versioned":
            return self.fs__data_temporal_versioned
        else:
            raise ValueError(f"Unknown strategy: {strategy}")