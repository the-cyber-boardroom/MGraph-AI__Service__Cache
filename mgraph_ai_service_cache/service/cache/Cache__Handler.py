from typing                                                         import Any, Type, Optional
from memory_fs.file_fs.File_FS                                      import File_FS
from memory_fs.helpers.Memory_FS__Latest                            import Memory_FS__Latest
from memory_fs.helpers.Memory_FS__Latest_Temporal                   import Memory_FS__Latest_Temporal
from memory_fs.helpers.Memory_FS__Temporal                          import Memory_FS__Temporal
from memory_fs.schemas.Schema__Memory_FS__File__Type                import Schema__Memory_FS__File__Type
from osbot_utils.type_safe.Type_Safe                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id  import Safe_Id
from mgraph_ai_service_cache.service.cache.Storage_FS__S3           import Storage_FS__S3


class Cache__Handler(Type_Safe):                                                       # Generic cache handler using Memory-FS patterns
    s3__bucket          : str                                                          # S3 bucket for cache storage
    s3__prefix          : str                         = ""                             # Prefix for this cache instance
    s3__storage         : Storage_FS__S3              = None                           # S3 storage backend
    fs__latest          : Memory_FS__Latest           = None                           # Latest-only pattern
    fs__temporal        : Memory_FS__Temporal         = None                           # Temporal-only pattern
    fs__latest_temporal : Memory_FS__Latest_Temporal  = None                           # Combined latest+temporal pattern
    cache_ttl_hours     : int                         = 24                             # Default TTL in hours

    def setup(self) -> 'Cache__Handler':                                               # Initialize cache system with Memory-FS patterns
        self.s3__storage         = Storage_FS__S3( s3_bucket = self.s3__bucket ,
                                                   s3_prefix = self.s3__prefix).setup()
        self.fs__temporal        = Memory_FS__Temporal       ( storage_fs = self.s3__storage)
        self.fs__latest_temporal = Memory_FS__Latest_Temporal( storage_fs = self.s3__storage)
        self.fs__latest          = Memory_FS__Latest         ( storage_fs = self.s3__storage)
        return self

    def file_for_latest(self, file_id   : Safe_Id                                    , # Create file for latest cache pattern
                              file_type : Type[Schema__Memory_FS__File__Type] = None
                        ) -> File_FS:
        return self.fs__latest.file(file_id=file_id, file_type=file_type)

    def file_for_temporal(self, file_id  : Safe_Id                                   , # Create file for temporal cache pattern
                                file_type : Type[Schema__Memory_FS__File__Type] = None
                          ) -> File_FS:
        return self.fs__temporal.file(file_id=file_id, file_type=file_type)

    def file_for_latest_temporal(self, file_id   : Safe_Id                           , # Create file for combined pattern
                                       file_type : Type[Schema__Memory_FS__File__Type] = None
                                  ) -> File_FS:
        return self.fs__latest_temporal.file(file_id=file_id, file_type=file_type)