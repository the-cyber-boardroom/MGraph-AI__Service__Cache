from datetime                                                               import datetime
from typing                                                                 import List, Optional, Dict, Any
from osbot_utils.helpers.Safe_Id                                            import Safe_Id
from osbot_utils.helpers.safe_str.Safe_Str__File__Path                      import Safe_Str__File__Path
from osbot_utils.type_safe.Type_Safe                                        import Type_Safe
from osbot_utils.type_safe.decorators.type_safe                             import type_safe
from osbot_utils.decorators.methods.cache_on_self                           import cache_on_self
from memory_fs.file_fs.File_FS                                              import File_FS
from memory_fs.storage_fs.Storage_FS                                        import Storage_FS
from memory_fs.schemas.Schema__Memory_FS__File__Config                      import Schema__Memory_FS__File__Config
from memory_fs.file_types.Memory_FS__File__Type__Json                       import Memory_FS__File__Type__Json
from memory_fs.path_handlers.Path__Handler__Temporal                        import Path__Handler__Temporal
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__SHA1__Short           import Safe_Str__SHA1__Short


class Cache__Storage__Manager(Type_Safe):                                                 # Manages the physical storage of cache data using Memory-FS properly

    storage_fs  : Storage_FS
    shard_depth : int = 2                                                                 # Directory sharding depth
    shard_chars : int = 2                                                                 # Characters per shard level

    # ===== Path Generation Methods =====

    def build_temporal_path(self, cache_id  : Safe_Id                    ,                # Build temporal storage path for cache data using Memory-FS patterns
                                  timestamp : Optional[datetime] = None
                            ) -> Safe_Str__File__Path:
        if timestamp is None:
            timestamp = datetime.utcnow()

        temporal_handler = Path__Handler__Temporal(areas=[Safe_Id("cache-id-" + str(cache_id))])
        return temporal_handler.generate_path()

    def build_hash_ref_path(self, hash_value : Safe_Str__SHA1__Short ,                    # Build sharded path for hash references
                                  ref_type   : str                    = "refs"
                           ) -> Safe_Str__File__Path:
        shards      = self.shard_hash(hash_value)
        shard_path  = "/".join(shards[:-1])                                               # All but last element
        filename    = f"{shards[-1]}.{ref_type}.json"
        path        = f"refs/by-hash/{shard_path}/{filename}"
        return Safe_Str__File__Path(path)

    def build_hash_latest_path(self, hash_value : Safe_Str__SHA1__Short                   # Build path for latest reference file
                               ) -> Safe_Str__File__Path:
        shards      = self.shard_hash(hash_value)
        shard_path  = "/".join(shards[:-1])
        filename    = f"{shards[-1]}.latest.json"
        path        = f"refs/by-hash/{shard_path}/{filename}"
        return Safe_Str__File__Path(path)

    def build_id_ref_path(self, cache_id : Safe_Id                                        # Build sharded path for ID references
                         ) -> Safe_Str__File__Path:
        shards      = self.shard_id(cache_id)
        shard_path  = "/".join(shards[:-1])
        filename    = f"{shards[-1]}.ref.json"
        path        = f"refs/by-id/{shard_path}/{filename}"
        return Safe_Str__File__Path(path)

    def shard_hash(self, hash_value : Safe_Str__SHA1__Short                               # Shard a short hash into directory components
                  ) -> List[str]:
        hash_str     = str(hash_value)
        shards       = []
        actual_depth = min(self.shard_depth, len(hash_str) // self.shard_chars)           # For short hashes (7 chars), adjust sharding strategy

        for i in range(actual_depth):
            start = i * self.shard_chars
            end   = min(start + self.shard_chars, len(hash_str))
            shards.append(hash_str[start:end])

        remainder_start = actual_depth * self.shard_chars                                 # Add remainder as final component
        if remainder_start < len(hash_str):
            shards.append(hash_str[remainder_start:])
        else:
            shards.append(hash_str)                                                       # Use full hash if no sharding possible

        return shards

    def shard_id(self, cache_id : Safe_Id                                                 # Shard a cache ID into directory components
                ) -> List[str]:
        id_str = str(cache_id)
        shards = []

        for i in range(min(self.shard_depth, len(id_str) // self.shard_chars)):          # Use first characters for sharding
            start = i * self.shard_chars
            end   = start + self.shard_chars
            shards.append(id_str[start:end])

        shards.append(id_str)                                                             # Add full ID as final component
        return shards

    # ===== Storage Operations Using Memory-FS =====

    @type_safe
    def store_data_with_memory_fs(self, cache_id      : Safe_Id          ,                # Store cache data using Memory-FS File_FS pattern
                                        data          : bytes             ,
                                        metadata      : Dict[str, Any]    ,
                                        content_type  : str
                                  ) -> Safe_Str__File__Path:
        data_path   = self.build_temporal_path(cache_id)                                  # Build path for this cache entry

        file_config = Schema__Memory_FS__File__Config(file_id    = cache_id              ,  # Create File_FS configuration
                                                      file_paths = [data_path]           ,  # Single path for now
                                                      file_type  = Memory_FS__File__Type__Json())  # Using JSON type

        file_fs = File_FS(file__config = file_config ,                                    # Create File_FS instance
                         storage_fs    = self.storage_fs)

        files_created = file_fs.create(file_data=data)                                    # Store the data using File_FS

        file_metadata = file_fs.metadata()                                                # Update the metadata with our cache-specific metadata
        if metadata.get("tags"):
            file_metadata.tags = set(metadata["tags"])

        file_fs.file_fs__metadata().update_metadata(file_bytes=data)                      # Update metadata (this is a bit of a workaround)

        return data_path

    @type_safe
    def store_reference(self, path     : Safe_Str__File__Path ,                           # Store a reference file using Memory-FS
                             ref_data : Dict[str, Any]
                       ) -> bool:
        ref_id = Safe_Id(path.split('/')[-1].split('.')[0])                              # Extract ID from filename

        file_config = Schema__Memory_FS__File__Config(file_id    = ref_id               ,
                                                      file_paths = [path]                ,
                                                      file_type  = Memory_FS__File__Type__Json())

        file_fs = File_FS(file__config = file_config ,
                         storage_fs    = self.storage_fs)

        files_created = file_fs.create(file_data=ref_data)                               # Store the reference data
        return len(files_created) > 0

    @type_safe
    def load_reference(self, path : Safe_Str__File__Path                                  # Load a reference file using Memory-FS
                      ) -> Optional[Dict[str, Any]]:
        if not self.storage_fs.file__exists(path):                                       # Check if file exists first
            return None

        json_data = self.storage_fs.file__json(path)                                     # For reference files, we can read directly as JSON
        return json_data

    @type_safe
    def load_data_with_memory_fs(self, cache_id : Safe_Id              ,                  # Load cache data using Memory-FS File_FS pattern
                                       path     : Safe_Str__File__Path
                                 ) -> Optional[bytes]:
        file_config = Schema__Memory_FS__File__Config(file_id    = cache_id             ,  # Recreate File_FS configuration
                                                      file_paths = [path]                ,
                                                      file_type  = Memory_FS__File__Type__Json())

        file_fs = File_FS(file__config = file_config ,                                   # Create File_FS instance
                         storage_fs    = self.storage_fs)

        if not file_fs.exists():                                                         # Check if file exists
            return None

        return file_fs.file_fs__content().bytes()                                        # Get the content as bytes

    @type_safe
    def load_metadata_with_memory_fs(self, cache_id : Safe_Id              ,              # Load just the metadata using Memory-FS
                                           path     : Safe_Str__File__Path
                                     ) -> Optional[Dict[str, Any]]:
        file_config = Schema__Memory_FS__File__Config(file_id    = cache_id             ,  # Recreate File_FS configuration
                                                      file_paths = [path]                ,
                                                      file_type  = Memory_FS__File__Type__Json())

        file_fs = File_FS(file__config = file_config ,                                   # Create File_FS instance
                         storage_fs    = self.storage_fs)

        if file_fs.file_fs__exists().metadata():                                         # Get the metadata
            metadata = file_fs.metadata()
            return metadata.json() if metadata else None

        return None

    @type_safe
    def delete_with_memory_fs(self, cache_id : Safe_Id              ,                     # Delete cache entry using Memory-FS
                                    path     : Safe_Str__File__Path
                              ) -> bool:
        file_config = Schema__Memory_FS__File__Config(file_id    = cache_id             ,  # Recreate File_FS configuration
                                                      file_paths = [path]                ,
                                                      file_type  = Memory_FS__File__Type__Json())

        file_fs = File_FS(file__config = file_config ,                                   # Create File_FS instance
                         storage_fs    = self.storage_fs)

        files_deleted = file_fs.delete()                                                 # Delete all files (data, config, metadata)
        return len(files_deleted) > 0

    # ===== Index Operations =====

    @type_safe
    def update_master_index(self, operation  : str                      ,                 # Update the master index using Memory-FS patterns
                                  cache_id   : Safe_Id                   ,
                                  hash_value : Safe_Str__SHA1__Short     ,
                                  size       : int
                           ) -> bool:
        index_path = Safe_Str__File__Path("indexes/master.index.json")

        index_data = self.load_reference(index_path) or {"total_entries"       : 0      ,  # Load existing index or create new
                                                         "total_unique_hashes" : 0      ,
                                                         "total_size_bytes"    : 0      ,
                                                         "last_updated"        : None   }

        if operation == "add":
            index_data["total_entries"]    += 1
            index_data["total_size_bytes"] += size                                       # Note: total_unique_hashes needs separate tracking
        elif operation == "delete":
            index_data["total_entries"]    = max(0, index_data["total_entries"] - 1)
            index_data["total_size_bytes"] = max(0, index_data["total_size_bytes"] - size)

        index_data["last_updated"] = datetime.utcnow().isoformat()

        return self.store_reference(index_path, index_data)

    # ===== Helper Methods =====

    @cache_on_self
    def create_cache_file_type(self):                                                    # Create a custom file type for cache entries
        return Memory_FS__File__Type__Json()                                             # This could be extended to create a custom Cache file type

    def get_cache_entry_paths(self, cache_id : Safe_Id                                   # Get all paths for a cache entry
                             ) -> Dict[str, Safe_Str__File__Path]:
        base_path = self.build_temporal_path(cache_id)
        base_str  = str(base_path)

        if base_str.endswith('.json'):                                                   # Remove .json if present to get base
            base_str = base_str[:-5]

        return {"data"     : Safe_Str__File__Path(f"{base_str}.json")          ,
                "config"   : Safe_Str__File__Path(f"{base_str}.json.config")   ,
                "metadata" : Safe_Str__File__Path(f"{base_str}.json.metadata") }