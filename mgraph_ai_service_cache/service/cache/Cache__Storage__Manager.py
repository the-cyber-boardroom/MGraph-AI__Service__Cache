from datetime import datetime
from typing import List, Optional, Dict, Any
from osbot_utils.helpers.Safe_Id import Safe_Id
from osbot_utils.helpers.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.Type_Safe import Type_Safe
from osbot_utils.type_safe.decorators.type_safe import type_safe
from osbot_utils.decorators.methods.cache_on_self import cache_on_self

# Memory-FS imports
from memory_fs.file_fs.File_FS import File_FS
from memory_fs.storage_fs.Storage_FS import Storage_FS
from memory_fs.schemas.Schema__Memory_FS__File__Config import Schema__Memory_FS__File__Config
from memory_fs.file_types.Memory_FS__File__Type__Json import Memory_FS__File__Type__Json
from memory_fs.path_handlers.Path__Handler__Temporal import Path__Handler__Temporal
from memory_fs.path_handlers.Path__Handler import Path__Handler

from mgraph_ai_service_cache.schemas.hashes.Safe_Str__SHA1__Short import Safe_Str__SHA1__Short


class Cache__Storage__Manager(Type_Safe):
    """Manages the physical storage of cache data using Memory-FS properly"""

    storage_fs: Storage_FS
    shard_depth: int = 2              # Directory sharding depth
    shard_chars: int = 2              # Characters per shard level

    # ===== Path Generation Methods =====

    def build_temporal_path(self, cache_id: Safe_Id, timestamp: Optional[datetime] = None) -> Safe_Str__File__Path:
        """Build temporal storage path for cache data using Memory-FS patterns"""
        if timestamp is None:
            timestamp = datetime.utcnow()

        # Use Path__Handler__Temporal for consistency with Memory-FS
        temporal_handler = Path__Handler__Temporal(
            areas=[Safe_Id("cache-id-" + str(cache_id))]
        )
        return temporal_handler.generate_path()

    def build_hash_ref_path(self, hash_value: Safe_Str__SHA1__Short, ref_type: str = "refs") -> Safe_Str__File__Path:
        """Build sharded path for hash references"""
        shards = self.shard_hash(hash_value)
        shard_path = "/".join(shards[:-1])  # All but last element
        filename = f"{shards[-1]}.{ref_type}.json"

        path = f"refs/by-hash/{shard_path}/{filename}"
        return Safe_Str__File__Path(path)

    def build_hash_latest_path(self, hash_value: Safe_Str__SHA1__Short) -> Safe_Str__File__Path:
        """Build path for latest reference file"""
        shards = self.shard_hash(hash_value)
        shard_path = "/".join(shards[:-1])
        filename = f"{shards[-1]}.latest.json"

        path = f"refs/by-hash/{shard_path}/{filename}"
        return Safe_Str__File__Path(path)

    def build_id_ref_path(self, cache_id: Safe_Id) -> Safe_Str__File__Path:
        """Build sharded path for ID references"""
        shards = self.shard_id(cache_id)
        shard_path = "/".join(shards[:-1])
        filename = f"{shards[-1]}.ref.json"

        path = f"refs/by-id/{shard_path}/{filename}"
        return Safe_Str__File__Path(path)

    def shard_hash(self, hash_value: Safe_Str__SHA1__Short) -> List[str]:
        """Shard a short hash into directory components"""
        hash_str = str(hash_value)
        shards = []

        # For short hashes (7 chars), adjust sharding strategy
        actual_depth = min(self.shard_depth, len(hash_str) // self.shard_chars)

        for i in range(actual_depth):
            start = i * self.shard_chars
            end = min(start + self.shard_chars, len(hash_str))
            shards.append(hash_str[start:end])

        # Add remainder as final component
        remainder_start = actual_depth * self.shard_chars
        if remainder_start < len(hash_str):
            shards.append(hash_str[remainder_start:])
        else:
            shards.append(hash_str)  # Use full hash if no sharding possible

        return shards

    def shard_id(self, cache_id: Safe_Id) -> List[str]:
        """Shard a cache ID into directory components"""
        id_str = str(cache_id)
        shards = []

        # Use first characters for sharding
        for i in range(min(self.shard_depth, len(id_str) // self.shard_chars)):
            start = i * self.shard_chars
            end = start + self.shard_chars
            shards.append(id_str[start:end])

        # Add full ID as final component
        shards.append(id_str)

        return shards

    # ===== Storage Operations Using Memory-FS =====

    @type_safe
    def store_data_with_memory_fs(self, cache_id: Safe_Id,
                                  data: bytes,
                                  metadata: Dict[str, Any],
                                  content_type: str) -> Safe_Str__File__Path:
        """Store cache data using Memory-FS File_FS pattern"""

        # Build path for this cache entry
        data_path = self.build_temporal_path(cache_id)

        # Create File_FS configuration
        file_config = Schema__Memory_FS__File__Config(
            file_id=cache_id,
            file_paths=[data_path],  # Single path for now
            file_type=Memory_FS__File__Type__Json()  # Using JSON type
        )

        # Create File_FS instance
        file_fs = File_FS(
            file__config=file_config,
            storage_fs=self.storage_fs
        )

        # Store the data using File_FS
        # This will create:
        # - cache-id-{id}.json (the data)
        # - cache-id-{id}.json.config (the config)
        # - cache-id-{id}.json.metadata (the metadata)
        files_created = file_fs.create(file_data=data)

        # Update the metadata with our cache-specific metadata
        # Note: File_FS already sets content__hash and content__size
        file_metadata = file_fs.metadata()
        if metadata.get("tags"):
            file_metadata.tags = set(metadata["tags"])

        # Update metadata (this is a bit of a workaround)
        file_fs.file_fs__metadata().update_metadata(file_bytes=data)

        return data_path

    @type_safe
    def store_reference(self, path: Safe_Str__File__Path, ref_data: Dict[str, Any]) -> bool:
        """Store a reference file using Memory-FS"""
        # For reference files, we can use simpler approach
        # since they don't need the full File_FS treatment

        # Create a simple File_FS for the reference
        ref_id = Safe_Id(path.split('/')[-1].split('.')[0])  # Extract ID from filename

        file_config = Schema__Memory_FS__File__Config(
            file_id=ref_id,
            file_paths=[path],
            file_type=Memory_FS__File__Type__Json()
        )

        file_fs = File_FS(
            file__config=file_config,
            storage_fs=self.storage_fs
        )

        # Store the reference data
        files_created = file_fs.create(file_data=ref_data)
        return len(files_created) > 0

    @type_safe
    def load_reference(self, path: Safe_Str__File__Path) -> Optional[Dict[str, Any]]:
        """Load a reference file using Memory-FS"""
        # Check if file exists first
        if not self.storage_fs.file__exists(path):
            return None

        # For reference files, we can read directly as JSON
        json_data = self.storage_fs.file__json(path)
        return json_data

    @type_safe
    def load_data_with_memory_fs(self, cache_id: Safe_Id, path: Safe_Str__File__Path) -> Optional[bytes]:
        """Load cache data using Memory-FS File_FS pattern"""

        # Recreate File_FS configuration
        file_config = Schema__Memory_FS__File__Config(
            file_id=cache_id,
            file_paths=[path],
            file_type=Memory_FS__File__Type__Json()
        )

        # Create File_FS instance
        file_fs = File_FS(
            file__config=file_config,
            storage_fs=self.storage_fs
        )

        # Check if file exists
        if not file_fs.exists():
            return None

        # Get the content as bytes
        return file_fs.file_fs__content().bytes()

    @type_safe
    def load_metadata_with_memory_fs(self, cache_id: Safe_Id, path: Safe_Str__File__Path) -> Optional[Dict[str, Any]]:
        """Load just the metadata using Memory-FS"""

        # Recreate File_FS configuration
        file_config = Schema__Memory_FS__File__Config(
            file_id=cache_id,
            file_paths=[path],
            file_type=Memory_FS__File__Type__Json()
        )

        # Create File_FS instance
        file_fs = File_FS(
            file__config=file_config,
            storage_fs=self.storage_fs
        )

        # Get the metadata
        if file_fs.file_fs__exists().metadata():
            metadata = file_fs.metadata()
            return metadata.json() if metadata else None

        return None

    @type_safe
    def delete_with_memory_fs(self, cache_id: Safe_Id, path: Safe_Str__File__Path) -> bool:
        """Delete cache entry using Memory-FS"""

        # Recreate File_FS configuration
        file_config = Schema__Memory_FS__File__Config(
            file_id=cache_id,
            file_paths=[path],
            file_type=Memory_FS__File__Type__Json()
        )

        # Create File_FS instance
        file_fs = File_FS(
            file__config=file_config,
            storage_fs=self.storage_fs
        )

        # Delete all files (data, config, metadata)
        files_deleted = file_fs.delete()
        return len(files_deleted) > 0

    # ===== Index Operations =====

    @type_safe
    def update_master_index(self, operation: str,
                          cache_id: Safe_Id,
                          hash_value: Safe_Str__SHA1__Short,
                          size: int) -> bool:
        """Update the master index using Memory-FS patterns"""
        index_path = Safe_Str__File__Path("indexes/master.index.json")

        # Load existing index or create new
        index_data = self.load_reference(index_path) or {
            "total_entries": 0,
            "total_unique_hashes": 0,
            "total_size_bytes": 0,
            "last_updated": None
        }

        if operation == "add":
            index_data["total_entries"] += 1
            index_data["total_size_bytes"] += size
            # Note: total_unique_hashes needs separate tracking
        elif operation == "delete":
            index_data["total_entries"] = max(0, index_data["total_entries"] - 1)
            index_data["total_size_bytes"] = max(0, index_data["total_size_bytes"] - size)

        index_data["last_updated"] = datetime.utcnow().isoformat()

        return self.store_reference(index_path, index_data)

    # ===== Helper Methods =====

    @cache_on_self
    def create_cache_file_type(self):
        """Create a custom file type for cache entries"""
        # This could be extended to create a custom Cache file type
        # For now, using JSON type
        return Memory_FS__File__Type__Json()

    def get_cache_entry_paths(self, cache_id: Safe_Id) -> Dict[str, Safe_Str__File__Path]:
        """Get all paths for a cache entry"""
        base_path = self.build_temporal_path(cache_id)
        base_str = str(base_path)

        # Remove .json if present to get base
        if base_str.endswith('.json'):
            base_str = base_str[:-5]

        return {
            "data": Safe_Str__File__Path(f"{base_str}.json"),
            "config": Safe_Str__File__Path(f"{base_str}.json.config"),
            "metadata": Safe_Str__File__Path(f"{base_str}.json.metadata")
        }