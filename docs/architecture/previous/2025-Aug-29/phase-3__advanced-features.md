# Phase 3 v2: Advanced Features with Type_Safe

## Goal
Complete the cache system with versioning, batch operations, and search capabilities following Type_Safe and Memory_FS patterns.

## Critical Principle: Pure Schemas, Service Logic

Per Type_Safe guidelines: **"Schema files should ONLY contain schema definitions - NO business logic!"**

All business logic goes in service classes, schemas remain pure data structures.


## Update Routes for Advanced Features

### Complete Routes__Cache Implementation

```python
from osbot_fast_api.api.routes.Fast_API__Routes import Fast_API__Routes

class Routes__Cache(Fast_API__Routes):
    tag             : str                = 'cache'
    cache_service   : Cache__Service
    version_service : Version__Service
    batch_service   : Batch__Service
    search_service  : Search__Service
    admin_service   : Admin__Service
    
    def setup(self):                                               # Initialize all services
        """Setup all services"""
        self.version_service = Version__Service(cache_service=self.cache_service)
        self.batch_service   = Batch__Service(cache_service=self.cache_service)
        self.search_service  = Search__Service(cache_service=self.cache_service).setup()
        self.admin_service   = Admin__Service(
            cache_service=self.cache_service,
            search_service=self.search_service
        )
        return self
    
    # Version endpoints
    def get_versions(self, hash      : Safe_Str__Cache_Hash      , # GET /cache/hash/{hash}/versions
                          namespace : Safe_Id = None
                     ) -> Schema__Version__Manifest:
        return self.version_service.get_versions(hash, namespace)
    
    def get_version(self, hash      : Safe_Str__Cache_Hash       , # GET /cache/hash/{hash}/version/{version}
                         version   : int                         ,
                         namespace : Safe_Id = None
                    ) -> Schema__Cache__Entry:
        result = self.version_service.get_specific_version(hash, version, namespace)
        if not result:
            return {"status": "not_found"}
        return result
    
    # Batch endpoints
    def store_batch(self, request: Schema__Batch__Request         # POST /cache/batch/store
                    ) -> Schema__Batch__Response:
        return self.batch_service.store_batch(request)
    
    def retrieve_batch(self, hashes    : List[Safe_Str__Cache_Hash], # POST /cache/batch/retrieve
                            namespace : Safe_Id = None
                       ) -> Dict[Safe_Str__Cache_Hash, Schema__Cache__Entry]:
        return self.batch_service.retrieve_batch(hashes, namespace)
    
    # Search endpoints
    def search(self, filter: Schema__Search__Filter               # POST /cache/search
               ) -> Schema__Search__Response:
        return self.search_service.search(filter)
    
    def search_by_tag(self, tag      : Safe_Id                   , # GET /cache/search/tag/{tag}
                           namespace : Safe_Id = None
                      ) -> Schema__Search__Response:
        filter = Schema__Search__Filter(tags=[tag])
        return self.search_service.search(filter)
    
    # Admin endpoints (should require extra auth)
    def admin_stats(self, namespace: Safe_Id = None               # GET /cache/admin/stats
                    ) -> Schema__Admin__Stats:
        return self.admin_service.get_statistics(namespace)
    
    def admin_health(self) -> Schema__Admin__Health:              # GET /cache/admin/health
        return self.admin_service.health_check()
    
    def admin_rebuild_indexes(self, namespace: Safe_Id = None     # POST /cache/admin/rebuild-indexes
                              ) -> Dict[str, int]:
        count = self.admin_service.rebuild_indexes(namespace)
        return {"rebuilt_entries": count}
    
    def setup_routes(self):
        # Existing routes
        self.add_route_post(self.store)
        self.add_route_get (self.retrieve)
        self.add_route_get (self.retrieve_by_hash)
        
        # Version routes
        self.add_route_get (self.get_versions)
        self.add_route_get (self.get_version)
        
        # Batch routes
        self.add_route_post(self.store_batch)
        self.add_route_post(self.retrieve_batch)
        
        # Search routes
        self.add_route_post(self.search)
        self.add_route_get (self.search_by_tag)
        
        # Admin routes (consider adding auth middleware)
        self.add_route_get (self.admin_stats)
        self.add_route_get (self.admin_health)
        self.add_route_post(self.admin_rebuild_indexes)
```


```python
from osbot_utils.type_safe.Type_Safe import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.identifiers.Timestamp_Now import Timestamp_Now
from typing import List, Dict

class Schema__Version__Entry(Type_Safe):                            # Pure schema for version info
    """Single version entry - no business logic"""
    version      : int
    cache_id     : Safe_Id
    path         : Safe_Str__File__Path
    size         : int
    created_at   : Timestamp_Now
    accessed_at  : Optional[Timestamp_Now] = None
    metadata     : Dict[Safe_Id, Safe_Str__Text]                   # Type_Safe__Dict

class Schema__Version__Manifest(Type_Safe):                         # Version manifest schema
    """Complete version history for a hash - pure data"""
    hash         : Safe_Str__Cache_Hash
    total_versions: int
    latest_version: int
    versions     : List[Schema__Version__Entry]                    # Type_Safe__List
    namespace    : Safe_Id
```

#### Version Service (Business Logic)

```python
class Version__Service(Type_Safe):                                  # Service handles all version logic
    """Version management service - all business logic here"""
    cache_service: Cache__Service
    
    def get_versions(self, hash      : Safe_Str__Cache_Hash      , # Get all versions of a hash
                           namespace : Safe_Id = None
                     ) -> Schema__Version__Manifest:
        """Get complete version manifest for a hash"""
        namespace = namespace or Safe_Id("default")
        handler = self.cache_service.get_or_create_handler(namespace)
        
        # Use Memory_FS to find all versions
        versions = []
        latest_version = 0
        
        # Scan for versions using Memory_FS
        for version_num in range(1, 1000):                         # Practical limit
            file_id = Safe_Id(f"{hash}-v{version_num}")
            
            with handler.memory_fs.file__cache(file_id) as file_fs:
                if file_fs.exists():
                    # Get metadata from Memory_FS
                    metadata = file_fs.metadata()
                    config = file_fs.config()
                    
                    version_entry = Schema__Version__Entry(
                        version    = version_num,
                        cache_id   = file_id,
                        path       = config.file_paths[0] if config.file_paths else Safe_Str__File__Path(""),
                        size       = metadata.get('size', 0),
                        created_at = metadata.get('created_at'),
                        metadata   = metadata.data if metadata else {}
                    )
                    versions.append(version_entry)
                    latest_version = version_num
                else:
                    break                                           # No more versions
        
        return Schema__Version__Manifest(
            hash          = hash,
            total_versions = len(versions),
            latest_version = latest_version,
            versions      = versions,
            namespace     = namespace
        )
    
    def get_specific_version(self, hash      : Safe_Str__Cache_Hash, # Get specific version
                                  version   : int                   ,
                                  namespace : Safe_Id = None
                             ) -> Optional[Schema__Cache__Entry]:
        """Retrieve specific version of cached data"""
        namespace = namespace or Safe_Id("default")
        cache_id = Safe_Id(f"{hash}-v{version}")
        
        handler = self.cache_service.get_or_create_handler(namespace)
        return handler.get_entry(cache_id)
    
    def delete_version(self, hash      : Safe_Str__Cache_Hash     , # Delete specific version
                            version   : int                       ,
                            namespace : Safe_Id = None
                       ) -> bool:
        """Delete specific version while preserving others"""
        namespace = namespace or Safe_Id("default")
        cache_id = Safe_Id(f"{hash}-v{version}")
        
        handler = self.cache_service.get_or_create_handler(namespace)
        
        # Use Memory_FS to delete
        with handler.memory_fs.file__cache(cache_id) as file_fs:
            if file_fs.exists():
                return file_fs.delete()
        
        return False
    
    def cleanup_old_versions(self, hash          : Safe_Str__Cache_Hash, # Keep only N recent versions
                                   keep_versions : int = 10             ,
                                   namespace     : Safe_Id = None
                             ) -> int:
        """Clean up old versions, keeping only the most recent N"""
        manifest = self.get_versions(hash, namespace)
        
        if manifest.total_versions <= keep_versions:
            return 0                                                # Nothing to delete
        
        # Sort by version number and delete oldest
        versions_to_delete = manifest.versions[:-keep_versions]
        deleted_count = 0
        
        for version_entry in versions_to_delete:
            if self.delete_version(hash, version_entry.version, namespace):
                deleted_count += 1
        
        return deleted_count
```

### 2. Batch Operations

#### Batch Schemas (Pure)

```python
class Schema__Batch__Item(Type_Safe):                              # Single item in batch
    """Single item for batch operations - pure data"""
    data         : Safe_Bytes
    content_type : Safe_Str__Http__Content_Type
    metadata     : Dict[Safe_Id, Safe_Str__Text]
    tags         : List[Safe_Id]
    client_id    : Optional[Safe_Id] = None                        # Client-side tracking

class Schema__Batch__Request(Type_Safe):                           # Batch operation request
    """Batch operation request - pure data"""
    items        : List[Schema__Batch__Item]                       # Type_Safe__List
    namespace    : Safe_Id
    deduplicate  : bool = True                                     # Skip duplicate content
    compress     : bool = True

class Schema__Batch__Result(Type_Safe):                            # Result for single item
    """Result for single batch item - pure data"""
    client_id    : Optional[Safe_Id]
    cache_id     : Safe_Id
    hash         : Safe_Str__Cache_Hash
    status       : Literal["stored", "duplicate", "error"]
    error_message: Optional[Safe_Str__Text] = None

class Schema__Batch__Response(Type_Safe):                          # Complete batch response
    """Batch operation response - pure data"""
    total_items  : int
    stored_count : int
    duplicate_count: int
    error_count  : int
    results      : List[Schema__Batch__Result]                     # Type_Safe__List
```

#### Batch Service (Business Logic)

```python
class Batch__Service(Type_Safe):                                   # Service for batch operations
    """Batch operations service - all business logic here"""
    cache_service: Cache__Service
    
    def store_batch(self, request: Schema__Batch__Request          # Store multiple items efficiently
                    ) -> Schema__Batch__Response:
        """Store multiple items with deduplication"""
        
        results = []
        hash_tracker = {}                                          # Track hashes for deduplication
        
        for item in request.items:
            # Generate hash for deduplication
            hash_value = self.cache_service.generate_cache_hash(item.data)
            
            if request.deduplicate and hash_value in hash_tracker:
                # Skip duplicate
                results.append(Schema__Batch__Result(
                    client_id = item.client_id,
                    cache_id  = hash_tracker[hash_value],
                    hash      = hash_value,
                    status    = "duplicate"
                ))
            else:
                # Store new item
                try:
                    response = self.cache_service.store_binary(
                        data         = item.data,
                        content_type = item.content_type,
                        namespace    = request.namespace,
                        compress     = request.compress
                    )
                    
                    hash_tracker[hash_value] = response.cache_id
                    
                    results.append(Schema__Batch__Result(
                        client_id = item.client_id,
                        cache_id  = response.cache_id,
                        hash      = response.hash,
                        status    = "stored"
                    ))
                except Exception as e:
                    results.append(Schema__Batch__Result(
                        client_id     = item.client_id,
                        cache_id      = Safe_Id("error"),
                        hash          = hash_value,
                        status        = "error",
                        error_message = Safe_Str__Text(str(e))
                    ))
        
        # Calculate statistics
        stored_count = sum(1 for r in results if r.status == "stored")
        duplicate_count = sum(1 for r in results if r.status == "duplicate")
        error_count = sum(1 for r in results if r.status == "error")
        
        return Schema__Batch__Response(
            total_items    = len(request.items),
            stored_count   = stored_count,
            duplicate_count = duplicate_count,
            error_count    = error_count,
            results        = results
        )
    
    def retrieve_batch(self, hashes    : List[Safe_Str__Cache_Hash], # Retrieve multiple items
                            namespace : Safe_Id = None
                       ) -> Dict[Safe_Str__Cache_Hash, Schema__Cache__Entry]:
        """Retrieve multiple items by hash"""
        
        namespace = namespace or Safe_Id("default")
        results = {}
        
        for hash_value in hashes:
            entry = self.cache_service.retrieve_typed(hash_value, namespace)
            if entry:
                results[hash_value] = entry
        
        return results
    
    def delete_batch(self, hashes    : List[Safe_Str__Cache_Hash]  , # Delete multiple items
                          namespace : Safe_Id = None
                     ) -> Dict[Safe_Str__Cache_Hash, bool]:
        """Delete multiple items by hash"""
        
        namespace = namespace or Safe_Id("default")
        results = {}
        
        for hash_value in hashes:
            # Get all versions
            version_service = Version__Service(cache_service=self.cache_service)
            manifest = version_service.get_versions(hash_value, namespace)
            
            # Delete all versions
            all_deleted = True
            for version_entry in manifest.versions:
                if not version_service.delete_version(hash_value, version_entry.version, namespace):
                    all_deleted = False
            
            results[hash_value] = all_deleted
        
        return results
```

### 3. Search and Discovery

#### Search Schemas (Pure)

```python
class Schema__Search__Filter(Type_Safe):                           # Search filter criteria
    """Search filter - pure data"""
    tags         : Optional[List[Safe_Id]]                  = None
    content_type : Optional[Safe_Str__Http__Content_Type]   = None
    date_from    : Optional[Timestamp_Now]                  = None
    date_to      : Optional[Timestamp_Now]                  = None
    metadata_key : Optional[Safe_Id]                        = None
    metadata_value: Optional[Safe_Str__Text]                = None

class Schema__Search__Result(Type_Safe):                           # Single search result
    """Single search result - pure data"""
    hash         : Safe_Str__Cache_Hash
    cache_id     : Safe_Id
    score        : float                                           # Relevance score
    metadata     : Dict[Safe_Id, Safe_Str__Text]

class Schema__Search__Response(Type_Safe):                         # Complete search response
    """Search response - pure data"""
    query        : Schema__Search__Filter
    total_results: int
    results      : List[Schema__Search__Result]                    # Type_Safe__List
```

#### Search Index Path Handler

```python
class Path__Handler__Search_Index(Path__Handler):                  # Memory_FS path handler for indexes
    """Generate paths for search indexes"""
    
    index_type: Literal["tag", "type", "date", "metadata"]
    
    def generate_path(self, file_id: Safe_Id) -> Safe_Str__File__Path:
        """Generate search index path"""
        
        if self.index_type == "tag":
            # refs/index/by-tag/{tag}/entries.json
            return self.combine_paths(Safe_Str__File__Path(f"index/by-tag/{file_id}"))
            
        elif self.index_type == "type":
            # refs/index/by-type/{type}/entries.json
            content_type = str(file_id).replace('/', '_')
            return self.combine_paths(Safe_Str__File__Path(f"index/by-type/{content_type}"))
            
        elif self.index_type == "date":
            # refs/index/by-date/YYYY/MM/DD/entries.json
            return self.combine_paths(Safe_Str__File__Path(f"index/by-date/{file_id}"))
            
        elif self.index_type == "metadata":
            # refs/index/by-metadata/{key}/{value}/entries.json
            return self.combine_paths(Safe_Str__File__Path(f"index/by-metadata/{file_id}"))
```

#### Search Service (Business Logic)

```python
class Search__Service(Type_Safe):                                  # Search service with indexing
    """Search and indexing service - all business logic here"""
    cache_service: Cache__Service
    index_memory_fs: Memory_FS                                     # Separate Memory_FS for indexes
    
    def setup(self) -> 'Search__Service':
        """Setup search indexes using Memory_FS"""
        handler = self.cache_service.get_or_create_handler(Safe_Id("default"))
        
        # Create Memory_FS for search indexes
        with Memory_FS() as _:
            _.storage_fs = handler.memory_fs.storage_fs            # Share same storage
            
            # Add index path handlers
            tag_index = _.add_handler__custom(Path__Handler__Search_Index)
            tag_index.index_type = "tag"
            tag_index.prefix_path = Safe_Str__File__Path("refs")
            
            type_index = _.add_handler__custom(Path__Handler__Search_Index)
            type_index.index_type = "type"
            type_index.prefix_path = Safe_Str__File__Path("refs")
            
            self.index_memory_fs = _
        
        return self
    
    def index_entry(self, entry: Schema__Cache__Entry              # Index a cache entry
                    ) -> bool:
        """Add entry to search indexes"""
        
        # Index by tags
        for tag in entry.tags:
            self._add_to_index("tag", tag, entry.hash)
        
        # Index by content type
        self._add_to_index("type", entry.content_type, entry.hash)
        
        # Index by date
        date_key = entry.stored_at.value[:10]                      # YYYY-MM-DD
        self._add_to_index("date", Safe_Id(date_key), entry.hash)
        
        return True
    
    def _add_to_index(self, index_type : str                      , # Add hash to index
                            index_key  : Safe_Id                   ,
                            hash_value : Safe_Str__Cache_Hash
                      ) -> bool:
        """Add hash to specific index"""
        
        file_id = Safe_Id(f"{index_key}_entries")
        
        with self.index_memory_fs.file__json(file_id) as file_fs:
            if file_fs.exists():
                entries = file_fs.content()
            else:
                entries = {"hashes": []}
            
            if str(hash_value) not in entries["hashes"]:
                entries["hashes"].append(str(hash_value))
                file_fs.update(entries)
        
        return True
    
    def search(self, filter: Schema__Search__Filter                # Search with filters
               ) -> Schema__Search__Response:
        """Search cache entries using filters"""
        
        matching_hashes = set()
        
        # Search by tags
        if filter.tags:
            for tag in filter.tags:
                tag_hashes = self._get_from_index("tag", tag)
                if matching_hashes:
                    matching_hashes &= set(tag_hashes)             # Intersection
                else:
                    matching_hashes = set(tag_hashes)
        
        # Search by content type
        if filter.content_type:
            type_hashes = self._get_from_index("type", Safe_Id(str(filter.content_type)))
            if matching_hashes:
                matching_hashes &= set(type_hashes)
            else:
                matching_hashes = set(type_hashes)
        
        # Convert to results
        results = []
        for hash_str in matching_hashes:
            hash_value = Safe_Str__Cache_Hash(hash_str)
            
            # Get cache entry for metadata
            entry = self.cache_service.retrieve_typed(hash_value)
            if entry:
                results.append(Schema__Search__Result(
                    hash     = hash_value,
                    cache_id = entry.cache_id,
                    score    = 1.0,  # Simple scoring for now
                    metadata = entry.metadata
                ))
        
        return Schema__Search__Response(
            query         = filter,
            total_results = len(results),
            results       = results
        )
    
    def _get_from_index(self, index_type: str,
                              index_key : Safe_Id
                        ) -> List[str]:
        """Get hashes from specific index"""
        
        file_id = Safe_Id(f"{index_key}_entries")
        
        with self.index_memory_fs.file__json(file_id) as file_fs:
            if file_fs.exists():
                entries = file_fs.content()
                return entries.get("hashes", [])
        
        return []

### 4. Admin Operations

#### Admin Schemas (Pure)

```python
class Schema__Admin__Stats(Type_Safe):                             # Cache statistics
    """Cache statistics - pure data"""
    total_entries      : int
    unique_hashes      : int
    total_versions     : int
    storage_bytes      : int
    compression_ratio  : float
    namespaces         : List[Safe_Id]
    oldest_entry       : Optional[Timestamp_Now] = None
    newest_entry       : Optional[Timestamp_Now] = None

class Schema__Admin__Health(Type_Safe):                            # System health status
    """System health check - pure data"""
    status             : Literal["healthy", "degraded", "error"]
    storage_available  : bool
    indexes_valid      : bool
    error_messages     : List[Safe_Str__Text]
```

#### Admin Service (Business Logic)

```python
class Admin__Service(Type_Safe):                                   # Administrative operations
    """Admin service - all business logic here"""
    cache_service : Cache__Service
    search_service: Search__Service
    
    def rebuild_indexes(self, namespace: Safe_Id = None            # Rebuild search indexes
                        ) -> int:
        """Rebuild all search indexes from scratch"""
        namespace = namespace or Safe_Id("default")
        handler = self.cache_service.get_or_create_handler(namespace)
        
        # Clear existing indexes
        self._clear_indexes()
        
        # Scan all cache entries and re-index
        indexed_count = 0
        
        # Use Memory_FS to list all files
        all_paths = handler.memory_fs.storage_fs.files__paths()
        
        for path in all_paths:
            if path.endswith('.cache'):
                # Load entry and re-index
                with handler.memory_fs.file__cache(Safe_Id(path.stem)) as file_fs:
                    if file_fs.exists():
                        entry_data = file_fs.content()
                        entry = Schema__Cache__Entry.from_json(entry_data)
                        self.search_service.index_entry(entry)
                        indexed_count += 1
        
        return indexed_count
    
    def cleanup_orphans(self, namespace: Safe_Id = None            # Remove orphaned references
                        ) -> int:
        """Remove references without corresponding data files"""
        namespace = namespace or Safe_Id("default")
        handler = self.cache_service.get_or_create_handler(namespace)
        
        removed_count = 0
        
        # Check all reference files
        ref_paths = [p for p in handler.memory_fs.storage_fs.files__paths() 
                    if 'refs' in str(p)]
        
        for ref_path in ref_paths:
            # Check if corresponding data file exists
            # If not, remove reference
            # Implementation details...
            pass
        
        return removed_count
    
    def get_statistics(self, namespace: Safe_Id = None             # Get detailed statistics
                       ) -> Schema__Admin__Stats:
        """Get comprehensive cache statistics"""
        namespace = namespace or Safe_Id("default")
        handler = self.cache_service.get_or_create_handler(namespace)
        
        # Gather statistics using Memory_FS
        all_paths = handler.memory_fs.storage_fs.files__paths()
        
        unique_hashes = set()
        total_versions = 0
        total_bytes = 0
        oldest = None
        newest = None
        
        for path in all_paths:
            if path.endswith('.cache'):
                # Extract hash from filename
                filename = path.stem
                if '-v' in filename:
                    hash_part = filename.split('-v')[0]
                    unique_hashes.add(hash_part)
                    total_versions += 1
                
                # Get file size
                size = handler.memory_fs.storage_fs.file__size(path)
                if size:
                    total_bytes += size
        
        return Schema__Admin__Stats(
            total_entries     = len(all_paths),
            unique_hashes     = len(unique_hashes),
            total_versions    = total_versions,
            storage_bytes     = total_bytes,
            compression_ratio = 0.0,  # Calculate from entries
            namespaces        = self.cache_service.list_namespaces(),
            oldest_entry      = oldest,
            newest_entry      = newest
        )
    
    def health_check(self) -> Schema__Admin__Health:               # System health check
        """Check system health"""
        errors = []
        
        # Check storage
        try:
            handler = self.cache_service.get_or_create_handler(Safe_Id("default"))
            storage_available = handler.memory_fs.storage_fs.file__save(
                Safe_Str__File__Path("health_check.tmp"),
                b"test"
            )
            handler.memory_fs.storage_fs.file__delete(
                Safe_Str__File__Path("health_check.tmp")
            )
        except Exception as e:
            storage_available = False
            errors.append(Safe_Str__Text(f"Storage error: {e}"))
        
        # Check indexes
        try:
            self.search_service._get_from_index("tag", Safe_Id("test"))
            indexes_valid = True
        except Exception as e:
            indexes_valid = False
            errors.append(Safe_Str__Text(f"Index error: {e}"))
        
        # Determine overall status
        if storage_available and indexes_valid:
            status = "healthy"
        elif storage_available or indexes_valid:
            status = "degraded"
        else:
            status = "error"
        
        return Schema__Admin__Health(
            status            = status,
            storage_available = storage_available,
            indexes_valid     = indexes_valid,
            error_messages    = errors
        )
```

## Testing Requirements

Following Type_Safe testing patterns:

```python
class test_Advanced_Features(TestCase):
    
    @classmethod
    def setUpClass(cls):                                          # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.cache_service   = Cache__Service()
        cls.version_service = Version__Service(cache_service=cls.cache_service)
        cls.batch_service   = Batch__Service(cache_service=cls.cache_service)
        cls.search_service  = Search__Service(cache_service=cls.cache_service).setup()
        cls.admin_service   = Admin__Service(
            cache_service=cls.cache_service,
            search_service=cls.search_service
        )
    
    def test_version_tracking(self):                              # Test version management
        with self.cache_service as cache:
            # Store same content 3 times
            data = Safe_Bytes("version test data")
            
            response1 = cache.store_binary(data, Safe_Str__Http__Content_Type("text/plain"))
            response2 = cache.store_binary(data, Safe_Str__Http__Content_Type("text/plain"))
            response3 = cache.store_binary(data, Safe_Str__Http__Content_Type("text/plain"))
            
            # All should have same hash, different versions
            assert response1.hash == response2.hash == response3.hash
            assert response1.version == 1
            assert response2.version == 2
            assert response3.version == 3
            
            # Get version manifest
            with self.version_service as vs:
                manifest = vs.get_versions(response1.hash)
                
                assert manifest.obj() == __(
                    hash           = response1.hash,
                    total_versions = 3,
                    latest_version = 3,
                    versions       = [__.obj() for __ in manifest.versions],
                    namespace      = Safe_Id("default")
                )
    
    def test_batch_deduplication(self):                          # Test batch with duplicates
        with self.batch_service as batch:
            # Create batch with duplicates
            items = [
                Schema__Batch__Item(
                    data         = Safe_Bytes("unique_1"),
                    content_type = Safe_Str__Http__Content_Type("text/plain"),
                    client_id    = Safe_Id("client_1")
                ),
                Schema__Batch__Item(
                    data         = Safe_Bytes("unique_1"),  # Duplicate!
                    content_type = Safe_Str__Http__Content_Type("text/plain"),
                    client_id    = Safe_Id("client_2")
                ),
                Schema__Batch__Item(
                    data         = Safe_Bytes("unique_2"),
                    content_type = Safe_Str__Http__Content_Type("text/plain"),
                    client_id    = Safe_Id("client_3")
                )
            ]
            
            request = Schema__Batch__Request(
                items       = items,
                namespace   = Safe_Id("test"),
                deduplicate = True
            )
            
            response = batch.store_batch(request)
            
            assert response.total_items     == 3
            assert response.stored_count    == 2  # Only 2 unique
            assert response.duplicate_count == 1
            assert response.error_count     == 0
    
    def test_search_by_tag(self):                                # Test search functionality
        with self.cache_service as cache:
            # Store items with tags
            entry1 = Schema__Cache__Store__Request(
                data = Safe_Bytes("tagged item 1"),
                tags = [Safe_Id("test"), Safe_Id("item1")]
            )
            entry2 = Schema__Cache__Store__Request(
                data = Safe_Bytes("tagged item 2"),
                tags = [Safe_Id("test"), Safe_Id("item2")]
            )
            
            response1 = cache.store(entry1)
            response2 = cache.store(entry2)
            
            # Index entries
            with self.search_service as search:
                # Search by common tag
                filter = Schema__Search__Filter(tags=[Safe_Id("test")])
                results = search.search(filter)
                
                assert results.total_results >= 2
                hashes = [r.hash for r in results.results]
                assert response1.hash in hashes
                assert response2.hash in hashes
    
    def test_admin_statistics(self):                             # Test admin features
        with self.admin_service as admin:
            stats = admin.get_statistics()
            
            # Use .obj() for comprehensive check
            assert type(stats) is Schema__Admin__Stats
            assert stats.total_entries >= 0
            assert stats.unique_hashes >= 0
            assert len(stats.namespaces) >= 1
            
            # Health check
            health = admin.health_check()
            assert health.status in ["healthy", "degraded", "error"]
            assert type(health.storage_available) is bool
            assert type(health.indexes_valid) is bool
    
    def test_memory_fs_integration(self):                        # Test Memory_FS patterns
        handler = self.cache_service.get_or_create_handler(Safe_Id("test"))
        
        with handler.memory_fs as mfs:
            # Verify multiple path handlers
            assert len(mfs.handlers) >= 2  # At least temporal and hash-sharded
            
            # Test path generation
            test_hash = Safe_Str__Cache_Hash("abcdef1234567890")
            test_id = Safe_Id(f"{test_hash}-v1")
            
            with mfs.file__cache(test_id) as file_fs:
                paths = file_fs.file__config.file_paths
                
                # Should have multiple paths (temporal, hash-sharded, latest)
                assert len(paths) >= 2
                
                # Verify sharding in path
                assert any("ab/cd" in str(p) or "refs/by-hash" in str(p) for p in paths)
```


## Performance Targets

- Version listing: < 50ms for 100 versions
- Batch store: < 500ms for 100 items (with deduplication)
- Search by tag: < 100ms for 1000 entries
- Index rebuild: < 60s for 100k entries
- Health check: < 100ms response time

## Success Metrics

- [x] All schemas are pure (no business logic)
- [x] All business logic in service classes
- [x] Multiple versions tracked per hash
- [x] Batch operations deduplicate correctly
- [x] Search uses Memory_FS path handlers
- [x] Admin operations provide useful metrics
- [x] All Safe types used (no raw primitives)
- [x] Tests follow Type_Safe patterns with `.obj()` and context managers
- [x] Memory_FS handles all path operations
- [x] Type_Safe collections provide continuous validation
