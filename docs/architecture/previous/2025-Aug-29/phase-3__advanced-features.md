# Phase 3: Advanced Features

## Goal
Complete the cache system with versioning, batch operations, and search capabilities.

## Features to Implement

### 1. Complete Versioning System

#### Current State (BROKEN):
- Every store creates new Random_Guid
- No relationship between versions
- No way to list versions

#### Target Implementation:

**Add to `Cache__Service`:**
```python
class Cache__Service(Type_Safe):
    
    def get_versions(self, hash: Safe_Str__Hash, 
                    namespace: Safe_Id = None) -> List[dict]:
        """Get all versions of a hash"""
        refs_path = self._get_refs_path(hash, namespace)
        refs_data = self._read_refs_json(refs_path)
        return refs_data.get('versions', [])
    
    def get_version(self, hash: Safe_Str__Hash,
                   version: int,
                   namespace: Safe_Id = None) -> dict:
        """Get specific version"""
        versions = self.get_versions(hash, namespace)
        for v in versions:
            if v['version'] == version:
                return self._retrieve_from_path(v['path'])
        return None
    
    def delete_version(self, hash: Safe_Str__Hash,
                      version: int,
                      namespace: Safe_Id = None) -> bool:
        """Delete specific version (keep others)"""
        # Update refs.json
        # Delete data file
        # Update latest.json if needed
```

**Version Metadata Structure:**
```python
{
    "hash": "a73f2e4b",
    "versions": [
        {
            "version": 1,
            "cache_id": "a73f2e4b-v1",
            "timestamp": 1734357600000,
            "path": "data/2025/01/10/11/a73f2e4b-v1.json",
            "size": 15234,
            "metadata": {...}
        },
        {
            "version": 2,
            "cache_id": "a73f2e4b-v2",
            "timestamp": 1734357700000,
            "path": "data/2025/01/10/11/a73f2e4b-v2.json",
            "size": 15240,
            "metadata": {...}
        }
    ]
}
```

### 2. Batch Operations

**New Methods in `Cache__Service`:**

```python
def store_batch(self, items: List[dict], 
               namespace: Safe_Id = None) -> List[Schema__Cache__Store__Response]:
    """Store multiple items efficiently"""
    results = []
    # Group by hash to detect duplicates
    hash_groups = {}
    for item in items:
        hash_val = self.generate_hash(item['data'])
        if hash_val not in hash_groups:
            hash_groups[hash_val] = []
        hash_groups[hash_val].append(item)
    
    # Store unique items only
    for hash_val, items in hash_groups.items():
        # Store first item, skip duplicates
        result = self._store_with_hash(hash_val, items[0])
        results.append(result)
    return results

def retrieve_batch(self, hashes: List[Safe_Str__Hash],
                  namespace: Safe_Id = None) -> Dict[str, Any]:
    """Retrieve multiple items efficiently"""
    results = {}
    # Could parallelize for S3
    for hash_val in hashes:
        results[hash_val] = self.retrieve_by_hash(hash_val, namespace)
    return results

def delete_batch(self, hashes: List[Safe_Str__Hash],
                namespace: Safe_Id = None) -> Dict[str, bool]:
    """Delete multiple items"""
    results = {}
    for hash_val in hashes:
        results[hash_val] = self.delete_by_hash(hash_val, namespace)
    return results
```

**New Route Endpoints:**
```python
# POST /cache/batch/store
# POST /cache/batch/retrieve  
# POST /cache/batch/delete
```

### 3. Search and Discovery

**Add Search Manager:**
```python
class Cache__Search_Manager(Type_Safe):
    """Manages search indexes and queries"""
    
    def index_entry(self, hash: Safe_Str__Hash, 
                   metadata: dict, 
                   tags: List[str]):
        """Add to search indexes"""
        # Update by-tag index
        # Update by-type index
        # Update temporal index
    
    def search_by_tag(self, tag: Safe_Id) -> List[Safe_Str__Hash]:
        """Find all entries with tag"""
        
    def search_by_type(self, content_type: str) -> List[Safe_Str__Hash]:
        """Find all entries of type"""
        
    def search_by_date_range(self, start: datetime, 
                            end: datetime) -> List[Safe_Str__Hash]:
        """Find entries in time range"""
        
    def search_complex(self, filters: dict) -> List[Safe_Str__Hash]:
        """Complex multi-criteria search"""
```

**Search Index Structure:**
```
refs/
├── by-tag/
│   └── {tag}/
│       └── entries.json  # List of hashes with this tag
├── by-type/
│   └── {content_type}/
│       └── entries.json  # List of hashes of this type
└── by-date/
    └── {YYYY}/{MM}/{DD}/
        └── entries.json  # Hashes created on this date
```

### 4. Reference Graph Navigation

**Add Graph Navigator:**
```python
class Cache__Graph_Navigator(Type_Safe):
    """Navigate reference relationships"""
    
    def get_related(self, hash: Safe_Str__Hash) -> List[Safe_Str__Hash]:
        """Get related cache entries"""
        
    def get_dependencies(self, hash: Safe_Str__Hash) -> List[Safe_Str__Hash]:
        """Get entries this depends on"""
        
    def get_dependents(self, hash: Safe_Str__Hash) -> List[Safe_Str__Hash]:
        """Get entries that depend on this"""
        
    def get_reference_tree(self, hash: Safe_Str__Hash, 
                          depth: int = 3) -> dict:
        """Get reference tree structure"""
```

### 5. Admin Operations

**Add Admin Service:**
```python
class Cache__Admin_Service(Type_Safe):
    """Administrative operations"""
    
    def rebuild_indexes(self) -> bool:
        """Rebuild all search indexes from data"""
        
    def cleanup_orphans(self) -> int:
        """Remove references without data"""
        
    def optimize_storage(self) -> dict:
        """Reorganize for better performance"""
        
    def export_backup(self, path: str) -> bool:
        """Export cache for backup"""
        
    def import_backup(self, path: str) -> bool:
        """Import from backup"""
        
    def get_statistics(self) -> dict:
        """Get detailed cache statistics"""
        return {
            "total_entries": 0,
            "total_unique_hashes": 0,
            "total_versions": 0,
            "storage_bytes": 0,
            "compression_ratio": 0.0,
            "hit_rate": 0.0
        }
```

### 6. Cache Metadata Enhancement

**Enhanced `.latest.json` Structure:**
```json
{
    "hash": "a73f2e4b",
    "version": 3,
    "cache_id": "a73f2e4b-v3",
    "path": "data/2025/01/10/11/a73f2e4b-v3.json",
    "timestamp": 1734357600000,
    
    "metadata": {
        "content_type": "application/json",
        "content_encoding": "gzip",
        "size_original": 15234,
        "size_stored": 5123,
        "tags": ["production", "api-response"],
        "source": "http",
        "custom": {...}
    },
    
    "stats": {
        "total_versions": 3,
        "access_count": 42,
        "last_accessed": 1734360000000,
        "created_at": 1734350000000
    },
    
    "relationships": {
        "parent": "parent-hash",
        "children": ["child1-hash", "child2-hash"],
        "related": ["related1-hash"]
    }
}
```

## Implementation Steps

### Step 1: Versioning (Day 1)
1. Implement version tracking in refs.json
2. Add version retrieval methods
3. Add version listing endpoint
4. Add version deletion capability
5. Test multi-version scenarios

### Step 2: Batch Operations (Day 2 Morning)
1. Implement `store_batch()` with deduplication
2. Implement `retrieve_batch()` 
3. Implement `delete_batch()`
4. Add batch endpoints to routes
5. Test batch performance

### Step 3: Search Capabilities (Day 2 Afternoon - Day 3 Morning)
1. Create `Cache__Search_Manager`
2. Implement tag indexing
3. Implement type indexing
4. Implement temporal indexing
5. Add search endpoints
6. Test search performance

### Step 4: Admin Features (Day 3 Afternoon)
1. Create `Cache__Admin_Service`
2. Implement index rebuilding
3. Implement cleanup operations
4. Implement statistics gathering
5. Add admin endpoints (with extra auth)

## New API Endpoints

```
/cache/retrieve/hash/{hash}/versions    GET   List all versions
/cache/retrieve/hash/{hash}/v/{version} GET   Get specific version
/cache/batch/store                      POST  Store multiple items
/cache/batch/retrieve                   POST  Retrieve multiple items
/cache/batch/delete                     POST  Delete multiple items
/cache/search                           POST  Complex search
/cache/search/by-tag/{tag}             GET   Search by tag
/cache/search/by-type/{type}           GET   Search by type
/cache/search/temporal                  GET   Search by date range
/cache/refs/hash/{hash}                GET   Get references
/cache/refs/tree/{hash}                GET   Get reference tree
/cache/admin/rebuild-indexes            POST  Rebuild indexes
/cache/admin/cleanup                   POST  Clean orphans
/cache/admin/stats                     GET   Get statistics
```

## Testing Requirements

### Versioning Tests:
```python
def test_multiple_versions():
    """Same content creates versions"""
    data = {"test": "data"}
    v1 = cache.store_json(data)
    v2 = cache.store_json(data)  
    v3 = cache.store_json(data)
    
    versions = cache.get_versions(v1.hash)
    assert len(versions) == 3
    assert versions[0]['version'] == 1
    assert versions[2]['version'] == 3
```

### Batch Tests:
```python
def test_batch_deduplication():
    """Batch detects duplicates"""
    items = [
        {"data": "same"},
        {"data": "same"},  # Duplicate
        {"data": "different"}
    ]
    results = cache.store_batch(items)
    assert len(results) == 2  # Only 2 unique
```

### Search Tests:
```python
def test_search_by_tag():
    """Search finds tagged entries"""
    cache.store_json({"data": 1}, tags=["test", "one"])
    cache.store_json({"data": 2}, tags=["test", "two"])
    
    results = cache.search_by_tag("test")
    assert len(results) == 2
```

## Performance Targets

- Version listing: < 50ms for 100 versions
- Batch store: < 500ms for 100 items
- Search by tag: < 100ms for 1000 entries
- Index rebuild: < 60s for 100k entries

## Success Metrics

- [ ] Multiple versions tracked per hash
- [ ] Batch operations deduplicate correctly
- [ ] Search returns relevant results
- [ ] Admin can rebuild corrupted indexes
- [ ] Statistics show accurate metrics
- [ ] All advanced endpoints functional