# Phase 1: Fix Core Hash-Based Storage

## Goal
Transform the cache from ID-centric to hash-centric storage while maintaining backward compatibility.

## Changes to `Cache__Service`

### 1. Fix `generate_cache_hash()` 
Current method is adequate but needs to be the PRIMARY identifier:
```python
# CURRENT: Hash is calculated but ignored
def store(self, request, namespace):
    cache_id = Random_Guid()  # WRONG: This should not be primary
    hash_value = self.generate_cache_hash(request.data)  # Calculated but not used properly
```

**CHANGE TO:**
```python
def store(self, request, namespace):
    hash_value = self.generate_cache_hash(request.data)
    version = self._get_next_version(hash_value, namespace)
    cache_id = f"{hash_value}-v{version}"  # Deterministic ID
```

### 2. Add Reference Management

**NEW CLASS NEEDED**: `Cache__Reference_Manager`
```python
class Cache__Reference_Manager(Type_Safe):
    """Manages hash-based references for fast lookups"""
    
    def get_latest_ref(self, hash: Safe_Str__Hash, namespace: Safe_Id) -> dict
    def update_refs(self, hash: Safe_Str__Hash, cache_id: str, path: str) -> bool
    def get_all_versions(self, hash: Safe_Str__Hash, namespace: Safe_Id) -> list
    def shard_path(self, hash: Safe_Str__Hash) -> str  # "a7/3f" from "a73f2e4b..."
```

### 3. Fix `retrieve()` Method

**CURRENT PROBLEM:**
```python
def retrieve(self, request, namespace):
    if request.hash:
        return dict(status='error', message='retrieval by hash not supported')  # BROKEN!
```

**CHANGE TO:**
```python
def retrieve(self, request, namespace):
    if request.hash:
        return self._retrieve_by_hash(request.hash, namespace, request.version)
    elif request.cache_id:
        # Extract hash from cache_id format: "{hash}-v{version}"
        hash_value = self._extract_hash_from_id(request.cache_id)
        return self._retrieve_by_hash(hash_value, namespace)
```

## Changes to Storage Layer

### 1. Modify `Cache__Handler`

**REMOVE:**
- `fs__temporal` - Keep only for data storage
- `fs__latest_temporal` - Redundant

**ADD:**
```python
class Cache__Handler(Type_Safe):
    refs_manager: Cache__Reference_Manager  # NEW
    
    def store_with_hash(self, hash: Safe_Str__Hash, data: Any, version: int) -> str:
        """Store data with hash-based path"""
        path = f"{hash}-v{version}"
        # Store in temporal but reference by hash
```

### 2. Create Reference Structure

**NEW METHOD in Cache__Handler:**
```python
def setup_references(self):
    """Create refs/by-hash structure"""
    # refs/by-hash/{h1}/{h2}/{hash}.latest.json
    # refs/by-hash/{h1}/{h2}/{hash}.refs.json
```

## Changes to Routes

### 1. Fix `Routes__Cache.retrieve_by_hash()`

**CURRENT:**
```python
def retrieve_by_hash(self, hash, namespace):
    return self.retrieve(cache_id=None, hash=hash, namespace=namespace)
    # Returns "not supported" error
```

**CHANGE TO:**
```python
def retrieve_by_hash(self, hash: Safe_Str__Hash, namespace: Safe_Id = None):
    request = Schema__Cache__Retrieve__Request(hash=hash)
    return self.cache_service.retrieve(request, namespace or Safe_Id("default"))
```

### 2. Add Version Endpoints

**NEW ROUTES NEEDED:**
```python
def get_versions(self, hash: Safe_Str__Hash, namespace: Safe_Id = None) -> dict:
    """GET /cache/retrieve/hash/{hash}/versions"""
    
def get_version(self, hash: Safe_Str__Hash, version: int, namespace: Safe_Id = None) -> dict:
    """GET /cache/retrieve/hash/{hash}/v/{version}"""
```

## File Structure Changes

### Current Structure (WRONG):
```
data/2025/01/10/11/cache-id-abc123.json
data/2025/01/10/11/cache-id-def456.json  # Could be same content!
```

### New Structure (CORRECT):
```
data/2025/01/10/11/a73f2e4b-v1.json
data/2025/01/10/11/a73f2e4b-v2.json  # Clear versioning
refs/by-hash/a7/3f/a73f2e4b.latest.json  # Points to v2
refs/by-hash/a7/3f/a73f2e4b.refs.json    # Lists v1, v2
```

## Implementation Steps

### Step 1: Add Reference Manager (Day 1 Morning)
1. Create `Cache__Reference_Manager` class
2. Add sharding logic (`hash[:2]/hash[2:4]/`)
3. Implement `.latest.json` creation with embedded metadata
4. Implement `.refs.json` for version listing

### Step 2: Modify Store Method (Day 1 Afternoon)
1. Change `store()` to use hash as primary key
2. Implement version checking
3. Create references after storing data
4. Update both `.latest.json` and `.refs.json`

### Step 3: Fix Retrieve Methods (Day 2 Morning)
1. Implement `_retrieve_by_hash()`
2. Read from `.latest.json` for metadata
3. Only read data file if needed
4. Fix `retrieve_by_hash` endpoint

### Step 4: Add Version Support (Day 2 Afternoon)
1. Add `/versions` endpoint
2. Add `/v/{version}` endpoint
3. Implement version listing
4. Test version retrieval

## Testing Requirements

### Unit Tests to Add:
1. `test_store_same_content_twice` - Should create versions, not duplicates
2. `test_retrieve_by_hash_works` - Currently returns error
3. `test_version_increments` - v1, v2, v3 for same hash
4. `test_latest_json_single_read` - Metadata without data read

### Integration Tests:
1. Store → Retrieve by hash → Verify content
2. Store multiple versions → List versions → Retrieve specific version
3. Verify sharding creates correct directory structure

## Backward Compatibility

During transition:
1. Support both old cache_id and new hash retrieval
2. Migration script to convert existing entries
3. Keep temporal organization for data files
4. Add references for existing entries

## Success Metrics

- [ ] `retrieve_by_hash` returns data, not error
- [ ] Same content stored twice creates version, not duplicate
- [ ] Metadata retrieval requires only 1 file read
- [ ] Hash sharding creates `a7/3f/` structure
- [ ] All existing tests still pass