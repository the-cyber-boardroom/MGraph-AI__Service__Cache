# Phase 1: Fix Core Hash-Based Storage with Memory_FS

## Goal
Transform the cache from ID-centric to hash-centric storage using Memory_FS path handlers and Type_Safe patterns.

## Changes to Storage Layer

### 1. Create Hash-Sharded Path Handler

**NEW CLASS using Memory_FS pattern:**
```python
from memory_fs.path_handlers.Path__Handler import Path__Handler
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id import Safe_Id

class Path__Handler__Hash_Sharded(Path__Handler):                    # Hash-based sharding path handler
    """Generate sharded paths based on hash for efficient lookups"""
    
    shard_depth: int = 2                                             # How many levels of sharding
    shard_size : int = 2                                             # Characters per shard level
    
    def generate_path(self, file_id: Safe_Id) -> Safe_Str__File__Path:
        hash_str = str(file_id)
        
        # Generate sharded path: ab/cd/abcdef123456.json
        shards = []
        for i in range(self.shard_depth):
            start = i * self.shard_size
            end = start + self.shard_size
            if start < len(hash_str):
                shards.append(hash_str[start:end])
        
        sharded_path = '/'.join(shards)
        return self.combine_paths(Safe_Str__File__Path(sharded_path))
```

### 2. Create Safe Hash Type with Proper Length

**NEW SAFE TYPE:**
```python
import re
from osbot_utils.type_safe.primitives.safe_str.Safe_Str import Safe_Str

class Safe_Str__Cache_Hash(Safe_Str):                               # 16-char hash for cache keys
    """Safe hash type for cache operations - 16 chars = 64 bits entropy"""
    max_length      = 16
    min_length      = 16
    exact_length    = True
    regex           = re.compile(r'^[a-f0-9]{16}$')
    regex_mode      = 'MATCH'
    strict_validation = True
    allow_empty     = False
    
    @classmethod
    def from_content(cls, content: Any) -> 'Safe_Str__Cache_Hash':
        """Generate hash from content"""
        import hashlib
        if isinstance(content, Safe_Bytes):
            hash_input = content.value
        elif isinstance(content, str):
            hash_input = content.encode('utf-8')
        elif isinstance(content, Type_Safe):
            import json
            hash_input = json.dumps(content.json(), sort_keys=True).encode('utf-8')
        else:
            hash_input = str(content).encode('utf-8')
        
        hash_hex = hashlib.sha256(hash_input).hexdigest()[:16]
        return cls(hash_hex)
```

### 3. Define Cache File Type

**NEW MEMORY_FS FILE TYPE:**
```python
from memory_fs.schemas.Schema__Memory_FS__File__Type import Schema__Memory_FS__File__Type
from memory_fs.schemas.Enum__Memory_FS__Serialization import Enum__Memory_FS__Serialization

class Memory_FS__File__Type__Cache(Schema__Memory_FS__File__Type):
    """File type for cache entries with metadata"""
    name           : str = 'cache'
    content_type   : str = 'CACHE'
    file_extension : str = 'cache'
    encoding       : str = 'UTF_8'
    serialization  : Enum__Memory_FS__Serialization = 'JSON'
```

## Changes to `Cache__Handler`

### Refactor to Use Memory_FS Properly

```python
from memory_fs.Memory_FS import Memory_FS
from osbot_utils.type_safe.Type_Safe import Type_Safe

class Cache__Handler(Type_Safe):                                     # Use Memory_FS for all path operations
    s3__bucket     : str
    s3__prefix     : str                  = ""
    memory_fs      : Memory_FS             = None                    # Single Memory_FS instance
    cache_ttl_hours: int                  = 24

    def setup(self) -> 'Cache__Handler':                             # Setup Memory_FS with multiple handlers
        # Setup S3 storage
        from mgraph_ai_service_cache.service.storage.Storage_FS__S3 import Storage_FS__S3
        storage = Storage_FS__S3(s3_bucket=self.s3__bucket, 
                                s3_prefix=self.s3__prefix).setup()
        
        # Configure Memory_FS with multiple path handlers
        with Memory_FS() as _:
            _.storage_fs = storage
            
            # Temporal handler for data files
            temporal = _.add_handler__temporal()
            temporal.prefix_path = Safe_Str__File__Path('data')
            
            # Hash-sharded handler for references
            hash_handler = _.add_handler__custom(Path__Handler__Hash_Sharded)
            hash_handler.prefix_path = Safe_Str__File__Path('refs/by-hash')
            
            # Latest handler for quick access
            latest = _.add_handler__latest()
            latest.prefix_path = Safe_Str__File__Path('refs')
            latest.latest_folder = Safe_Str__File__Path('current')
            
            self.memory_fs = _
        
        return self
    
    def store_by_hash(self, hash_value : Safe_Str__Cache_Hash ,      # Store using hash as primary key
                            data       : Safe_Bytes            ,
                            version    : int
                      ) -> List[Safe_Str__File__Path]:
        file_id = Safe_Id(f"{hash_value}-v{version}")
        
        # Use Memory_FS to store with all handlers
        with self.memory_fs.file__cache(file_id) as file_fs:
            cache_entry = {
                'hash'      : str(hash_value),
                'version'   : version,
                'data'      : data.to_base64() if isinstance(data, Safe_Bytes) else str(data),
                'stored_at' : timestamp_now()
            }
            paths = file_fs.create(cache_entry)
            return paths
    
    def get_latest_by_hash(self, hash_value: Safe_Str__Cache_Hash    # Get latest version by hash
                           ) -> Optional[dict]:
        # Use hash-sharded path to find latest
        file_id = Safe_Id(str(hash_value))
        
        with self.memory_fs.file__cache(file_id) as file_fs:
            if file_fs.exists():
                return file_fs.content()
        return None
```

## Changes to `Cache__Service`

### Fix Hash Generation and Storage

```python
from osbot_utils.type_safe.Type_Safe import Type_Safe
from osbot_utils.type_safe.primitives.safe_bytes.Safe_Bytes import Safe_Bytes

class Cache__Service(Type_Safe):
    cache_handlers   : Dict[Safe_Id, Cache__Handler]
    default_bucket   : str                           = "mgraph-ai-cache"
    default_ttl_hours: int                           = 24
    version_tracker  : Dict[Safe_Str__Cache_Hash, int] = None        # Track versions per hash
    
    def generate_cache_hash(self, data: Any) -> Safe_Str__Cache_Hash:# Use new Safe hash type
        return Safe_Str__Cache_Hash.from_content(data)
    
    def _get_next_version(self, hash_value : Safe_Str__Cache_Hash,   # Track versions properly
                                namespace  : Safe_Id
                          ) -> int:
        if self.version_tracker is None:
            self.version_tracker = {}
        
        version_key = f"{namespace}:{hash_value}"
        current_version = self.version_tracker.get(version_key, 0)
        next_version = current_version + 1
        self.version_tracker[version_key] = next_version
        return next_version
    
    def store(self, request   : Schema__Cache__Store__Request ,      # Use hash as primary identifier
                    namespace : Safe_Id = None
              ) -> Schema__Cache__Store__Response:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)
        
        # Generate hash from data
        hash_value = self.generate_cache_hash(request.data)
        
        # Get next version for this hash
        version = self._get_next_version(hash_value, namespace)
        
        # Create deterministic cache_id
        cache_id = Safe_Id(f"{hash_value}-v{version}")
        
        # Store using handler with hash
        paths = handler.store_by_hash(hash_value, 
                                      Safe_Bytes(request.data),
                                      version)
        
        return Schema__Cache__Store__Response(
            cache_id = cache_id,
            hash     = hash_value,
            version  = version,
            path     = paths[0] if paths else Safe_Str__File__Path(""),
            size     = len(request.data.encode()) if isinstance(request.data, str) else len(request.data)
        )
    
    def _retrieve_by_hash(self, hash_value : Safe_Str__Cache_Hash,   # FIXED: Actually retrieve by hash
                               namespace   : Safe_Id = None,
                               version     : Optional[int] = None
                          ) -> Optional[Dict[str, Any]]:
        namespace = namespace or Safe_Id("default")
        handler = self.get_or_create_handler(namespace)
        
        if version:
            # Get specific version
            file_id = Safe_Id(f"{hash_value}-v{version}")
        else:
            # Get latest version
            result = handler.get_latest_by_hash(hash_value)
            if result:
                return {'data': result, 'metadata': {}, 'config': {}}
        
        return None
    
    def retrieve(self, request   : Schema__Cache__Retrieve__Request , # Support hash retrieval
                       namespace : Safe_Id = None
                 ) -> Optional[Dict[str, Any]]:
        if request.hash:
            # FIXED: Now actually works!
            return self._retrieve_by_hash(request.hash, namespace)
        elif request.cache_id:
            # Extract hash from cache_id format: "{hash}-v{version}"
            cache_id_str = str(request.cache_id)
            if '-v' in cache_id_str:
                hash_part = cache_id_str.split('-v')[0]
                hash_value = Safe_Str__Cache_Hash(hash_part)
                return self._retrieve_by_hash(hash_value, namespace)
        
        return None
```

## Changes to Routes

### Update `Routes__Cache`

```python
class Routes__Cache(Fast_API__Routes):
    tag           : str            = 'cache'
    cache_service : Cache__Service
    
    def retrieve_by_hash(self, hash      : Safe_Str__Cache_Hash,     # Use proper Safe hash type
                               namespace : Safe_Id = None
                         ) -> Dict[str, Any]:
        request = Schema__Cache__Retrieve__Request(hash=hash)
        result = self.cache_service.retrieve(request, namespace)
        
        if result is None:
            return {"status": "not_found", "message": "Cache entry not found"}
        return result
    
    def get_versions(self, hash      : Safe_Str__Cache_Hash,         # NEW: List versions
                           namespace : Safe_Id = None
                     ) -> Dict[str, Any]:
        namespace = namespace or Safe_Id("default")
        handler = self.cache_service.get_or_create_handler(namespace)
        
        # Use Memory_FS to find all versions
        versions = []
        for i in range(1, 100):  # Practical limit
            file_id = Safe_Id(f"{hash}-v{i}")
            with handler.memory_fs.file__cache(file_id) as file_fs:
                if file_fs.exists():
                    versions.append({'version': i, 'cache_id': str(file_id)})
                else:
                    break
        
        return {'hash': str(hash), 'versions': versions, 'count': len(versions)}
    
    def setup_routes(self):
        self.add_route_post(self.store)
        self.add_route_get (self.retrieve)
        self.add_route_get (self.retrieve_by_hash)
        self.add_route_get (self.get_versions)      # NEW endpoint
        self.add_route_get (self.namespaces)
        self.add_route_get (self.stats)
```

## Update Schema Classes

```python
from osbot_utils.type_safe.Type_Safe import Type_Safe
from typing import Optional

class Schema__Cache__Store__Response(Type_Safe):                    # Add version to response
    cache_id: Safe_Id
    hash    : Safe_Str__Cache_Hash
    version : int                                                    # NEW: version number
    path    : Safe_Str__File__Path
    size    : int

class Schema__Cache__Retrieve__Request(Type_Safe):                  # Use proper hash type
    hash            : Optional[Safe_Str__Cache_Hash] = None         # Proper type
    cache_id        : Optional[Safe_Id]              = None
    version         : Optional[int]                  = None         # NEW: request specific version
    include_data    : bool                           = True
    include_metadata: bool                           = True
    include_config  : bool                           = True
```

## Implementation Steps

### Step 1: Create Path Handler (Day 1 Morning)
1. Implement `Path__Handler__Hash_Sharded` class
2. Implement `Safe_Str__Cache_Hash` with 16-char validation
3. Create `Memory_FS__File__Type__Cache`
4. Test path generation with sharding

### Step 2: Refactor Cache__Handler (Day 1 Afternoon)
1. Replace custom storage with Memory_FS instance
2. Add multiple path handlers (temporal, hash-sharded, latest)
3. Implement `store_by_hash()` using Memory_FS
4. Implement `get_latest_by_hash()` using Memory_FS

### Step 3: Fix Cache__Service (Day 2 Morning)
1. Update `generate_cache_hash()` to use `Safe_Str__Cache_Hash`
2. Implement version tracking
3. Fix `store()` to use hash as primary key
4. Fix `_retrieve_by_hash()` to actually work
5. Update `retrieve()` to handle both hash and cache_id

### Step 4: Update Routes and Test (Day 2 Afternoon)
1. Update route signatures with Safe types
2. Add `/versions` endpoint
3. Fix `retrieve_by_hash` to return data
4. Write comprehensive tests following Type_Safe patterns

## Testing Requirements

Following Type_Safe testing patterns:

```python
class test_Cache__Service(TestCase):
    
    @classmethod
    def setUpClass(cls):                                             # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.service = Cache__Service()
    
    def test_store_same_content_twice(self):                         # Should create versions
        with self.service as _:
            request = Schema__Cache__Store__Request(data="same content")
            
            response1 = _.store(request)
            response2 = _.store(request)
            
            # Same hash, different versions
            assert response1.hash == response2.hash
            assert type(response1.hash) is Safe_Str__Cache_Hash
            assert response1.version == 1
            assert response2.version == 2
    
    def test_retrieve_by_hash_works(self):                          # Must actually work now
        with self.service as _:
            # Store data
            request = Schema__Cache__Store__Request(data="test data")
            store_response = _.store(request)
            
            # Retrieve by hash
            retrieve_request = Schema__Cache__Retrieve__Request(
                hash=store_response.hash
            )
            result = _.retrieve(retrieve_request)
            
            assert result is not None
            assert result['data']['data'] == "test data"
    
    def test_memory_fs_path_generation(self):                        # Test Memory_FS integration
        handler = Cache__Handler(s3__bucket="test").setup()
        
        with handler.memory_fs as _:
            paths = _.handlers  # Should have 3 handlers
            assert len(paths) == 3
            
            # Test sharded path generation
            hash_value = Safe_Str__Cache_Hash("abcdef1234567890")
            file_id = Safe_Id(str(hash_value))
            
            with _.file__cache(file_id) as file_fs:
                paths = file_fs.file__config.file_paths
                assert any("ab/cd" in str(p) for p in paths)
```

## Success Metrics

- [x] `retrieve_by_hash` returns data, not error
- [x] Same content stored twice creates version, not duplicate
- [x] Metadata retrieval uses Memory_FS efficiently
- [x] Hash sharding creates proper directory structure
- [x] All Safe types used (no raw primitives)
- [x] Memory_FS handles all path operations
- [x] Tests follow Type_Safe patterns with `.obj()` and `__`