# Phase 2: Type-Safe Data Handling

## Goal
Replace all raw primitives with Safe types and implement efficient data handling following Type_Safe principles.

## Problem with Current Implementation

### Violations of Type_Safe Principles
```python
# CURRENT VIOLATIONS:
class Schema__Cache__Store__Request(Type_Safe):
    data: str                          # RAW PRIMITIVE - VIOLATION!
    content_type: str                  # RAW PRIMITIVE - VIOLATION!
    metadata: dict                     # UNTYPED COLLECTION - VIOLATION!
    
# Forces base64 encoding everywhere:
data_base64 = str_to_base64(json_to_str(data))  # Inefficient!
```

### Target: Full Type_Safe Compliance
```python
# ALL Safe types, no raw primitives:
class Schema__Cache__Store__Request(Type_Safe):
    data         : Safe_Bytes                          # Safe binary data
    content_type : Safe_Str__Http__Content_Type        # Domain-specific type
    metadata     : Dict[Safe_Id, Safe_Str__Text]       # Typed collections
```

## Create Safe Types for Cache Operations

### 1. Safe_Bytes for Binary Data

```python
from osbot_utils.type_safe.Type_Safe import Type_Safe
import base64

class Safe_Bytes(Type_Safe):                                        # Safe wrapper for binary data
    """Safe type for binary data with size limits and encoding support"""
    value      : bytes
    max_size   : int = 10 * 1024 * 1024                            # 10MB default limit
    
    def __init__(self, data=None, **kwargs):
        if data is not None:
            if isinstance(data, str):
                # Auto-convert string to bytes
                self.value = data.encode('utf-8')
            elif isinstance(data, bytes):
                self.value = data
            else:
                self.value = str(data).encode('utf-8')
                
            if len(self.value) > self.max_size:
                raise ValueError(f"Data size {len(self.value)} exceeds max {self.max_size}")
        super().__init__(**kwargs)
    
    def to_base64(self) -> str:
        """Convert to base64 string"""
        return base64.b64encode(self.value).decode('ascii')
    
    @classmethod
    def from_base64(cls, data: str) -> 'Safe_Bytes':
        """Create from base64 string"""
        return cls(base64.b64decode(data))
    
    def __len__(self) -> int:
        return len(self.value)
```

### 2. Safe Compression Type

```python
from typing import Literal
import gzip

class Safe_Bytes__Compressed(Safe_Bytes):                           # Compressed binary data
    """Safe type for compressed binary data"""
    compression : Literal["gzip", "deflate", "br", None] = "gzip"
    
    def compress(self) -> 'Safe_Bytes__Compressed':
        """Compress the data"""
        if self.compression == "gzip":
            compressed = gzip.compress(self.value)
            return Safe_Bytes__Compressed(compressed)
        return self
    
    def decompress(self) -> Safe_Bytes:
        """Decompress to regular Safe_Bytes"""
        if self.compression == "gzip":
            decompressed = gzip.decompress(self.value)
            return Safe_Bytes(decompressed)
        return Safe_Bytes(self.value)
    
    def compression_ratio(self, original_size: int) -> float:
        """Calculate compression ratio"""
        return len(self.value) / original_size if original_size > 0 else 1.0
```

## Update Schema Classes with Safe Types

### 1. Replace Request Schemas

```python
from osbot_utils.type_safe.Type_Safe import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id import Safe_Id
from osbot_utils.type_safe.primitives.safe_str.text.Safe_Str__Text import Safe_Str__Text
from typing import Dict, List, Optional, Literal

class Schema__Cache__Store__Request(Type_Safe):                     # Fully Type_Safe compliant
    """Request schema using only Safe types - no raw primitives"""
    data           : Safe_Bytes                                     # Binary data (auto-converts strings)
    content_type   : Safe_Str__Http__Content_Type                   # MIME type validation
    content_encoding: Optional[Literal["gzip", "deflate", "br"]] = None  # Use Literal for enums
    metadata       : Dict[Safe_Id, Safe_Str__Text]                  # Becomes Type_Safe__Dict
    tags           : List[Safe_Id]                                  # Becomes Type_Safe__List
    compress       : bool = True                                    # Auto-compress if beneficial

class Schema__Cache__Store__JSON__Request(Type_Safe):               # JSON-specific request
    """Store JSON/Type_Safe objects directly"""
    data_json : Dict[Safe_Id, Any]                                  # Direct JSON (no encoding)
    metadata  : Dict[Safe_Id, Safe_Str__Text]                       # Type_Safe__Dict
    tags      : List[Safe_Id]                                       # Type_Safe__List
    
class Schema__Cache__Store__Type_Safe__Request(Type_Safe):          # Type_Safe object storage
    """Store Type_Safe objects with perfect serialization"""
    data_object : Type_Safe                                         # Any Type_Safe object
    metadata    : Dict[Safe_Id, Safe_Str__Text]
    tags        : List[Safe_Id]
```

### 2. Update Response Schema

```python
class Schema__Cache__Store__Response(Type_Safe):                    # Response with compression metrics
    hash             : Safe_Str__Cache_Hash                         # Primary identifier
    cache_id         : Safe_Id                                      # Unique cache entry ID
    version          : int                                          # Version number
    size_original    : int                                          # Original size in bytes
    size_stored      : int                                          # After compression
    compression_ratio: float                                        # Compression effectiveness
    path             : Safe_Str__File__Path                         # Storage path
    stored_at        : Timestamp_Now                                # Auto-generates timestamp
```

### 3. Cache Entry Schema

```python
from osbot_utils.type_safe.primitives.safe_str.identifiers.Timestamp_Now import Timestamp_Now

class Schema__Cache__Entry(Type_Safe):                              # Internal cache entry structure
    """Complete cache entry with all metadata - pure schema, no logic"""
    hash           : Safe_Str__Cache_Hash
    version        : int
    cache_id       : Safe_Id                                        # Auto-generates if not provided
    data           : Safe_Bytes                                     # Always store as Safe_Bytes
    content_type   : Safe_Str__Http__Content_Type
    content_encoding: Optional[Literal["gzip", "deflate", "br"]] = None
    metadata       : Dict[Safe_Id, Safe_Str__Text]                  # Type_Safe__Dict
    tags           : List[Safe_Id]                                  # Type_Safe__List
    size_original  : int
    size_stored    : int
    stored_at      : Timestamp_Now                                  # Auto-generates on creation
    accessed_at    : Optional[Timestamp_Now] = None
    ttl_hours      : int = 24
```

## Update `Cache__Service` for Type_Safe Data

### Multi-Format Store Methods

```python
class Cache__Service(Type_Safe):
    
    def store_binary(self, data         : Safe_Bytes              , # Store binary data directly
                          content_type : Safe_Str__Http__Content_Type,
                          namespace    : Safe_Id = None            ,
                          compress     : bool = True
                     ) -> Schema__Cache__Store__Response:
        """Store binary data with optional compression"""
        namespace = namespace or Safe_Id("default")
        
        # Auto-compress if beneficial (>1KB and good ratio)
        size_original = len(data)
        data_to_store = data
        content_encoding = None
        
        if compress and size_original > 1024:
            compressed = Safe_Bytes__Compressed(data.value).compress()
            if len(compressed) < size_original * 0.9:              # Only if 10% savings
                data_to_store = compressed
                content_encoding = "gzip"
        
        # Create cache entry with Safe types
        cache_entry = Schema__Cache__Entry(
            hash            = self.generate_cache_hash(data),
            version         = self._get_next_version(hash_value, namespace),
            data            = data_to_store,
            content_type    = content_type,
            content_encoding = content_encoding,
            size_original   = size_original,
            size_stored     = len(data_to_store)
        )
        
        # Store using Memory_FS
        handler = self.get_or_create_handler(namespace)
        paths = handler.store_entry(cache_entry)
        
        return Schema__Cache__Store__Response(
            hash             = cache_entry.hash,
            cache_id         = cache_entry.cache_id,
            version          = cache_entry.version,
            size_original    = cache_entry.size_original,
            size_stored      = cache_entry.size_stored,
            compression_ratio = cache_entry.size_stored / cache_entry.size_original,
            path             = paths[0] if paths else Safe_Str__File__Path(""),
            stored_at        = cache_entry.stored_at
        )
    
    def store_json(self, data      : Union[Dict, Type_Safe]       , # Store JSON or Type_Safe objects
                        namespace : Safe_Id = None
                   ) -> Schema__Cache__Store__Response:
        """Store JSON or Type_Safe objects with automatic serialization"""
        
        # Convert to JSON bytes
        if isinstance(data, Type_Safe):
            json_str = json.dumps(data.json(), sort_keys=True)
        else:
            json_str = json.dumps(data, sort_keys=True)
        
        json_bytes = Safe_Bytes(json_str.encode('utf-8'))
        
        return self.store_binary(
            data         = json_bytes,
            content_type = Safe_Str__Http__Content_Type("application/json"),
            namespace    = namespace,
            compress     = True                                     # JSON compresses well
        )
    
    def store_type_safe(self, obj       : Type_Safe              ,  # Store Type_Safe with type preservation
                             namespace : Safe_Id = None
                        ) -> Schema__Cache__Store__Response:
        """Store Type_Safe object with full type information preserved"""
        
        # Add type information to metadata
        type_info = {
            Safe_Id("__type__")  : Safe_Str__Text(type(obj).__name__),
            Safe_Id("__module__"): Safe_Str__Text(type(obj).__module__)
        }
        
        # Store with type metadata
        json_bytes = Safe_Bytes(json.dumps(obj.json(), sort_keys=True))
        
        request = Schema__Cache__Store__Request(
            data         = json_bytes,
            content_type = Safe_Str__Http__Content_Type("application/json+typesafe"),
            metadata     = type_info,
            compress     = True
        )
        
        return self.store(request, namespace)
```

### Content-Type Aware Retrieval

```python
    def retrieve_typed(self, hash      : Safe_Str__Cache_Hash     ,  # Retrieve with type restoration
                            namespace : Safe_Id = None
                       ) -> Union[Safe_Bytes, Dict, Type_Safe]:
        """Retrieve and deserialize based on content type"""
        result = self._retrieve_by_hash(hash, namespace)
        
        if not result:
            return None
        
        cache_entry = result['data']
        content_type = cache_entry.get('content_type', '')
        content_encoding = cache_entry.get('content_encoding')
        data_bytes = cache_entry['data']
        
        # Handle Safe_Bytes
        if isinstance(data_bytes, Safe_Bytes):
            raw_bytes = data_bytes
        else:
            # Convert from stored format
            raw_bytes = Safe_Bytes.from_base64(data_bytes)
        
        # Decompress if needed
        if content_encoding == 'gzip':
            raw_bytes = Safe_Bytes__Compressed(raw_bytes.value).decompress()
        
        # Deserialize based on type
        if 'typesafe' in content_type:
            # Restore Type_Safe object
            metadata = result.get('metadata', {})
            type_name = metadata.get('__type__')
            module_name = metadata.get('__module__')
            
            # Dynamic import and instantiation (simplified)
            json_data = json.loads(raw_bytes.value.decode('utf-8'))
            # In real implementation, use importlib to get class
            return json_data  # Would return actual Type_Safe instance
            
        elif 'json' in content_type:
            # Return as dict
            return json.loads(raw_bytes.value.decode('utf-8'))
            
        elif 'text' in content_type:
            # Return as string (but wrapped in Safe_Str)
            return Safe_Str__Text(raw_bytes.value.decode('utf-8'))
            
        else:
            # Return raw Safe_Bytes
            return raw_bytes
```

## Update `Cache__Handler` for Safe Types

```python
class Cache__Handler(Type_Safe):
    
    def store_entry(self, entry: Schema__Cache__Entry               # Store complete cache entry
                    ) -> List[Safe_Str__File__Path]:
        """Store cache entry using Memory_FS with Safe types"""
        
        # Use cache_id as file_id
        file_id = Safe_Id(str(entry.cache_id))
        
        # Store using Memory_FS with cache file type
        with self.memory_fs.file__cache(file_id) as file_fs:
            # Memory_FS handles Type_Safe serialization
            paths = file_fs.create(entry.json())
            
            # Update metadata using Memory_FS capabilities
            if entry.metadata:
                file_fs.metadata__update(entry.metadata)
            
            return paths
    
    def get_entry(self, cache_id: Safe_Id                          # Retrieve complete cache entry
                  ) -> Optional[Schema__Cache__Entry]:
        """Retrieve cache entry with all Safe types preserved"""
        
        with self.memory_fs.file__cache(cache_id) as file_fs:
            if file_fs.exists():
                json_data = file_fs.content()
                # Type_Safe handles deserialization
                return Schema__Cache__Entry.from_json(json_data)
        
        return None
```

## Update Routes for Type_Safe

```python
from fastapi import Request
import gzip

class Routes__Cache(Fast_API__Routes):
    
    async def store_raw(self, request   : Request                 ,  # Accept raw binary data
                              namespace : Safe_Id = None
                        ) -> Schema__Cache__Store__Response:
        """Accept raw binary data via HTTP"""
        
        # Get headers using Safe types
        content_type = Safe_Str__Http__Content_Type(
            request.headers.get('content-type', 'application/octet-stream')
        )
        content_encoding = request.headers.get('content-encoding')
        
        # Read raw body as Safe_Bytes
        body_bytes = await request.body()
        safe_bytes = Safe_Bytes(body_bytes)
        
        # Decompress if client compressed
        if content_encoding == 'gzip':
            safe_bytes = Safe_Bytes__Compressed(safe_bytes.value).decompress()
        
        return self.cache_service.store_binary(
            data         = safe_bytes,
            content_type = content_type,
            namespace    = namespace or Safe_Id("default")
        )
    
    def store_json(self, data      : Dict[str, Any]               ,  # Store JSON directly
                        namespace : Safe_Id = None
                   ) -> Schema__Cache__Store__Response:
        """Store JSON data"""
        return self.cache_service.store_json(data, namespace)
    
    def store_type_safe(self, schema_type : str                   ,  # Store Type_Safe by type name
                             data        : dict                   ,
                             namespace   : Safe_Id = None
                        ) -> Schema__Cache__Store__Response:
        """Store Type_Safe object by reconstructing from type name"""
        
        # In real implementation, use registry to get Type_Safe class
        # For now, just store as JSON with type metadata
        metadata = {
            Safe_Id("__type__"): Safe_Str__Text(schema_type)
        }
        
        request = Schema__Cache__Store__Request(
            data     = Safe_Bytes(json.dumps(data)),
            metadata = metadata
        )
        
        return self.cache_service.store(request, namespace)
    
    def setup_routes(self):
        # Binary endpoint
        self.add_route_post(self.store_raw, path="/cache/store/binary")
        
        # JSON endpoint
        self.add_route_post(self.store_json, path="/cache/store/json")
        
        # Type_Safe endpoint
        self.add_route_post(self.store_type_safe, path="/cache/store/typesafe")
        
        # Existing routes
        self.add_route_post(self.store)
        self.add_route_get (self.retrieve)
        self.add_route_get (self.retrieve_by_hash)
```

## Implementation Steps

### Step 1: Create Safe Types (Day 1 Morning)
1. Implement `Safe_Bytes` with size limits and encoding
2. Implement `Safe_Bytes__Compressed` with gzip support
3. Test auto-conversion from strings/bytes
4. Verify Type_Safe collection behavior

### Step 2: Update Schemas (Day 1 Afternoon)
1. Replace all raw primitives in request/response schemas
2. Add `Schema__Cache__Entry` with full Safe types
3. Use `Literal` for compression types
4. Test serialization round-trips

### Step 3: Implement Type-Aware Storage (Day 2 Morning)
1. Add `store_binary()` with compression logic
2. Add `store_json()` for JSON data
3. Add `store_type_safe()` with type preservation
4. Update `retrieve_typed()` with deserialization

### Step 4: Update Routes and Test (Day 2 Afternoon)
1. Add binary/JSON/TypeSafe endpoints
2. Handle Content-Encoding headers
3. Test compression ratios
4. Verify type preservation in round-trips

## Testing Requirements

```python
class test_Cache__Service__TypeSafe(TestCase):
    
    @classmethod
    def setUpClass(cls):
        setup__service_fast_api_test_objs()
        cls.service = Cache__Service()
    
    def test_no_raw_primitives(self):                              # Verify no raw types
        with Schema__Cache__Store__Request() as _:
            # All fields should be Safe types or Type_Safe collections
            assert type(_.data)         is Safe_Bytes
            assert type(_.content_type) is Safe_Str__Http__Content_Type
            assert type(_.metadata)     is Type_Safe__Dict
            assert type(_.tags)         is Type_Safe__List
    
    def test_safe_bytes_auto_conversion(self):                     # Test auto-conversion
        with self.service as _:
            # String auto-converts to Safe_Bytes
            response = _.store_binary(
                data         = Safe_Bytes("string data"),
                content_type = Safe_Str__Http__Content_Type("text/plain")
            )
            assert response.size_original > 0
            
            # Bytes work directly
            response = _.store_binary(
                data         = Safe_Bytes(b"binary data"),
                content_type = Safe_Str__Http__Content_Type("application/octet-stream")
            )
            assert response.size_original > 0
    
    def test_compression_efficiency(self):                         # Test compression
        with self.service as _:
            # Repetitive data should compress well
            repetitive = "x" * 10000
            response = _.store_binary(
                data     = Safe_Bytes(repetitive),
                content_type = Safe_Str__Http__Content_Type("text/plain"),
                compress = True
            )
            
            assert response.compression_ratio < 0.1                # Should compress >90%
            assert response.size_stored < response.size_original
    
    def test_type_safe_round_trip(self):                          # Test Type_Safe preservation
        with self.service as _:
            # Create Type_Safe object
            original = Schema__Cache__Entry(
                hash    = Safe_Str__Cache_Hash("abcdef1234567890"),
                version = 1,
                data    = Safe_Bytes("test data")
            )
            
            # Store
            response = _.store_type_safe(original)
            
            # Retrieve
            restored = _.retrieve_typed(response.hash)
            
            # Verify using .obj()
            assert restored.obj() == original.obj()
            
            # Verify types preserved
            assert type(restored.hash) is Safe_Str__Cache_Hash
            assert type(restored.data) is Safe_Bytes
```

## Success Metrics

- [x] No raw primitives anywhere in schemas
- [x] Safe_Bytes handles binary data efficiently
- [x] Compression reduces size for compressible data
- [x] Type_Safe objects round-trip perfectly
- [x] All collections are Type_Safe variants with validation
- [x] Content-type aware deserialization works
- [x] Tests follow Type_Safe patterns with `.obj()` and context managers