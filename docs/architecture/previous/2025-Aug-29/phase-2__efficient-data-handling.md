# Phase 2: Efficient Data Handling

## Goal
Remove base64 encoding requirement and add support for efficient binary data and compression.

## Problem with Current Implementation

### Current Data Flow (INEFFICIENT):
```
Binary Data → Base64 Encode → JSON String → Network → Base64 Decode → Store
(100KB image → 133KB base64 → Network overhead → Decode → Store)
```

### Target Data Flow (EFFICIENT):
```
Binary Data → Optional Compress → Direct Binary → Network → Store
(100KB image → 30KB gzipped → Network → Store)
```

## Changes to Schema Classes

### 1. Replace `Schema__Cache__Store__Request`

**CURRENT (BROKEN):**
```python
class Schema__Cache__Store__Request(Type_Safe):
    data: str  # Forces base64 encoding!
    content_type: Safe_Str__Http__Content_Type
```

**NEW SCHEMAS NEEDED:**
```python
class Schema__Cache__Store__Binary__Request(Type_Safe):
    """For binary data - no encoding needed"""
    data_bytes: bytes  # Direct binary
    content_type: Safe_Str__Http__Content_Type
    content_encoding: Optional[Literal["gzip", "deflate", "br"]]
    metadata: Dict[Safe_Id, Any]
    
class Schema__Cache__Store__Json__Request(Type_Safe):
    """For JSON/Type_Safe objects"""
    data_json: dict  # Direct JSON
    metadata: Dict[Safe_Id, Any]
    
class Schema__Cache__Store__Text__Request(Type_Safe):
    """For text data"""
    data_text: str  # UTF-8 text
    content_type: Literal["text/plain", "text/html", "text/markdown"]
    metadata: Dict[Safe_Id, Any]
```

### 2. Update Response Schema

```python
class Schema__Cache__Store__Response(Type_Safe):
    hash: Safe_Str__Hash  # Primary identifier
    version: int  # Version number
    size_original: int  # Original size
    size_stored: int  # After compression
    compression_ratio: float  # For metrics
    path: Safe_Str__File__Path
```

## Changes to `Cache__Service`

### 1. Multi-Format Store Methods

```python
class Cache__Service(Type_Safe):
    
    def store_binary(self, data: bytes, 
                    content_type: str,
                    namespace: Safe_Id = None,
                    compress: bool = True) -> Schema__Cache__Store__Response:
        """Store binary data directly"""
        if compress and len(data) > 1024:  # Compress if > 1KB
            data_compressed = gzip.compress(data)
            if len(data_compressed) < len(data) * 0.9:  # Only if 10% savings
                return self._store_raw(data_compressed, content_type, "gzip", namespace)
        return self._store_raw(data, content_type, None, namespace)
    
    def store_json(self, data: Union[dict, Type_Safe], 
                   namespace: Safe_Id = None) -> Schema__Cache__Store__Response:
        """Store JSON or Type_Safe objects"""
        if isinstance(data, Type_Safe):
            json_data = data.json()
        else:
            json_data = data
        json_bytes = json.dumps(json_data, sort_keys=True).encode('utf-8')
        return self.store_binary(json_bytes, "application/json", namespace)
    
    def store_text(self, text: str,
                   content_type: str = "text/plain",
                   namespace: Safe_Id = None) -> Schema__Cache__Store__Response:
        """Store text data"""
        return self.store_binary(text.encode('utf-8'), content_type, namespace)
```

### 2. Smart Hash Generation

```python
def generate_hash(self, data: Any) -> Safe_Str__Hash:
    """Generate hash from any data type"""
    if isinstance(data, bytes):
        hash_input = data
    elif isinstance(data, str):
        hash_input = data.encode('utf-8')
    elif isinstance(data, Type_Safe):
        hash_input = json.dumps(data.json(), sort_keys=True).encode('utf-8')
    elif isinstance(data, dict):
        hash_input = json.dumps(data, sort_keys=True).encode('utf-8')
    else:
        hash_input = str(data).encode('utf-8')
    
    return Safe_Str__Hash(hashlib.sha256(hash_input).hexdigest()[:10])
```

### 3. Content-Type Aware Retrieval

```python
def retrieve_typed(self, hash: Safe_Str__Hash, 
                  namespace: Safe_Id = None) -> Union[bytes, dict, str, Type_Safe]:
    """Retrieve and deserialize based on content type"""
    result = self._retrieve_by_hash(hash, namespace)
    content_type = result.get('content_type', '')
    content_encoding = result.get('content_encoding')
    data_bytes = result['data']
    
    # Decompress if needed
    if content_encoding == 'gzip':
        data_bytes = gzip.decompress(data_bytes)
    
    # Deserialize based on type
    if 'json' in content_type:
        return json.loads(data_bytes.decode('utf-8'))
    elif 'text' in content_type:
        return data_bytes.decode('utf-8')
    else:
        return data_bytes  # Return raw bytes
```

## Changes to Routes

### 1. Update `Routes__Cache.store()`

**CURRENT:**
```python
def store(self, request: Schema__Cache__Store__Request, namespace: Safe_Id = None):
    return self.cache_service.store(request, namespace)
```

**NEW:**
```python
from fastapi import Request, Response

async def store_raw(self, request: Request, 
                   namespace: Safe_Id = None) -> Schema__Cache__Store__Response:
    """Accept raw binary data"""
    content_type = request.headers.get('content-type', 'application/octet-stream')
    content_encoding = request.headers.get('content-encoding')
    
    # Read raw body
    body_bytes = await request.body()
    
    # Decompress if client compressed
    if content_encoding == 'gzip':
        body_bytes = gzip.decompress(body_bytes)
    
    return self.cache_service.store_binary(body_bytes, content_type, namespace)
```

### 2. Add Typed Endpoints

```python
def setup_routes(self):
    # Binary endpoint
    self.add_route_post(self.store_raw, path="/store/binary")
    
    # JSON endpoint  
    self.add_route_post(self.store_json, path="/store/json")
    
    # Text endpoint
    self.add_route_post(self.store_text, path="/store/text")
    
    # Type_Safe endpoint
    self.add_route_post(self.store_typesafe, path="/store/typesafe")
```

## Changes to Storage Layer

### 1. Update `Storage_FS__S3`

Add compression metadata:
```python
def file__save_with_metadata(self, path: Safe_Str__File__Path,
                            data: bytes,
                            metadata: dict) -> bool:
    """Save with S3 metadata"""
    return self.s3.file_create_from_bytes(
        file_bytes=data,
        bucket=self.s3_bucket,
        key=self._get_s3_key(path),
        metadata=metadata  # S3 metadata for content-type, encoding
    )
```

## High-Level API Wrapper

### New Simple Cache Client

```python
class Cache__Client(Type_Safe):
    """Simple high-level cache interface"""
    
    def put(self, key: str, value: Any, ttl: int = None) -> bool:
        """Simple put - auto-detects type"""
        # Generate hash from key
        cache_key_hash = self._hash_key(key)
        
        # Store with auto-detection
        if isinstance(value, bytes):
            return self.service.store_binary(value)
        elif isinstance(value, (dict, Type_Safe)):
            return self.service.store_json(value)
        else:
            return self.service.store_text(str(value))
    
    def get(self, key: str, type_class: Type = None) -> Any:
        """Simple get - auto-deserializes"""
        cache_key_hash = self._hash_key(key)
        result = self.service.retrieve_typed(cache_key_hash)
        
        if type_class and isinstance(result, dict):
            return type_class.from_json(result)
        return result
```

## Implementation Steps

### Step 1: Add Binary Support (Day 1 Morning)
1. Create new request schemas for different data types
2. Add `store_binary()` method
3. Remove base64 encoding requirement
4. Test with image/zip files

### Step 2: Add Compression (Day 1 Afternoon)
1. Add gzip compression for data > 1KB
2. Add compression metrics to response
3. Handle Content-Encoding headers
4. Test compression ratios

### Step 3: Type-Aware Methods (Day 2 Morning)
1. Add `store_json()` for JSON/Type_Safe
2. Add `store_text()` for strings
3. Add smart type detection
4. Update retrieval to deserialize correctly

### Step 4: Simple API (Day 2 Afternoon)
1. Create `Cache__Client` wrapper
2. Implement `put(key, value)` interface
3. Implement `get(key, type)` interface
4. Add TTL support (future feature)

## Testing Requirements

### Performance Tests:
```python
def test_binary_no_base64_overhead():
    """100KB binary should store as ~100KB, not 133KB"""
    data = os.urandom(100 * 1024)
    response = cache.store_binary(data)
    assert response.size_stored < 105 * 1024  # Max 5% overhead

def test_compression_saves_space():
    """Repetitive data should compress well"""
    data = b"x" * 100_000
    response = cache.store_binary(data, compress=True)
    assert response.compression_ratio < 0.1  # Should compress to <10%
```

### Type Tests:
```python
def test_type_safe_roundtrip():
    """Type_Safe objects preserve types"""
    user = Schema__User(name="Alice", age=30)
    cache.put("user:1", user)
    restored = cache.get("user:1", Schema__User)
    assert type(restored) is Schema__User
    assert restored.name == "Alice"
```

## Success Metrics

- [ ] Binary data stores without base64 overhead
- [ ] Large text/JSON compresses automatically
- [ ] Type_Safe objects round-trip correctly
- [ ] Simple `put()`/`get()` API works
- [ ] 50% bandwidth reduction for compressible data