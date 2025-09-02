# MGraph-AI Cache Service - LLM Brief

## Service Overview
The MGraph-AI Cache Service is a production-ready caching system deployed at `https://cache.dev.mgraph.ai/`. It provides intelligent content-addressable storage with multiple caching strategies, backed by AWS S3.

## Core Capabilities

### Storage Strategies
The service supports four distinct storage strategies:
- **direct**: Simple hash-based storage
- **temporal**: Time-organized storage with timestamps
- **temporal_latest**: Temporal storage that maintains a "latest" pointer
- **temporal_versioned**: Full versioning with temporal organization and version history

### Content Types
- **String data**: Plain text content via `/cache/store/string/{strategy}/{namespace}`
- **JSON data**: Structured JSON via `/cache/store/json/{strategy}/{namespace}`
- **Binary data**: Raw bytes, including compressed content via `/cache/store/binary/{strategy}/{namespace}`

### Key Features
- **Content-addressable**: Uses configurable hash algorithms (MD5, SHA256, SHA384)
- **Namespace isolation**: Organize cache entries by namespace
- **Automatic sharding**: Hash-based path sharding for optimal S3 performance
- **Compression support**: Handles gzip-compressed content transparently
- **Dual indexing**: Access by either hash or unique cache ID
- **TTL support**: Configurable time-to-live for cache entries

## API Endpoints

### Storage Operations
```
POST /cache/store/string/{strategy}/{namespace}
POST /cache/store/json/{strategy}/{namespace}
POST /cache/store/binary/{strategy}/{namespace}
```
**Note**: For string endpoint, use `Content-Type: text/plain` header

### Retrieval Operations
```
GET /cache/retrieve/by-hash/{cache_hash}/{namespace}
GET /cache/retrieve/by-id/{cache_id}/{namespace}
```

### Management Operations
```
GET /cache/exists/{cache_hash}/{namespace}
GET /cache/namespaces
GET /cache/stats/namespaces/{namespace}
DELETE /cache/delete/by-id/{cache_id}/{namespace}
```

### Authentication (if required)
The service requires API key authentication. Include the API key in headers:
```python
headers = {
    "your-api-key-name": "your-api-key-value",
    "Content-Type": "application/json"  # or appropriate content type
}
```

## Usage Examples

### Example: Caching a ZIP file from URL

```python
import requests
import hashlib

# Setup authentication if required
headers = {
    # "your-api-key-name": "your-api-key-value"
    "Content-Type": "application/octet-stream"
}

# 1. Download the ZIP file
url = "https://example.com/data.zip"
response = requests.get(url)
zip_content = response.content

# 2. Store in cache (hash will be auto-generated from content)
cache_response = requests.post(
    "https://cache.dev.mgraph.ai/cache/store/binary/temporal_latest/my-zips",
    data=zip_content,
    headers=headers
)

# Response includes:
# {
#   "cache_id": "unique-guid",  # Unique identifier (GUID format)
#   "hash": "content-hash",      # 16-character hash by default
#   "paths": {                   # Storage paths organized by type
#       "data": [...],
#       "by_hash": [...],
#       "by_id": [...]
#   },
#   "size": 12345
# }

cache_id = cache_response.json()['cache_id']
cache_hash = cache_response.json()['hash']

# 3. Retrieve by hash (gets latest version)
retrieved = requests.get(
    f"https://cache.dev.mgraph.ai/cache/retrieve/by-hash/{cache_hash}/my-zips",
    headers=headers
)

# Or retrieve by specific cache ID
retrieved = requests.get(
    f"https://cache.dev.mgraph.ai/cache/retrieve/by-id/{cache_id}/my-zips",
    headers=headers
)

# Retrieved response format:
# {
#   "data": <your-content>,
#   "metadata": {
#       "cache_hash": "...",
#       "cache_id": "...",
#       "stored_at": "...",
#       "strategy": "temporal_latest",
#       "namespace": "my-zips"
#   }
# }
```

### Example: Caching JSON with Exclusions

```python
# Store JSON data
data = {
    "result": "processed",
    "timestamp": "2025-01-01",  # This might change
    "data": {"key": "value"}
}

response = requests.post(
    "https://cache.dev.mgraph.ai/cache/store/json/temporal_versioned/results",
    json=data
)
```

## Architecture Details

### Storage Backend
- **S3-backed**: All data stored in AWS S3 buckets
- **Memory-FS abstraction**: Uses memory-fs library for file system operations
- **Path sharding**: Automatic 2-level sharding (e.g., `ab/cd/abcdef...`) for optimal S3 performance

### Hash System
- **Configurable algorithms**: MD5, SHA256 (default), SHA384
- **Variable length**: 10-96 character hashes (default: 16)
- **Content-based**: Hashes derived from actual content, not metadata

### Reference System
- **Dual indexing**: 
  - Hash → Cache IDs mapping (supports multiple versions)
  - Cache ID → Hash mapping (direct lookup)
- **Version tracking**: Maintains version count and latest pointer

## Best Practices

### When to Use Each Strategy

**Use `direct` when:**
- You only need the latest version
- No temporal organization needed
- Simplest use case

**Use `temporal` when:**
- You want time-based organization
- Don't need quick access to latest version
- Want to preserve all historical data

**Use `temporal_latest` when:**
- You need both temporal organization AND quick latest access
- Most common for API response caching

**Use `temporal_versioned` when:**
- Full version history is important
- Need to track all changes over time
- Audit trail requirements

### Namespace Guidelines
- Use descriptive namespaces (e.g., `github-repos`, `api-responses`, `processed-files`)
- Namespaces provide isolation and can have different TTL settings
- Default namespace is "default" if not specified

### Performance Considerations
- Binary endpoint handles compression transparently
- Hash sharding ensures even distribution in S3
- Batch operations should respect S3 rate limits
- Consider using `temporal_latest` for frequently accessed, updating content

## Integration Tips

### For URL-based Caching
```python
# Use URL as cache key basis
def cache_url_content(url, namespace="url-cache"):
    # Generate deterministic hash from URL
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    
    # Check if already cached
    exists_response = requests.get(
        f"https://cache.dev.mgraph.ai/cache/exists/{url_hash}/{namespace}"
    )
    
    if exists_response.json()["exists"]:
        # Retrieve from cache
        return requests.get(
            f"https://cache.dev.mgraph.ai/cache/retrieve/by-hash/{url_hash}/{namespace}"
        )
    
    # Download and cache
    content = requests.get(url).content
    cache_response = requests.post(
        f"https://cache.dev.mgraph.ai/cache/store/binary/temporal_latest/{namespace}",
        data=content
    )
    
    return {"data": content, "metadata": cache_response.json()}
```

### For API Response Caching
```python
# Cache API responses with request parameters as key
def cache_api_response(endpoint, params, response_data):
    # Create cache key from endpoint + params
    cache_key = {
        "endpoint": endpoint,
        "params": params
    }
    
    # Store with temporal_versioned for history
    return requests.post(
        "https://cache.dev.mgraph.ai/cache/store/json/temporal_versioned/api-responses",
        json={
            "request": cache_key,
            "response": response_data,
            "cached_at": datetime.now().isoformat()
        }
    )
```

## Service Information

### Health & Status
- Health check: `GET /info/health`
- Service status: `GET /info/status`
- Version info: `GET /info/versions`

### Current Version
- Service: v0.5.15
- API: OpenAPI 3.1.0
- Environment: AWS Lambda (when deployed)

### Dependencies
- osbot-fast-api-serverless
- memory-fs
- AWS S3 for storage backend

## Common Use Cases

1. **API Response Caching**: Cache expensive API calls with temporal_latest
2. **File Download Cache**: Store downloaded files to avoid re-fetching
3. **Processing Results**: Cache computation results with versioning
4. **Build Artifacts**: Store build outputs with temporal organization
5. **Data Pipeline Checkpoints**: Cache intermediate processing stages
6. **Content Deduplication**: Use hash-based storage to avoid duplicates

## Important Response Formats

### Successful Retrieval Response
When retrieving cached content, the response format is:
```json
{
  "data": <actual-content>,
  "metadata": {
    "cache_hash": "16-char-hash",
    "cache_id": "uuid-format-id",
    "stored_at": "timestamp",
    "strategy": "storage-strategy",
    "namespace": "namespace-name",
    "content_encoding": "gzip"  // if compressed
  }
}
```

### Not Found Response
When content is not found:
```json
{
  "status": "not_found",
  "message": "Cache entry not found"
}
```

### Delete Response
```json
{
  "status": "success",  // or "partial" if some deletions failed
  "cache_id": "uuid",
  "deleted_count": 5,
  "failed_count": 0,
  "deleted_paths": [...],
  "failed_paths": []
}
```

## Testing & Validation

### Key Behaviors from QA Tests
- **Cache IDs**: Always unique GUIDs per storage operation, even for identical content
- **Hash values**: Identical content produces same hash (16 chars by default), regardless of namespace
- **Namespace isolation**: Complete - cannot retrieve content across namespaces
- **Concurrent operations**: Fully supported - service handles multiple simultaneous operations
- **Cleanup**: DELETE operations remove all associated files (data, references, indexes)

### Content-Type Headers
- **String data**: Use `Content-Type: text/plain`
- **JSON data**: Use `Content-Type: application/json`
- **Binary data**: Use `Content-Type: application/octet-stream` or specific type

### Expected Response Times
- Store operations: Sub-second for small files
- Retrieval: Sub-second when cached
- Delete: May take longer for versioned content with multiple files

---

*This brief is designed for LLM consumption when building services that integrate with the MGraph-AI Cache Service.*