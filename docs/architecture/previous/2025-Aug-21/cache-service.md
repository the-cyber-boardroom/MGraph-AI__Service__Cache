# MGraph-AI Cache Service - Complete Architecture Specification

## Executive Summary

The MGraph-AI Cache Service is a generic, high-performance caching system built on the G3 (Graphs of Graphs of Graphs) model. It provides content-agnostic storage with a focus on ultra-fast reads (1-2 file operations) while accepting slower writes (8-10 file operations). The service stores data once and creates multiple lightweight reference graphs for different access patterns.

## Table of Contents

1. [Core Architecture Principles](#core-architecture-principles)
2. [Storage Architecture](#storage-architecture)
3. [API Specification](#api-specification)
4. [Data Models and Schemas](#data-models-and-schemas)
5. [Performance Specifications](#performance-specifications)
6. [Implementation Requirements](#implementation-requirements)
7. [Integration Specifications](#integration-specifications)

---

## 1. Core Architecture Principles

### 1.1 G3 Model (Graphs of Graphs of Graphs)

The cache implements a graph-based storage model where:
- **Data is stored once**: Single source of truth in temporal directories
- **Multiple reference graphs**: Different access patterns via JSON pointers
- **Graph relationships**: References can point to other references
- **No data duplication**: Only lightweight JSON files for navigation

### 1.2 Design Principles

1. **Generic and Universal**
   - Content-agnostic: Works with any data type
   - No business logic: Pure storage and retrieval
   - Metadata transparency: Stores but doesn't interpret metadata

2. **Hash-Centric Architecture**
   - Content addressing: SHA-256 hash as primary identifier
   - Version support: Multiple cache IDs per hash
   - Immutable storage: Data never modified after writing

3. **Performance Optimization**
   - Read-optimized: 1-2 file reads for common operations
   - Write-tolerant: 8-10 file operations acceptable
   - Metadata embedding: `.latest.json` contains config and metadata

4. **Storage Flexibility**
   - Backend agnostic: Memory-FS abstraction layer
   - Multi-cloud ready: S3, LocalStack, Azure support
   - Development friendly: In-memory and SQLite options

---

## 2. Storage Architecture

### 2.1 Directory Structure

```
cache/
├── data/                          # Single source of truth
│   └── YYYY/MM/DD/HH/            # Temporal organization
│       └── cache-id-{id}.{ext}   # Actual data files
│           ├── .config            # Memory-FS config
│           └── .metadata          # Memory-FS metadata
│
├── refs/                          # Reference graphs
│   ├── by-hash/                  # Hash-based access
│   │   └── {h1}/{h2}/            # Sharded directories
│   │       ├── {hash}.refs.json  # All versions list
│   │       └── {hash}.latest.json # Latest with embedded metadata
│   ├── by-id/                    # Direct ID access
│   ├── by-type/                  # Type-based grouping
│   └── by-url/                   # URL-based organization
│
├── indexes/                       # Master indexes
└── graphs/                        # MGraph-AI relationships
```

### 2.2 File Specifications

#### 2.2.1 Data Files

**Primary Data File**: `cache-id-{id}.json`
- Contains actual cached data
- Format depends on content type
- Immutable after creation

**Config File**: `cache-id-{id}.json.config`
```json
{
  "file_id": "string",
  "file_paths": ["array of paths"],
  "file_type": {
    "name": "string",
    "content_type": "MIME type",
    "file_extension": "string",
    "encoding": "utf-8|binary|...",
    "serialization": "json|binary|string|..."
  },
  "exists_strategy": "first|all|any"
}
```

**Metadata File**: `cache-id-{id}.json.metadata`
```json
{
  "content__hash": "SHA-256 hash",
  "content__size": integer,
  "timestamp": unix_milliseconds,
  "tags": ["array", "of", "tags"],
  "chain_hash": "optional parent hash",
  "previous_version_path": "optional path"
}
```

#### 2.2.2 Reference Files

**Latest Reference**: `{hash}.latest.json`
```json
{
  "hash": "full hash",
  "cache_id": "current cache ID",
  "path": "full path to data",
  "timestamp": unix_milliseconds,
  "config": {embedded config object},
  "metadata": {embedded metadata object},
  "stats": {
    "total_versions": integer,
    "version_number": integer,
    "access_count": integer,
    "last_accessed": unix_milliseconds
  }
}
```

**References List**: `{hash}.refs.json`
```json
{
  "hash": "full hash",
  "total_versions": integer,
  "cache_ids": [
    {
      "id": "cache ID",
      "timestamp": unix_milliseconds,
      "path": "full path to data"
    }
  ]
}
```

### 2.3 Sharding Strategy

- **Default Configuration**: 2-level, 2-character sharding
- **Example**: Hash `a73f2e4b...` → Path `a7/3f/a73f2e4b...`
- **Configurable**: Depth and characters per level adjustable
- **Benefits**: Prevents directory size limitations

### 2.4 Version Management

- **Multiple IDs per Hash**: Same content, different cache entries
- **Sequential Numbering**: Versions numbered 1, 2, 3...
- **Latest Access**: Direct pointer in `.latest.json`
- **History Preservation**: All versions retained

---

## 3. API Specification

### 3.1 Route Structure

The API follows RESTful principles with resources before actions:

```
/cache
├── /info                    # Service information
├── /store                   # Storage operations
├── /retrieve                # Retrieval operations
├── /exists                  # Existence checking
├── /search                  # Search and discovery
├── /refs                    # Reference navigation
├── /stats                   # Analytics
├── /batch                   # Batch operations
└── /admin                   # Administration
```

### 3.2 Core Endpoints

#### Storage Endpoints

**POST /cache/store**
- Store data with auto-generated hash
- Request: `{data, content_type, metadata, tags}`
- Response: `{cache_id, hash, version, path, size}`

**PUT /cache/store/{hash}**
- Store data with specific hash (idempotent)
- Request: `{data, content_type, metadata, tags}`
- Response: Same as POST or existing entry

#### Retrieval Endpoints

**GET /cache/retrieve/hash/{hash}**
- Get latest version with full details
- Response: `{cache_id, hash, data, metadata, config, stats}`

**GET /cache/retrieve/hash/{hash}/metadata**
- Get metadata only (1 file read)
- Response: `{metadata, config, stats}`

**GET /cache/retrieve/hash/{hash}/data**
- Get raw data only
- Response: Binary/text data with appropriate Content-Type

**GET /cache/retrieve/hash/{hash}/versions**
- List all versions
- Response: `{hash, versions: [{cache_id, timestamp, version}]}`

**GET /cache/retrieve/hash/{hash}/v/{version}**
- Get specific version
- Response: Same as hash retrieval

### 3.3 Request/Response Patterns

#### Generic Store Request
```json
{
  "data": "base64_or_string",
  "content_type": "application/json",
  "metadata": {
    "source": "llm|http|filesystem",
    "custom_field": "any_value"
  },
  "tags": ["tag1", "tag2"]
}
```

#### Generic Retrieve Response
```json
{
  "cache_id": "abc123",
  "hash": "a73f2e4b...",
  "data": "actual_data",
  "metadata": {
    "content__hash": "a73f2e4b...",
    "content__size": 15234,
    "timestamp": 1734357600000,
    "tags": ["tag1", "tag2"]
  },
  "config": {
    "file_type": {...},
    "exists_strategy": "first"
  },
  "stats": {
    "version": 1,
    "total_versions": 3,
    "access_count": 42
  }
}
```

---

## 4. Data Models and Schemas

### 4.1 Core Types

#### Cache ID
- Type: `Safe_Id`
- Format: Alphanumeric + hyphen
- Example: `cache-id-abc123`
- Generation: Auto-generated with prefix

#### Content Hash
- Type: `Safe_Str__Hash`
- Algorithm: SHA-256
- Truncation: Configurable (default 10 chars for refs)
- Example: `a73f2e4b8c`

#### Timestamp
- Type: `Timestamp_Now`
- Format: Unix milliseconds (UTC)
- Example: `1734357600000`

### 4.2 Metadata Schema

```typescript
interface CacheMetadata {
  // Required fields (system-managed)
  content__hash: string;
  content__size: number;
  timestamp: number;
  
  // Optional fields (user-provided)
  tags?: string[];
  source?: string;
  
  // Domain-specific (transparent to cache)
  [key: string]: any;
}
```

### 4.3 Version Information

```typescript
interface VersionInfo {
  version_number: number;
  cache_id: string;
  timestamp: number;
  path: string;
  parent_version?: number;
  change_summary?: string;
}
```

---

## 5. Performance Specifications

### 5.1 Read Performance Targets

| Operation | File Reads | Target Latency |
|-----------|------------|----------------|
| Latest by hash | 1-2 | < 50ms |
| Metadata only | 1 | < 20ms |
| Specific version | 2 | < 50ms |
| Check existence | 1 | < 20ms |
| List versions | 1 | < 30ms |

### 5.2 Write Performance Expectations

| Operation | File Operations | Acceptable Latency |
|-----------|-----------------|-------------------|
| Store new | 8-10 | < 200ms |
| Update refs | 3-4 | < 100ms |
| Batch store | 6-8 per item | < 500ms |

### 5.3 Optimization Strategies

1. **`.latest.json` Embedding**
   - Contains config and metadata
   - Eliminates additional reads
   - Single file for common operations

2. **Sharded Storage**
   - Prevents directory bottlenecks
   - Enables parallel access
   - Supports millions of entries

3. **Temporal Organization**
   - Natural partitioning
   - Efficient cleanup
   - Time-based queries

---

## 6. Implementation Requirements

### 6.1 Technology Stack

#### Required Components
- **FastAPI**: REST API framework
- **Memory-FS**: File system abstraction
- **OSBot-Utils**: Type safety and utilities
- **Boto3-FS**: S3 storage backend

#### Storage Backends
- **Development**: Memory, LocalStack
- **Testing**: SQLite, Memory
- **Production**: S3, Azure Blob
- **Future**: Compressed archives

### 6.2 Memory-FS Integration

```python
class Storage_FS(Type_Safe):
    def file__bytes(self, path: Safe_Str__File__Path) -> bytes
    def file__save(self, path: Safe_Str__File__Path, data: bytes) -> bool
    def file__exists(self, path: Safe_Str__File__Path) -> bool
    def file__delete(self, path: Safe_Str__File__Path) -> bool
    def file__json(self, path: Safe_Str__File__Path) -> dict
```

### 6.3 Configuration

```yaml
cache_config:
  storage:
    backend: "s3"  # s3|memory|sqlite|filesystem
    
  sharding:
    enabled: true
    depth: 2
    chars_per_level: 2
    
  temporal:
    granularity: "hour"  # year|month|day|hour|minute
    archive_after_days: 30
    
  versioning:
    max_versions_per_hash: null  # null = unlimited
    cleanup_strategy: "keep_latest"
    
  performance:
    max_file_size_mb: 100
    batch_size_limit: 100
    cache_ttl_days: 90
```

---

## 7. Integration Specifications

### 7.1 Client Library Interface

```python
class CacheClient:
    def store(self, data: Any, 
             metadata: Dict = None, 
             tags: List[str] = None) -> CacheResponse
    
    def retrieve(self, hash: str = None, 
                cache_id: str = None,
                version: int = None) -> CacheEntry
    
    def exists(self, hash: str) -> bool
    
    def search(self, filters: SearchFilters) -> List[CacheEntry]
    
    def get_metadata(self, hash: str) -> Metadata
```

### 7.2 Use Case Integrations

#### LLM Service Integration
```python
# Store LLM response
cache.store(
    data=llm_response,
    metadata={
        "source": "llm",
        "model": "gpt-4",
        "provider": "openai",
        "prompt_hash": "xyz789"
    }
)
```

#### Web Monitoring Integration
```python
# Store web capture with versioning
cache.store(
    data=html_content,
    metadata={
        "source": "web",
        "url": "https://example.com",
        "capture_time": timestamp,
        "headers": response_headers
    }
)
```

### 7.3 Authentication and Security

1. **API Key Authentication**
   - Required for all endpoints
   - Per-client rate limiting
   - Usage tracking

2. **Access Control**
   - Read/write permissions
   - Admin operations restricted
   - Audit logging

3. **Data Security**
   - HTTPS only
   - Optional encryption at rest
   - Hash verification on retrieval