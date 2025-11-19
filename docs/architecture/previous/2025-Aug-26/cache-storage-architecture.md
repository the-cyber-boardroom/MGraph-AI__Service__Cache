# Cache Storage Architecture - Technical Brief

## Overview

The MGraph-AI Service Cache implements a graph-based (G3 - Graphs of Graphs of Graphs) storage architecture designed for high-performance caching with support for versioning, multiple access patterns, and temporal organization. The system prioritizes ultra-fast reads while accepting slower write operations.

## Core Design Principles

1. **Single Source of Truth**: Data is stored once in the `data/` directory
2. **Graph-Based References**: All access patterns use lightweight JSON references pointing to actual data
3. **Fast Reads, Slower Writes**: Optimized for read-heavy workloads with 1-2 file reads for most operations
4. **Version Support**: Same content hash can have multiple cache IDs (versions)
5. **Memory-FS Integration**: Leverages Memory-FS patterns with `.config` and `.metadata` files
6. **Temporal Organization**: Data organized by timestamp for efficient time-based queries

## Directory Structure

```
cache/
├── data/                          # SINGLE SOURCE OF TRUTH - Actual data storage
│   ├── 2024/                     # Year
│   │   └── 12/                   # Month
│   │       └── 15/               # Day
│   │           └── 14/           # Hour
│   │               ├── cache-id-abc123.json           # Actual data
│   │               ├── cache-id-abc123.json.config    # Memory-FS config
│   │               └── cache-id-abc123.json.metadata  # Memory-FS metadata
│   └── 2025/
│       └── 01/
│           └── 10/
│               └── 11/
│                   ├── cache-id-def456.json
│                   ├── cache-id-def456.json.config
│                   └── cache-id-def456.json.metadata
│
├── refs/                          # REFERENCE GRAPHS - All pointers to data
│   ├── by-hash/                  # Hash-based references (sharded)
│   │   └── a7/                   # First 2 chars of hash
│   │       └── 3f/               # Next 2 chars of hash
│   │           ├── a73f2e4b.refs.json      # List of all versions
│   │           └── a73f2e4b.latest.json    # Latest with embedded metadata
│   │
│   ├── by-id/                    # ID-based references (sharded)
│   │   └── ab/                   # First 2 chars of cache-id
│   │       └── c1/               # Next 2 chars of cache-id
│   │           └── abc123.ref.json         # Single pointer to data
│   │
│   ├── by-type/                  # Type-based references
│   │   ├── llm/
│   │   │   ├── openai/
│   │   │   │   ├── gpt-4.refs.json         # All GPT-4 cache entries
│   │   │   │   └── gpt-4.latest.json       # Latest GPT-4 entry
│   │   │   └── anthropic/
│   │   │       ├── claude.refs.json
│   │   │       └── claude.latest.json
│   │   ├── http/
│   │   │   └── domains/
│   │   │       ├── github-com.refs.json
│   │   │       └── github-com.latest.json
│   │   └── filesystem/
│   │       ├── paths.refs.json
│   │       └── paths.latest.json
│   │
│   └── by-url/                   # URL-based references (for web content)
│       └── com/
│           └── github/
│               └── owasp-sbot/
│                   ├── osbot-utils.refs.json
│                   └── osbot-utils.latest.json
│
├── indexes/                       # MASTER INDEXES - Graph metadata
│   ├── master.index.json         # Master index of all cache entries
│   ├── temporal.index.json       # Temporal index for time-based queries
│   └── sharding.index.json       # Sharding configuration
│
└── graphs/                        # RESERVED - MGraph-AI graph structures
    └── relationships/
        └── cache-dependencies.mgraph
```

## File Schemas

### Data Files (in `data/` directory)

#### Primary Data File
```
data/2025/01/10/11/cache-id-def456.json
```
Contains the actual cached data (e.g., LLM response, HTTP response, file content)

#### Config File (.json.config)
```json
{
  "file_id": "def456",
  "file_paths": ["data/2025/01/10/11"],
  "file_type": {
    "name": "json",
    "content_type": "application/json; charset=utf-8",
    "file_extension": "json",
    "encoding": "utf-8",
    "serialization": "json"
  },
  "exists_strategy": "first"
}
```

#### Metadata File (.json.metadata)
```json
{
  "content__hash": "a73f2e4b...",
  "content__size": 15234,
  "timestamp": 1734357600000,
  "tags": ["production", "gpt-4", "cached"],
  "chain_hash": null,
  "previous_version_path": "data/2024/12/15/14/cache-id-xyz789.json"
}
```

### Reference Files (in `refs/` directory)

#### Latest File (.latest.json)
Contains embedded config and metadata for single-read operations:

```json
{
  "hash": "a73f2e4b...",
  "cache_id": "def456",
  "path": "data/2025/01/10/11/cache-id-def456.json",
  "timestamp": 1734357600000,
  
  "config": {
    "file_id": "def456",
    "file_paths": ["data/2025/01/10/11"],
    "file_type": {
      "name": "json",
      "content_type": "application/json; charset=utf-8",
      "file_extension": "json",
      "encoding": "utf-8",
      "serialization": "json"
    },
    "exists_strategy": "first"
  },
  
  "metadata": {
    "content__hash": "a73f2e4b...",
    "content__size": 15234,
    "timestamp": 1734357600000,
    "tags": ["production", "gpt-4"],
    "chain_hash": null,
    "previous_version_path": "data/2024/12/15/14/cache-id-xyz789.json"
  },
  
  "stats": {
    "total_versions": 3,
    "version_number": 3,
    "access_count": 42,
    "last_accessed": 1734360000000
  }
}
```

#### References File (.refs.json)
Lightweight list of all versions:

```json
{
  "hash": "a73f2e4b...",
  "total_versions": 3,
  "cache_ids": [
    {
      "id": "abc123",
      "timestamp": 1734350400000,
      "path": "data/2024/12/15/14/cache-id-abc123.json"
    },
    {
      "id": "xyz789",
      "timestamp": 1734354000000,
      "path": "data/2024/12/15/14/cache-id-xyz789.json"
    },
    {
      "id": "def456",
      "timestamp": 1734357600000,
      "path": "data/2025/01/10/11/cache-id-def456.json"
    }
  ]
}
```

#### Single Reference File (.ref.json)
Points to one specific cache entry:

```json
{
  "cache_id": "abc123",
  "path": "data/2024/12/15/14/cache-id-abc123.json",
  "hash": "a73f2e4b...",
  "timestamp": 1734350400000
}
```

## Key Concepts

### Cache ID vs Cache Hash

- **Cache ID**: Unique identifier for each cache entry (e.g., `abc123`, `def456`)
- **Cache Hash**: Content hash that may be shared by multiple cache IDs (e.g., `a73f2e4b`)

This separation allows storing multiple versions of the same content (same hash, different IDs), essential for:
- Web page monitoring over time
- Multiple LLM responses to the same prompt
- Historical tracking of changing content

### Sharding Strategy

References are sharded to prevent directory size issues:
- Default: 2-level sharding with 2 characters per level
- Example: Hash `a73f2e4b` → Path `a7/3f/a73f2e4b`
- Configurable depth and character count per deployment

### Temporal Organization

Data files are organized by timestamp of creation:
- Pattern: `YYYY/MM/DD/HH/cache-id-{id}.{ext}`
- Enables efficient time-range queries
- Natural partitioning for archival/cleanup

## Operation Workflows

### READ Operations

#### Get Latest by Hash (1 file read for metadata, 2 for data)
```
1. READ: refs/by-hash/a7/3f/a73f2e4b.latest.json
   → Returns: config, metadata, stats, path
2. READ: data/2025/01/10/11/cache-id-def456.json (if data needed)
   → Returns: actual cached data
```

#### Get Specific by ID (1-2 file reads)
```
1. READ: refs/by-id/de/f4/def456.ref.json
   → Returns: path to data
2. READ: data/2025/01/10/11/cache-id-def456.json
   → Returns: actual cached data
```

#### Get All Versions (2-N file reads)
```
1. READ: refs/by-hash/a7/3f/a73f2e4b.refs.json
   → Returns: list of all cache IDs with this hash
2-N. READ: Individual data files as needed
```

#### Get Stats Only (1 file read)
```
1. READ: refs/by-hash/a7/3f/a73f2e4b.latest.json
   → Extract stats/metadata without reading actual data
```

### WRITE Operations

#### Store New Cache Entry
```
1. Generate cache-id → ghi789
2. Calculate content hash → a73f2e4b
3. WRITE: data/2025/01/10/12/cache-id-ghi789.json
4. WRITE: data/2025/01/10/12/cache-id-ghi789.json.config
5. WRITE: data/2025/01/10/12/cache-id-ghi789.json.metadata
6. READ: refs/by-hash/a7/3f/a73f2e4b.refs.json (check existing)
7. UPDATE: refs/by-hash/a7/3f/a73f2e4b.refs.json (append new ID)
8. WRITE: refs/by-hash/a7/3f/a73f2e4b.latest.json (with embedded metadata)
9. WRITE: refs/by-id/gh/i7/ghi789.ref.json
10. UPDATE: indexes/master.index.json (update statistics)
```

## Performance Characteristics

### Read Performance
- **Latest by hash**: 1 file read (metadata only) or 2 file reads (with data)
- **Specific by ID**: 1-2 file reads
- **Stats/Analytics**: 1 file read per entry
- **No computation**: Direct file lookups, no sorting or filtering needed

### Write Performance
- **New entry**: 8-10 file operations
- **Atomic operations**: Each file write is atomic
- **Eventually consistent**: References updated after data written

### Storage Efficiency
- **Single data copy**: Data stored once, referenced multiple times
- **Lightweight references**: JSON pointers typically < 1KB
- **Embedded metadata**: Slight duplication in `.latest.json` files for performance

## Use Cases

### 1. LLM Response Caching
- Multiple responses to same prompt (same hash, different IDs)
- Fast retrieval of latest response
- Historical comparison of model outputs

### 2. Web Content Monitoring
- Track changes to web pages over time
- Each capture gets unique ID, may share hash if unchanged
- Temporal queries for historical analysis

### 3. HTTP Request/Response Caching
- Cache API responses by URL and method
- Support for conditional requests using metadata
- Bandwidth optimization via size checking

### 4. File System Emulation
- Virtual file paths mapped to cache entries
- Directory listing via reference traversal
- Version control for virtual files

## Storage Backend Support

The architecture supports multiple storage backends through the Storage_FS abstraction:

- **Memory**: In-memory storage for testing/development
- **Local File System**: Direct file system storage
- **S3/Boto3**: AWS S3 or S3-compatible storage (MinIO, LocalStack)
- **SQLite**: Embedded database storage
- **Zip**: Compressed archive storage

## Configuration Options

```python
{
  "storage_backend": "s3",           # s3, memory, filesystem, sqlite
  "sharding": {
    "enabled": true,
    "depth": 2,
    "chars_per_level": 2
  },
  "temporal": {
    "granularity": "hour",           # year, month, day, hour, minute
    "archive_after_days": 30
  },
  "versioning": {
    "max_versions_per_hash": null,   # null = unlimited
    "cleanup_strategy": "keep_latest"
  }
}
```

## Future Enhancements

1. **Compression**: Transparent compression for large cache entries
2. **Encryption**: At-rest encryption for sensitive data
3. **Replication**: Multi-region replication for global cache
4. **Indexing**: Advanced indexing for complex queries
5. **Garbage Collection**: Automated cleanup of old versions
6. **Access Control**: Per-entry or per-hash access controls

## Implementation Notes

- All paths use forward slashes (`/`) regardless of OS
- Timestamps are Unix milliseconds (UTC)
- Hashes are SHA-256 truncated to configurable length
- Cache IDs use Safe_Id format (alphanumeric + hyphen)
- JSON files use UTF-8 encoding
- File operations should be atomic where possible

## Dependencies

- **Memory-FS**: File system abstraction layer
- **OSBot-Utils**: Type safety and utilities
- **Boto3-FS**: S3 storage backend
- **MGraph-AI**: Graph analysis capabilities
