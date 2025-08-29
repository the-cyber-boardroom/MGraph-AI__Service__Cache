# Cache Service API Routes - Architecture Document

## Overview

The MGraph-AI Cache Service provides a generic, high-performance caching API that focuses exclusively on storing and retrieving cached data. The service is content-agnostic, treating all cached items as data with associated metadata, regardless of their source (LLM responses, HTTP responses, files, etc.).

## Core Design Principles

### 1. Generic and Universal
- All cache operations work with hash/id pairs, regardless of content type
- No business logic or domain-specific operations
- The service stores metadata but doesn't interpret it

### 2. Hash-Centric Architecture
- Content hash is the primary identifier for all cached data
- Multiple cache IDs can exist for the same hash (versioning)
- Hash-based sharding for scalability

### 3. RESTful Design
- Resources (hash, id) come before actions in URL paths
- Standard HTTP methods (GET, POST, PUT, DELETE, HEAD)
- Path parameters for required identifiers, query parameters for optional filters

### 4. Performance Optimized
- Separate endpoints for metadata vs full data retrieval
- Batch operations to reduce round trips
- Latest version accessible without traversing history

### 5. Storage Agnostic
- Works with any storage backend (S3, filesystem, memory, SQLite)
- Consistent API regardless of storage implementation

## API Route Structure

```
/cache
├── /info                                # Service information
│   ├── GET  /health                    # Health check - returns service health status
│   ├── GET  /status                    # Service status - operational state and environment
│   ├── GET  /server                    # Server information - host, version, uptime
│   └── GET  /versions                  # Component versions - all dependencies
│
├── /store                               # Data storage operations
│   ├── POST /                          # Store data with auto-generated hash
│   └── PUT  /{hash}                    # Store data with specific hash (idempotent)
│
├── /retrieve                            # Data retrieval operations
│   ├── GET  /hash/{hash}               # Get latest version by hash (full response)
│   ├── GET  /hash/{hash}/data          # Get just the cached data (no metadata)
│   ├── GET  /hash/{hash}/metadata      # Get just metadata (no data) - fast stats
│   ├── GET  /hash/{hash}/config        # Get just config information
│   ├── GET  /hash/{hash}/latest        # Explicitly get latest (same as /hash/{hash})
│   ├── GET  /hash/{hash}/versions      # List all versions for this hash
│   ├── GET  /hash/{hash}/v/{version}   # Get specific version number
│   └── GET  /id/{cache_id}             # Get specific entry by cache ID
│
├── /exists                              # Existence checking
│   ├── HEAD /hash/{hash}               # Check if hash exists (status code only)
│   ├── GET  /hash/{hash}               # Check with details (version count, size)
│   ├── HEAD /id/{cache_id}             # Check if cache ID exists
│   └── POST /batch                     # Check multiple hashes at once
│
├── /search                              # Search and discovery
│   ├── POST /                          # Complex search with filters in body
│   ├── GET  /recent                    # Recently added entries
│   ├── GET  /by-tag/{tag}              # Search by single tag
│   ├── GET  /by-type/{content_type}    # Search by content type
│   ├── GET  /by-date/{date}            # Search by specific date (YYYY-MM-DD)
│   └── GET  /temporal                  # Search by time range (query params)
│
├── /refs                                # Reference graph navigation
│   ├── GET  /hash/{hash}               # Get all references for a hash
│   ├── GET  /hash/{hash}/stats         # Get reference statistics
│   ├── GET  /id/{cache_id}             # Get references for cache ID
│   ├── GET  /latest                    # List all latest references
│   └── GET  /tree                      # Get reference tree structure
│
├── /stats                               # Analytics and metrics
│   ├── GET  /summary                   # Overall cache statistics
│   ├── GET  /usage                     # Usage patterns and metrics
│   ├── GET  /usage/hourly              # Hourly breakdown
│   ├── GET  /usage/daily               # Daily breakdown
│   ├── GET  /storage                   # Storage utilization details
│   ├── GET  /performance               # Performance metrics
│   └── GET  /top                       # Most accessed items
│
├── /batch                               # Batch operations for efficiency
│   ├── POST /store                     # Store multiple items atomically
│   ├── POST /retrieve                  # Retrieve multiple items
│   ├── POST /metadata                  # Get metadata for multiple items
│   └── POST /delete                    # Delete multiple items
│
└── /admin                               # Administrative operations
    ├── POST   /rebuild-index            # Rebuild all cache indexes
    ├── POST   /rebuild-refs             # Rebuild reference graphs
    ├── POST   /cleanup                  # Clean old/orphaned entries
    ├── POST   /optimize                 # Optimize storage layout
    ├── DELETE /hash/{hash}              # Delete all versions of a hash
    ├── DELETE /hash/{hash}/v/{version}  # Delete specific version
    ├── DELETE /id/{cache_id}            # Delete by cache ID
    ├── DELETE /before/{date}            # Delete entries before date
    └── POST   /export                   # Export cache data for backup
```

## Request/Response Patterns

### Store Operations

**POST /cache/store** - Store with auto-generated hash
- Accepts data and metadata in request body
- Automatically calculates content hash
- Returns cache_id, hash, and storage location
- Creates new version if hash already exists

**PUT /cache/store/{hash}** - Store with specific hash
- Idempotent operation
- Useful for content-addressed storage
- Client controls the hash calculation
- Returns existing entry if hash matches

### Retrieve Operations

**GET /cache/retrieve/hash/{hash}** - Full retrieval
- Returns latest version by default
- Includes data, metadata, config, and stats
- Single response with all information

**GET /cache/retrieve/hash/{hash}/metadata** - Metadata only
- Fast operation (typically 1 file read)
- Returns size, timestamp, tags, version info
- No data transfer - ideal for stats/dashboards

**GET /cache/retrieve/hash/{hash}/data** - Data only
- Returns raw cached data
- Appropriate Content-Type header set
- No metadata overhead

### Search Operations

**POST /cache/search** - Complex queries
- Supports multiple filter criteria
- Pagination via limit/offset
- Can exclude data for faster results
- Returns matching entries with metadata

**GET /cache/search/by-tag/{tag}** - Simple tag search
- Quick lookup by single tag
- Returns all entries with specified tag
- Supports pagination via query params

### Batch Operations

**POST /cache/batch/retrieve** - Multiple retrieval
- Accepts list of hashes or cache_ids
- Returns map of found items
- Indicates missing items separately
- More efficient than multiple single requests

## Metadata Structure

The service stores but does not interpret metadata, allowing clients to store domain-specific information:

### Generic Metadata Fields
- `timestamp`: When the cache entry was created
- `content_type`: MIME type of the cached data
- `size`: Size of the cached data in bytes
- `tags`: List of string tags for categorization
- `source`: Origin of the data (e.g., "llm", "http", "filesystem")

### Domain-Specific Metadata
Clients can add any additional metadata fields:
- LLM: `model`, `provider`, `prompt_hash`, `temperature`
- HTTP: `url`, `method`, `status_code`, `headers`
- Filesystem: `path`, `permissions`, `owner`

The cache service stores these fields transparently without validation or interpretation.

## Versioning System

### Version Management
- Each unique hash can have multiple versions
- Versions are numbered sequentially (1, 2, 3, ...)
- Latest version is always quickly accessible
- Historical versions retained for comparison

### Version Access Patterns
```
/hash/{hash}           → Latest version (most common)
/hash/{hash}/latest    → Explicit latest (same as above)
/hash/{hash}/v/1       → First version
/hash/{hash}/v/2       → Second version
/hash/{hash}/versions  → List all versions with metadata
```

## Performance Characteristics

### Read Operations
- **Latest by hash**: 1-2 file reads maximum
- **Metadata only**: 1 file read (from .latest.json)
- **Specific version**: 2 file reads (ref + data)
- **Batch retrieve**: Optimized for parallel reads

### Write Operations
- **Single store**: 8-10 file operations (data + metadata + refs)
- **Batch store**: Optimized with transaction-like behavior
- **Index updates**: Asynchronous where possible

### Caching Strategy
- `.latest.json` files contain embedded metadata
- Reference files are lightweight pointers
- Sharded directory structure prevents bottlenecks
- Temporal organization enables efficient cleanup

## Error Handling

### Standard HTTP Status Codes
- `200 OK`: Successful retrieval
- `201 Created`: New cache entry created
- `204 No Content`: Successful deletion
- `400 Bad Request`: Invalid parameters
- `404 Not Found`: Hash or cache_id doesn't exist
- `409 Conflict`: Version conflict during store
- `500 Internal Server Error`: Storage backend failure

### Error Response Format
All errors return consistent JSON structure with:
- `error`: Error type/code
- `message`: Human-readable description
- `details`: Additional context (optional)
- `timestamp`: When error occurred

## Security Considerations

### Access Control
- API key authentication for all endpoints
- Rate limiting per API key
- Separate admin credentials for destructive operations

### Data Validation
- Content size limits configurable
- Hash verification on retrieval
- Metadata schema validation (optional)

### Audit Trail
- All operations logged with timestamp and client
- Deletion operations require confirmation
- Admin operations specially logged

## Migration and Compatibility

### API Versioning
- Version included in base path: `/v1/cache/...`
- Backward compatibility maintained within major versions
- Deprecation notices in headers before breaking changes

### Storage Migration
- Tools provided for backend migration
- Zero-downtime migration support
- Backward-compatible storage formats

## Client Integration

### SDK Support
Clients should implement:
- Automatic retry with exponential backoff
- Hash calculation before store operations
- Metadata enrichment for their domain
- Batch operations for multiple items

### Usage Patterns

**Simple Store and Retrieve**
1. POST to `/cache/store` with data and metadata
2. Receive hash in response
3. GET from `/cache/retrieve/hash/{hash}` when needed

**Conditional Retrieval**
1. GET `/cache/retrieve/hash/{hash}/metadata`
2. Check timestamp, size, or tags
3. GET `/cache/retrieve/hash/{hash}/data` if needed

**Version Comparison**
1. GET `/cache/retrieve/hash/{hash}/versions`
2. Select versions to compare
3. GET `/cache/retrieve/hash/{hash}/v/{version}` for each

## Monitoring and Observability

### Health Endpoints
- `/cache/info/health`: Basic health check
- `/cache/info/status`: Detailed status including storage
- `/cache/stats/summary`: Overall cache metrics

### Key Metrics
- Cache hit/miss ratio
- Storage utilization percentage
- Average response times by operation
- Version count distribution
- Top accessed hashes