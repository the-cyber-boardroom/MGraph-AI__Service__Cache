# MGraph-AI Cache Service - Implementation Architecture

## Current State Problems

The existing implementation has fundamental flaws:
- Uses `Random_Guid()` for every store operation instead of content-based hashing
- Forces all data through base64 encoding
- `retrieve_by_hash` returns "not supported" 
- No versioning despite storing cache_id with each entry
- Temporal storage pattern instead of hash-based sharding

## Target Architecture

### Core Principle: Content-Addressed Storage
- **Primary Key**: Content hash (SHA-256)
- **Secondary Key**: Cache ID (for version tracking)
- **Access Pattern**: Hash → Latest version or specific version
- **Storage Pattern**: Data in temporal dirs, references in hash-sharded dirs

### Key Changes Required

#### 1. Storage Structure
```
cache/
├── data/                         # Temporal organization (keep as-is)
│   └── YYYY/MM/DD/HH/           
│       └── {hash}-v{n}.{ext}    # Change from cache-id to hash-version
│
└── refs/                         # NEW: Add hash-based references
    └── by-hash/                  
        └── {h1}/{h2}/            # Sharded by first 4 chars
            ├── {hash}.latest.json # Embedded metadata for 1-read
            └── {hash}.refs.json   # All versions list
```

#### 2. Data Flow Changes

**Current (BROKEN)**:
```
Store: data → Random_Guid → cache_id → temporal storage
Retrieve: cache_id → temporal storage → data
By Hash: "not supported"
```

**Target (CORRECT)**:
```
Store: data → SHA-256 hash → check existing → store with version
Retrieve: hash → refs/by-hash → latest.json → data path → data
By ID: cache_id → hash mapping → same as above
```

#### 3. Data Handling

**Remove**:
- Forced base64 encoding in `Schema__Cache__Store__Request`
- String-only data field

**Add**:
- Direct binary support in request/response
- Type-aware serialization
- Optional compression

## Critical Implementation Changes

### Phase Overview

**Phase 1: Fix Core Hash-Based Storage** (1-2 days)
- Implement hash-first storage
- Add reference system
- Fix retrieve_by_hash

**Phase 2: Efficient Data Handling** (1-2 days)
- Remove base64 requirement
- Add binary support
- Add compression

**Phase 3: Advanced Features** (2-3 days)
- Versioning system
- Batch operations
- Search capabilities

## Success Criteria

1. **Functional**: 
   - Same content always produces same hash
   - Retrieve by hash returns latest version
   - No duplicate storage of identical content

2. **Performance**:
   - Metadata retrieval: 1 file read
   - Full retrieval: 2 file reads
   - Write: 8-10 file operations acceptable

3. **API Simplicity**:
   - `cache.put("key", value)` → auto-hash → store
   - `cache.get("key")` → hash lookup → retrieve

## What Stays The Same

- Memory-FS abstraction layer
- S3 backend implementation
- FastAPI route structure
- Authentication system
- LocalStack testing setup

## What Must Change

- `Cache__Service.store()`: Use hash, not Random_Guid
- `Cache__Service.retrieve()`: Support hash as primary
- `Schema__Cache__Store__Request`: Support multiple data types
- Add `refs/by-hash/` directory structure
- Implement `.latest.json` with embedded metadata