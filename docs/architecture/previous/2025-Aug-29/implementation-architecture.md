# MGraph-AI Cache Service - Implementation Architecture

## Current State Problems

The existing implementation has fundamental flaws that violate Type_Safe and Memory_FS principles:
- Uses `Random_Guid()` for every store operation instead of content-based hashing
- Forces all data through base64 encoding (violates "no raw primitives" rule)
- `retrieve_by_hash` returns "not supported" 
- No versioning despite storing cache_id with each entry
- Temporal storage pattern without leveraging Memory_FS capabilities
- Raw primitives throughout (str, bytes) instead of Safe types

## Target Architecture

### Core Principle: Content-Addressed Storage with Type_Safe
- **Primary Key**: Content hash (SHA-256, 16 chars minimum)
- **Secondary Key**: Cache ID (for version tracking)
- **Access Pattern**: Hash → Latest version or specific version
- **Storage Pattern**: Memory_FS with multiple path handlers
- **Type Safety**: Safe types throughout, no raw primitives

### Key Architecture Changes

#### 1. Leverage Memory_FS Path Handlers
```
Memory_FS Instance
├── Path__Handler__Temporal       # Data storage (existing)
│   └── data/YYYY/MM/DD/HH/
│       └── {hash}-v{n}.cache
│
└── Path__Handler__Hash_Sharded   # NEW: Hash-based references
    └── refs/by-hash/
        └── {h1}/{h2}/            # Sharded by first 4 chars
            ├── {hash}.latest     # Embedded metadata
            └── {hash}.versions   # All versions list
```

#### 2. Type_Safe Data Flow

**Current (BROKEN)**:
```
Store: raw str → base64 → Random_Guid → temporal storage
Retrieve: cache_id → temporal storage → raw str
By Hash: "not supported"
```

**Target (CORRECT)**:
```
Store: Safe_Bytes → SHA-256 hash → check existing → store with version
Retrieve: Safe_Str__Hash → Memory_FS multi-path → latest → data
By ID: Safe_Id → hash mapping → same as above
```

#### 3. Safe Type Data Handling

**Remove**:
- Raw `str` data field
- Raw `bytes` anywhere
- Forced base64 encoding

**Add**:
- `Safe_Bytes` for binary data
- `Safe_Str__Hash` with proper length (16+ chars)
- Type-aware serialization via Memory_FS
- Optional compression with Safe types

## Critical Implementation Principles

### Memory_FS Integration

1. **Use Path Handlers**: Don't create custom reference managers
2. **Multi-path support**: One Memory_FS instance with multiple handlers
3. **File types**: Define `Memory_FS__File__Type__Cache`
4. **Storage backends**: Leverage Storage_FS__S3 metadata capabilities

### Type_Safe Compliance

1. **NO raw primitives**: Every str → Safe_Str variant, bytes → Safe_Bytes
2. **Auto-initialization**: Let Type_Safe handle defaults
3. **Pure schemas**: No business logic in schema files
4. **Type_Safe collections**: Dict → Type_Safe__Dict with validation

## Phase Overview

**Phase 1: Fix Core Hash-Based Storage** (1-2 days)
- Implement Memory_FS path handlers for hash sharding
- Use Safe_Str__Hash (16+ chars) as primary key
- Fix retrieve_by_hash using Memory_FS patterns

**Phase 2: Type-Safe Data Handling** (1-2 days)
- Replace all raw primitives with Safe types
- Add Safe_Bytes support
- Implement compression with Type_Safe

**Phase 3: Advanced Features** (2-3 days)
- Versioning using Memory_FS patterns
- Batch operations with Type_Safe validation
- Search via Memory_FS path strategies

## Success Criteria

1. **Type Safety**: 
   - No raw primitives anywhere
   - All collections are Type_Safe variants
   - Continuous runtime validation

2. **Memory_FS Alignment**:
   - Path handlers for all storage patterns
   - Multi-path file storage
   - Proper file type definitions

3. **Functional**: 
   - Same content always produces same hash
   - Retrieve by hash returns latest version
   - No duplicate storage of identical content

4. **Performance**:
   - Metadata retrieval: 1 file read via Memory_FS
   - Full retrieval: 2 file reads
   - S3 metadata used effectively

5. **API Simplicity**:
   - Type_Safe schemas throughout
   - Auto-conversion where appropriate
   - Clean separation of concerns

## What Stays The Same

- Memory_FS abstraction layer (properly used)
- S3 backend implementation
- FastAPI route structure with Type_Safe
- Authentication system
- LocalStack testing setup

## What Must Change

- All raw primitives → Safe types
- Custom reference manager → Memory_FS path handlers
- Manual initialization → Type_Safe auto-init
- Business logic in schemas → Pure schemas
- Custom sharding → Path__Handler pattern
- 10-char hashes → 16+ char Safe_Str__Hash

## Testing Strategy

Following Type_Safe testing patterns:
- Use `setUpClass` for expensive operations
- Context managers with `_` throughout
- `.obj()` with `__` for comparisons
- Test type enforcement and auto-conversion
- Verify Memory_FS path generation