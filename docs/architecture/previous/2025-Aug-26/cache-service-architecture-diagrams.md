# Cache Service Architecture Diagrams

## Overview
This document provides visual representations of the MGraph-AI Cache Service architecture, illustrating the storage structure, data flow, and API interactions.

## 1. System Architecture Overview

```mermaid
graph TB
    subgraph "Client Applications"
        LLM[LLM Service]
        HTTP[HTTP Proxy]
        FS[File System]
        WEB[Web Monitor]
    end
    
    subgraph "Cache Service API Layer"
        API[FastAPI Routes]
        AUTH[Authentication]
        RL[Rate Limiting]
    end
    
    subgraph "Cache Service Core"
        CS[Cache Service]
        GM[Graph Manager]
        VM[Version Manager]
        IDX[Index Manager]
    end
    
    subgraph "Storage Layer"
        MFS[Memory-FS Abstraction]
        subgraph "Storage Backends"
            S3[S3/Boto3]
            SQLite[SQLite]
            MEM[Memory]
            DISK[Local Disk]
        end
    end
    
    LLM --> API
    HTTP --> API
    FS --> API
    WEB --> API
    
    API --> AUTH
    AUTH --> RL
    RL --> CS
    
    CS --> GM
    CS --> VM
    CS --> IDX
    
    GM --> MFS
    VM --> MFS
    IDX --> MFS
    
    MFS --> S3
    MFS --> SQLite
    MFS --> MEM
    MFS --> DISK
```

## 2. G3 Storage Structure

```mermaid
graph LR
    subgraph "Data Storage (Single Source of Truth)"
        D1[data/2024/12/15/14/cache-id-abc123.json]
        D2[data/2024/12/15/14/cache-id-xyz789.json]
        D3[data/2025/01/10/11/cache-id-def456.json]
    end
    
    subgraph "Reference Graphs"
        subgraph "By Hash"
            H1[refs/by-hash/a7/3f/a73f2e4b.refs.json]
            H2[refs/by-hash/a7/3f/a73f2e4b.latest.json]
        end
        
        subgraph "By ID"
            ID1[refs/by-id/ab/c1/abc123.ref.json]
            ID2[refs/by-id/xy/z7/xyz789.ref.json]
            ID3[refs/by-id/de/f4/def456.ref.json]
        end
        
        subgraph "By Type"
            T1[refs/by-type/llm/openai/gpt-4.refs.json]
            T2[refs/by-type/http/domains/github-com.refs.json]
        end
        
        subgraph "By URL"
            U1[refs/by-url/com/github/owasp-sbot.refs.json]
        end
    end
    
    H1 --> D1
    H1 --> D2
    H1 --> D3
    H2 --> D3
    
    ID1 --> D1
    ID2 --> D2
    ID3 --> D3
    
    T1 --> D1
    T1 --> D3
    T2 --> D2
    
    U1 --> D1
    U1 --> D2
    U1 --> D3
```

## 3. Read Operation Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Cache Service
    participant Graph Manager
    participant Storage
    
    Note over Client,Storage: Scenario: Get Latest by Hash
    
    Client->>API: GET /cache/retrieve/hash/{hash}
    API->>Cache Service: retrieve_by_hash(hash)
    Cache Service->>Graph Manager: get_latest_ref(hash)
    Graph Manager->>Storage: read(refs/by-hash/a7/3f/a73f2e4b.latest.json)
    Storage-->>Graph Manager: {cache_id, path, metadata, config}
    
    Note over Cache Service: Check if data needed
    
    Cache Service->>Storage: read(data/2025/01/10/11/cache-id-def456.json)
    Storage-->>Cache Service: actual data
    Cache Service-->>API: {data, metadata, config, stats}
    API-->>Client: 200 OK + response
    
    Note over Client,Storage: Total: 2 file reads
```

## 4. Write Operation Flow

```mermaid
sequenceDiagram
    participant Client
    participant API
    participant Cache Service
    participant Version Manager
    participant Graph Manager
    participant Storage
    
    Client->>API: POST /cache/store
    API->>Cache Service: store(data, metadata)
    Cache Service->>Cache Service: calculate_hash(data)
    Cache Service->>Cache Service: generate_cache_id()
    
    Note over Cache Service,Storage: Step 1: Store Data Files
    
    Cache Service->>Storage: write(data/2025/01/10/12/cache-id-ghi789.json)
    Cache Service->>Storage: write(data/.../cache-id-ghi789.json.config)
    Cache Service->>Storage: write(data/.../cache-id-ghi789.json.metadata)
    
    Note over Cache Service,Storage: Step 2: Update References
    
    Cache Service->>Graph Manager: update_hash_refs(hash, cache_id)
    Graph Manager->>Storage: read(refs/by-hash/a7/3f/a73f2e4b.refs.json)
    Graph Manager->>Storage: update(refs/by-hash/a7/3f/a73f2e4b.refs.json)
    Graph Manager->>Storage: write(refs/by-hash/a7/3f/a73f2e4b.latest.json)
    
    Cache Service->>Graph Manager: create_id_ref(cache_id)
    Graph Manager->>Storage: write(refs/by-id/gh/i7/ghi789.ref.json)
    
    Cache Service->>Storage: update(indexes/master.index.json)
    
    Cache Service-->>API: {cache_id, hash, version, path}
    API-->>Client: 201 Created
    
    Note over Client,Storage: Total: 8-10 file operations
```

## 5. API Route Hierarchy

```mermaid
graph TB
    ROOT[/cache]
    
    ROOT --> INFO[/info]
    INFO --> INFO_HEALTH[GET /health]
    INFO --> INFO_STATUS[GET /status]
    INFO --> INFO_SERVER[GET /server]
    INFO --> INFO_VERSIONS[GET /versions]
    
    ROOT --> STORE[/store]
    STORE --> STORE_POST[POST /]
    STORE --> STORE_PUT[PUT /{hash}]
    
    ROOT --> RETRIEVE[/retrieve]
    RETRIEVE --> RET_HASH[/hash/{hash}]
    RET_HASH --> RET_HASH_GET[GET /]
    RET_HASH --> RET_HASH_DATA[GET /data]
    RET_HASH --> RET_HASH_META[GET /metadata]
    RET_HASH --> RET_HASH_CONFIG[GET /config]
    RET_HASH --> RET_HASH_VERSIONS[GET /versions]
    RET_HASH --> RET_HASH_VERSION[GET /v/{version}]
    RETRIEVE --> RET_ID[GET /id/{cache_id}]
    
    ROOT --> EXISTS[/exists]
    EXISTS --> EXISTS_HASH[/hash/{hash}]
    EXISTS_HASH --> EXISTS_HEAD[HEAD /]
    EXISTS_HASH --> EXISTS_GET[GET /]
    EXISTS --> EXISTS_BATCH[POST /batch]
    
    ROOT --> SEARCH[/search]
    SEARCH --> SEARCH_POST[POST /]
    SEARCH --> SEARCH_TAG[GET /by-tag/{tag}]
    SEARCH --> SEARCH_TYPE[GET /by-type/{type}]
    SEARCH --> SEARCH_TEMPORAL[GET /temporal]
    
    ROOT --> BATCH[/batch]
    BATCH --> BATCH_STORE[POST /store]
    BATCH --> BATCH_RETRIEVE[POST /retrieve]
    BATCH --> BATCH_DELETE[POST /delete]
```

## 6. Version Management System

```mermaid
graph TB
    subgraph "Same Hash - Multiple Versions"
        HASH[Hash: a73f2e4b]
        
        V1[Version 1<br/>cache-id: abc123<br/>timestamp: T1]
        V2[Version 2<br/>cache-id: xyz789<br/>timestamp: T2]
        V3[Version 3<br/>cache-id: def456<br/>timestamp: T3]
        
        HASH --> V1
        HASH --> V2
        HASH --> V3
    end
    
    subgraph "Reference Files"
        REFS[a73f2e4b.refs.json<br/>Lists all versions]
        LATEST[a73f2e4b.latest.json<br/>Points to V3]
    end
    
    subgraph "Use Cases"
        UC1[Web Page Monitoring<br/>Same content captured<br/>at different times]
        UC2[LLM Responses<br/>Same prompt run<br/>multiple times]
        UC3[HTTP Caching<br/>Same response from<br/>different requests]
    end
    
    REFS -.-> V1
    REFS -.-> V2
    REFS -.-> V3
    LATEST ==> V3
    
    UC1 --> HASH
    UC2 --> HASH
    UC3 --> HASH
```

## 7. Sharding Strategy

```mermaid
graph TB
    subgraph "Hash Sharding Example"
        FULL_HASH[Full Hash: a73f2e4b8c9d0e1f]
        
        FULL_HASH --> L1[Level 1: a7/]
        L1 --> L2[Level 2: 3f/]
        L2 --> FILE[File: a73f2e4b8c9d0e1f.refs.json]
        
        PATH[Full Path: refs/by-hash/a7/3f/a73f2e4b8c9d0e1f.refs.json]
    end
    
    subgraph "Configuration"
        CONFIG[Sharding Config<br/>depth: 2<br/>chars_per_level: 2]
    end
    
    subgraph "Benefits"
        B1[Prevents directory size issues]
        B2[Improves file system performance]
        B3[Enables efficient distribution]
        B4[Supports parallel access]
    end
    
    CONFIG --> FULL_HASH
    PATH --> B1
    PATH --> B2
    PATH --> B3
    PATH --> B4
```

## 8. Performance Optimization Flow

```mermaid
graph LR
    subgraph "Client Request"
        REQ[Need Cache Entry]
    end
    
    subgraph "Fast Path (1 read)"
        LATEST[Read .latest.json<br/>Has embedded metadata]
        DECISION{Need<br/>actual<br/>data?}
    end
    
    subgraph "Optional (2nd read)"
        DATA[Read actual data file]
    end
    
    subgraph "Response"
        META_ONLY[Return metadata only<br/>Size, timestamp, tags]
        FULL[Return full response<br/>Data + metadata]
    end
    
    REQ --> LATEST
    LATEST --> DECISION
    DECISION -->|No| META_ONLY
    DECISION -->|Yes| DATA
    DATA --> FULL
```

## 9. Storage Backend Architecture

```mermaid
graph TB
    subgraph "Memory-FS Abstraction Layer"
        MFS[Memory-FS<br/>Type-Safe Interface]
        
        subgraph "Core Operations"
            OP1[file__save]
            OP2[file__bytes]
            OP3[file__exists]
            OP4[file__delete]
            OP5[file__json]
        end
    end
    
    subgraph "Storage Implementations"
        subgraph "Development"
            MEM_IMPL[Storage_FS__Memory<br/>In-memory for testing]
            LOCAL_IMPL[Storage_FS__LocalStack<br/>S3 emulation]
        end
        
        subgraph "Production"
            S3_IMPL[Storage_FS__S3<br/>AWS S3 via Boto3]
            SQLITE_IMPL[Storage_FS__SQLite<br/>Embedded database]
        end
        
        subgraph "Future"
            ZIP_IMPL[Storage_FS__Zip<br/>Compressed archives]
            AZURE_IMPL[Storage_FS__Azure<br/>Azure Blob Storage]
        end
    end
    
    MFS --> OP1
    MFS --> OP2
    MFS --> OP3
    MFS --> OP4
    MFS --> OP5
    
    OP1 --> MEM_IMPL
    OP1 --> LOCAL_IMPL
    OP1 --> S3_IMPL
    OP1 --> SQLITE_IMPL
    
    OP2 --> MEM_IMPL
    OP2 --> LOCAL_IMPL
    OP2 --> S3_IMPL
    OP2 --> SQLITE_IMPL
```

## 10. Data Flow for Different Use Cases

```mermaid
graph TB
    subgraph "Input Sources"
        LLM_REQ[LLM Request]
        HTTP_REQ[HTTP Request]
        FILE_REQ[File Upload]
        WEB_REQ[Web Scrape]
    end
    
    subgraph "Cache Service"
        HASH_CALC[Calculate Hash]
        CHECK_EXISTS{Hash<br/>Exists?}
        NEW_VERSION[Create New Version]
        STORE[Store Data]
    end
    
    subgraph "Storage Structure"
        DATA_STORE[data/temporal/...]
        REFS[refs/by-hash/...]
        LATEST[.latest.json]
    end
    
    subgraph "Retrieval Patterns"
        GET_LATEST[Get Latest Version]
        GET_SPECIFIC[Get Specific Version]
        GET_ALL[Get All Versions]
    end
    
    LLM_REQ --> HASH_CALC
    HTTP_REQ --> HASH_CALC
    FILE_REQ --> HASH_CALC
    WEB_REQ --> HASH_CALC
    
    HASH_CALC --> CHECK_EXISTS
    CHECK_EXISTS -->|Yes| NEW_VERSION
    CHECK_EXISTS -->|No| STORE
    NEW_VERSION --> STORE
    
    STORE --> DATA_STORE
    STORE --> REFS
    STORE --> LATEST
    
    DATA_STORE --> GET_LATEST
    DATA_STORE --> GET_SPECIFIC
    DATA_STORE --> GET_ALL
    
    REFS --> GET_ALL
    LATEST --> GET_LATEST
```

## 11. Request/Response Lifecycle

```mermaid
stateDiagram-v2
    [*] --> ClientRequest: API Call
    
    ClientRequest --> Authentication: Valid API Key
    Authentication --> RateLimiting: Check Limits
    RateLimiting --> RouteHandler: Process Request
    
    RouteHandler --> CacheService: Business Logic
    
    state CacheService {
        [*] --> ValidateInput
        ValidateInput --> CheckCache
        
        state CheckCache {
            [*] --> ReadRefs: Check References
            ReadRefs --> RefFound: Found
            ReadRefs --> RefNotFound: Not Found
        }
        
        RefFound --> ReadData: Fetch Data
        RefNotFound --> ReturnNotFound
        
        ReadData --> PrepareResponse
        ReturnNotFound --> PrepareResponse
    }
    
    CacheService --> FormatResponse: Create Response
    FormatResponse --> ClientResponse: Return Result
    ClientResponse --> [*]
```

## 12. Cache Entry Lifecycle

```mermaid
graph LR
    subgraph "Creation"
        CREATE[New Entry<br/>cache-id: abc123]
        ACTIVE[Active<br/>Being accessed]
    end
    
    subgraph "Versioning"
        V1[Version 1]
        V2[Version 2<br/>Same hash]
        V3[Version 3<br/>Latest]
    end
    
    subgraph "Aging"
        WARM[Warm<br/>Recent access]
        COLD[Cold<br/>No recent access]
    end
    
    subgraph "Cleanup"
        ARCHIVE[Archived<br/>Moved to cold storage]
        DELETE[Deleted<br/>Removed from system]
    end
    
    CREATE --> ACTIVE
    ACTIVE --> V1
    V1 --> V2
    V2 --> V3
    
    ACTIVE --> WARM
    WARM --> COLD
    
    COLD --> ARCHIVE
    COLD --> DELETE
    ARCHIVE --> DELETE
```

---

## Diagram Descriptions

1. **System Architecture Overview**: High-level view of all components and their interactions
2. **G3 Storage Structure**: Shows the graph-based reference system pointing to single data source
3. **Read Operation Flow**: Sequence diagram of a typical read operation (2 file reads)
4. **Write Operation Flow**: Sequence diagram of a write operation (8-10 file operations)
5. **API Route Hierarchy**: Complete API endpoint structure
6. **Version Management System**: How multiple versions share the same hash
7. **Sharding Strategy**: Hash-based directory sharding for scalability
8. **Performance Optimization Flow**: Decision flow for fast metadata access
9. **Storage Backend Architecture**: Memory-FS abstraction and implementations
10. **Data Flow for Different Use Cases**: How different sources flow through the cache
11. **Request/Response Lifecycle**: State diagram of request processing
12. **Cache Entry Lifecycle**: Timeline of a cache entry from creation to deletion

These diagrams complement the architecture documents and provide visual understanding of the cache service design.