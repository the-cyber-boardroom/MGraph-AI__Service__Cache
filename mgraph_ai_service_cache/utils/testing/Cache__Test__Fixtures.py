from typing                                                                       import Dict, Any, Optional
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from osbot_utils.utils.Misc                                                       import timestamp_now
from mgraph_ai_service_cache.service.cache.Cache__Service                         import Cache__Service


class Cache__Test__Fixtures(Type_Safe):                                               # Manages reusable test fixtures for cache service tests
    cache_service     : Cache__Service                    = None                      # Cache service instance for fixture storage
    manifest_cache_id : Random_Guid                       = None                      # Predictable ID for manifest storage
    fixtures_bucket   : str                               = "test-cache-fixtures"     # S3 bucket for test fixtures
    namespace         : Safe_Str__Id                      = None                      # Namespace for fixture isolation
    fixtures          : Dict[str, dict[str, Any]]                                     # Map of fixture names to metadata
    setup_completed   : bool                              = False                     # Prevents redundant setup
    manifest          : Dict[str, Any]                    = None                      # Loaded manifest data
    delete_on_exit    : bool                              = False                     # Whether to cleanup fixtures

    # Core fixtures that tests commonly need
    default_fixtures = { 'string_simple'   : "test retrieve string data"                         ,
                         'string_medium'   : "test data " * 50                                   ,     # ~500 chars
                         'string_large'    : "x" * 1000                                          ,
                         'json_simple'     : {"key": "value", "number": 42}                      ,
                         'json_complex'    : { "nested" : {"data": [1, 2, 3], "more": {"level": 3}},
                                              "flag"    : True                                    ,
                                              "count"   : 100                                     ,
                                              "tags"    : ["test", "fixture", "cache"]           },
                         'json_empty'      : {}                                                  ,
                         'binary_small'    : b'\x01\x02\x03\x04\x05'                             ,
                         'binary_medium'   : bytes(range(100))                                   ,
                         'binary_large'    : bytes(range(256)) * 4                               }

    def setup(self) -> 'Cache__Test__Fixtures':                                       # Initialize cache service and load/create fixtures
        if self.setup_completed:
            return self

        self.cache_service = Cache__Service(default_bucket=self.fixtures_bucket)      # Initialize cache service

        if not self.namespace:                                                        # Set default namespace if not provided
            self.namespace = Safe_Str__Id("test-fixtures")

        if not self.manifest_cache_id:                                                # Use predictable GUID for manifest
            self.manifest_cache_id = Random_Guid("00000000-0000-0000-0000-000000000001")

        if self.load_manifest():                                                      # Try to load existing manifest
            if self.verify_fixtures():
                self.setup_completed = True
                return self                                                            # All fixtures exist, we're done

        self.create_fixtures()                                                        # Create missing fixtures
        self.save_manifest()
        self.setup_completed = True
        return self

    def load_manifest(self) -> bool:                                                  # Load fixture manifest from cache service
        result = self.cache_service.retrieve_by_id(cache_id  = self.manifest_cache_id,
                                                   namespace = self.namespace        )
        if result:
            self.manifest = result.get("data")
            self.fixtures = self.manifest.get("fixtures", {})
            return True
        return False

    def verify_fixtures(self) -> bool:                                                # Check all fixtures still exist
        for fixture_name, fixture_info in self.fixtures.items():
            cache_id = Random_Guid(fixture_info.get("cache_id"))
            result = self.cache_service.retrieve_by_id(cache_id  = cache_id,
                                                       namespace = self.namespace)
            if not result:
                return False                                                           # Missing fixture, need to recreate
        return True

    def create_fixtures(self):                                                        # Create test fixtures in cache
        for name, data in self.default_fixtures.items():
            if name not in self.fixtures:
                if isinstance(data, bytes):                                           # Determine hash based on data type
                    cache_hash = self.cache_service.hash_from_bytes(data)
                elif isinstance(data, dict):
                    cache_hash = self.cache_service.hash_from_json(data)
                else:
                    cache_hash = self.cache_service.hash_from_string(str(data))

                cache_id = Random_Guid()

                self.cache_service.store_with_strategy(storage_data = data        ,   # Store in cache
                                                       cache_hash   = cache_hash  ,
                                                       cache_id     = cache_id    ,
                                                       strategy     = "direct"     ,   # Use direct for test fixtures
                                                       namespace    = self.namespace)

                self.fixtures[name] = { "cache_id" : str(cache_id)          ,         # Update fixtures map
                                       "hash"      : str(cache_hash)        ,
                                       "type"      : type(data).__name__    }

    def save_manifest(self):                                                          # Save fixture manifest to cache
        manifest_data = { "fixtures"   : self.fixtures     ,
                         "created_at" : timestamp_now()    ,
                         "bucket"     : self.fixtures_bucket}

        manifest_hash = self.cache_service.hash_from_json(manifest_data)

        self.cache_service.store_with_strategy(storage_data = manifest_data      ,
                                               cache_hash   = manifest_hash      ,
                                               cache_id     = self.manifest_cache_id,
                                               strategy     = "direct"            ,
                                               namespace    = self.namespace     )
        self.manifest = manifest_data

    def get_fixture(self, name: str) -> Optional[Dict[str, Any]]:                     # Get fixture info by name
        return self.fixtures.get(name)

    def get_fixture_data(self, name: str) -> Any:                                     # Retrieve actual fixture data
        fixture_info = self.get_fixture(name)
        if fixture_info:
            cache_id = Random_Guid(fixture_info["cache_id"])
            result = self.cache_service.retrieve_by_id(cache_id  = cache_id,
                                                       namespace = self.namespace)
            return result.get("data") if result else None
        return None

    def get_fixture_id(self, name: str) -> Optional[Random_Guid]:                     # Get cache ID for fixture
        fixture_info = self.get_fixture(name)
        if fixture_info:
            return Random_Guid(fixture_info["cache_id"])
        return None

    def get_fixture_hash(self, name: str) -> Optional[str]:                           # Get hash for fixture
        fixture_info = self.get_fixture(name)
        if fixture_info:
            return fixture_info["hash"]
        return None

    def cleanup_fixtures(self):                                                       # Delete all fixtures (optional cleanup)
        if not self.delete_on_exit:
            return

        for fixture_name, fixture_info in self.fixtures.items():
            cache_id = Random_Guid(fixture_info["cache_id"])
            self.cache_service.delete_by_id(cache_id  = cache_id,
                                           namespace = self.namespace)

        self.cache_service.delete_by_id(cache_id  = self.manifest_cache_id,           # Delete manifest
                                        namespace = self.namespace        )

        self.fixtures.clear()
        self.manifest = {}

    def cleanup_all(self):  # Complete cleanup including bucket
        self.cleanup_fixtures()  # Delete fixtures first

        if self.cache_service and self.fixtures_bucket:
            # Delete the bucket itself
            handler = self.cache_service.get_or_create_handler(self.namespace)
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(self.fixtures_bucket):
                    s3.bucket_delete_all_files(self.fixtures_bucket)
                    s3.bucket_delete(self.fixtures_bucket)