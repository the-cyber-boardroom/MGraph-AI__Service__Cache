from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_utils.testing.__                                                         import __, __SKIP__
from memory_fs.storage_fs.providers.Storage_FS__Memory                              import Storage_FS__Memory
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.utils.Objects                                                      import base_classes, obj
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__File__Refs           import Schema__Cache__File__Refs
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Storage_Mode          import Enum__Cache__Storage_Mode
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy       import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Config                            import Cache__Config
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service

class test_Cache__Service(TestCase):

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME expensive setup
        # Use memory mode for fast tests
        cls.config          = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY)
        cls.cache_service   = Cache__Service(cache_config=cls.config)
        cls.test_namespace  = Safe_Str__Id("test-service")
        cls.created_ids     = []                                                # Track for cleanup         # todo: since we are in memory, we will not need these
        cls.path_now        = Path__Handler__Temporal().path_now()                           # Current temporal path

    # @classmethod
    # def tearDownClass(cls):                                                 # Clean up any test data
    #     for cache_id in cls.created_ids:
    #         try:
    #             cls.cache_service.delete_by_id(cache_id, cls.test_namespace)
    #         except:
    #             pass

    def test__init__(self):                                                 # Test initialization
        with Cache__Service() as _:
            assert type(_)                   is Cache__Service
            assert base_classes(_)           == [Type_Safe, object]
            assert type(_.cache_config)     is Cache__Config
            assert _.cache_handlers          == {}                         # Empty initially
            assert _.storage_backend         is not None                   # Created on init

            # Config should auto-detect mode (memory without AWS creds)
            assert _.cache_config.storage_mode == Enum__Cache__Storage_Mode.MEMORY

    def test__init__with_config(self):                                     # Test initialization with explicit config
        config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY,
                              default_ttl_hours=48)

        with Cache__Service(cache_config=config) as _:
            assert _.cache_config            is config
            assert _.cache_config.storage_mode      == Enum__Cache__Storage_Mode.MEMORY
            assert _.cache_config.default_ttl_hours == 48

    def test_get_or_create_handler(self):                                  # Test handler creation and caching
        with self.cache_service as _:
            # First call creates handler
            handler1 = _.get_or_create_handler(self.test_namespace)
            assert handler1 is not None
            assert handler1.namespace       == str(self.test_namespace)
            assert handler1.storage_backend is _.storage_backend()

            # Second call returns same handler
            handler2 = _.get_or_create_handler(self.test_namespace)
            assert handler2 is handler1                                    # Same instance

            # Different namespace gets different handler
            other_namespace = Safe_Str__Id("other-namespace")
            handler3 = _.get_or_create_handler(other_namespace)
            assert handler3 is not handler1
            assert handler3.namespace == str(other_namespace)

            # But they share the same storage backend
            assert handler3.storage_backend is handler1.storage_backend

    def test_get_storage_info(self):                                       # Test storage info retrieval
        with self.cache_service as _:
            info = _.get_storage_info()

            assert info == {'storage_mode': 'memory', 'ttl_hours': 24}
            assert 's3_bucket' not in info                                 # No S3 config in memory mode

    def test_storage_fs(self):                                                  # Test storage backend access
        with self.cache_service as _:
            storage = _.storage_fs()

            assert storage is not None
            assert storage is _.storage_backend()                               # Same instance

            assert type(storage) is Storage_FS__Memory                          # In memory mode, should be Memory storage

    def test_store_with_strategy(self):                                    # Test storing data
        with self.cache_service as _:
            test_data = {"test": "data", "value": 42}
            cache_hash = _.hash_from_json(test_data)
            cache_id = Random_Guid()

            result = _.store_with_strategy(storage_data = test_data      ,
                                          cache_hash   = cache_hash      ,
                                          cache_id     = cache_id        ,
                                          strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                          namespace    = self.test_namespace)

            self.created_ids.append(cache_id)                              # Track for cleanup

            assert result.cache_id    == cache_id
            assert result.cache_hash  == cache_hash
            assert result.namespace   == self.test_namespace
            assert result.size      > 0
            assert 'data' in result.paths
            assert 'by_hash' in result.paths
            assert 'by_id' in result.paths

    def test_retrieve_by_id(self):                                         # Test retrieval by ID
        with self.cache_service as _:
            # Store test data
            test_data = {"retrieve": "by_id", "test": True}
            cache_hash = _.hash_from_json(test_data)
            cache_id = Random_Guid()

            _.store_with_strategy(storage_data = test_data,
                                 cache_hash   = cache_hash,
                                 cache_id     = cache_id,
                                 strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                 namespace    = self.test_namespace)

            self.created_ids.append(cache_id)

            # Retrieve it
            result = _.retrieve_by_id(cache_id, self.test_namespace)

            assert result is not None
            assert result['data']      == test_data
            assert result['data_type'] == 'json'
            assert 'metadata' in result

    def test_retrieve_by_hash(self):                                       # Test retrieval by hash
        with self.cache_service as _:
            # Store test data
            test_data = {"retrieve": "by_hash", "test": True}
            cache_hash = _.hash_from_json(test_data)
            cache_id = Random_Guid()

            _.store_with_strategy(storage_data = test_data,
                                 cache_hash   = cache_hash,
                                 cache_id     = cache_id,
                                 strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                 namespace    = self.test_namespace)

            self.created_ids.append(cache_id)

            # Retrieve by hash (should get latest)
            result = _.retrieve_by_hash(cache_hash, self.test_namespace)

            assert result is not None
            assert result['data']      == test_data
            assert result['data_type'] == 'json'

    def test_retrieve_by_id__not_found(self):                              # Test retrieval of non-existent ID
        with self.cache_service as _:
            non_existent = Random_Guid()
            result = _.retrieve_by_id(non_existent, self.test_namespace)

            assert result is None

    def test_delete_by_id(self):                                           # Test deletion by ID
        with self.cache_service as _:
            # Store test data
            test_data = {"delete": "test"}
            cache_hash = _.hash_from_json(test_data)
            cache_id = Random_Guid()

            _.store_with_strategy(storage_data = test_data,
                                 cache_hash   = cache_hash,
                                 cache_id     = cache_id,
                                 strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                 namespace    = self.test_namespace)

            # Delete it
            result = _.delete_by_id(cache_id, self.test_namespace)

            assert result['status']        == 'success'
            assert result['cache_id']      == str(cache_id)
            assert result['deleted_count'] > 0
            assert result['failed_count']  == 0

            # Verify it's gone
            retrieve_result = _.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieve_result is None

    def test_delete_by_id__not_found(self):                                # Test deletion of non-existent ID
        with self.cache_service as _:
            non_existent = Random_Guid()
            result = _.delete_by_id(non_existent, self.test_namespace)

            assert result['status']  == 'not_found'
            assert result['message'] == f"Cache ID {non_existent} not found"

    def test_hash_functions(self):                                         # Test hash generation functions
        with self.cache_service as _:
            # String hash
            string_data = "test string"
            hash1 = _.hash_from_string(string_data)
            assert hash1 is not None
            assert len(hash1) == 16                                        # Default length

            # Bytes hash
            bytes_data = b"test bytes"
            hash2 = _.hash_from_bytes(bytes_data)
            assert hash2 is not None

            # JSON hash
            json_data = {"key": "value", "number": 123}
            hash3 = _.hash_from_json(json_data)
            assert hash3 is not None

            # Same data should produce same hash
            hash3_again = _.hash_from_json(json_data)
            assert hash3 == hash3_again

    def test_determine_data_type(self):                                    # Test data type determination
        with self.cache_service as _:
            assert _.determine_data_type(b"bytes")    == "binary"
            assert _.determine_data_type({"dict": 1}) == "json"
            assert _.determine_data_type([1, 2, 3])   == "json"
            assert _.determine_data_type("string")    == "string"
            assert _.determine_data_type(123)         == "string"         # Numbers become strings

    def test_get_namespace__file_counts(self):                             # Test file counting
        with self.cache_service as _:
            # Store some test data
            for i in range(3):
                data = {"item": i}
                hash_val = _.hash_from_json(data)
                cache_id = Random_Guid()
                _.store_with_strategy(storage_data = data,
                                     cache_hash   = hash_val,
                                     cache_id     = cache_id,
                                     strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                     namespace    = self.test_namespace)
                self.created_ids.append(cache_id)

            # Get counts
            counts = _.get_namespace__file_counts(self.test_namespace)

            assert counts['namespace']    == str(self.test_namespace)
            assert counts['storage_mode'] == 'memory'
            assert counts['total_files']  > 0
            assert 'file_counts' in counts
            assert 'direct_files' in counts['file_counts']

    def test_retrieve_by_id__refs(self):                                 # Test retrieving configuration
        with self.cache_service as _:
            # Store test data
            test_data = {"config": "test"}
            cache_hash = _.hash_from_json(test_data)
            cache_id = Random_Guid()

            _.store_with_strategy(storage_data = test_data,
                                 cache_hash   = cache_hash,
                                 cache_id     = cache_id,
                                 strategy     = Enum__Cache__Store__Strategy.TEMPORAL,
                                 namespace    = self.test_namespace)

            self.created_ids.append(cache_id)

            # Get id refs
            refs = _.retrieve_by_id__refs(cache_id, self.test_namespace)

            assert type(refs) is Schema__Cache__File__Refs
            assert refs.obj() == __(cache_id        = cache_id      ,
                                    cache_hash      = cache_hash    ,
                                    namespace       = 'test-service',
                                    strategy        = 'temporal',
                                    all_paths       =__(data    = [f'test-service/data/temporal/{self.path_now}/{cache_id}.json'            ,
                                                                   f'test-service/data/temporal/{self.path_now}/{cache_id}.json.config'     ,
                                                                   f'test-service/data/temporal/{self.path_now}/{cache_id}.json.metadata'   ],
                                                       by_hash  = [ 'test-service/refs/by-hash/9e/db/9edb6ed62ec59b6c.json'                 ,
                                                                    'test-service/refs/by-hash/9e/db/9edb6ed62ec59b6c.json.config'          ,
                                                                    'test-service/refs/by-hash/9e/db/9edb6ed62ec59b6c.json.metadata'        ],
                                                       by_id     = __SKIP__                                                                 ),
                                   file_paths       = __( content_files = [f'test-service/data/temporal/{self.path_now}/{cache_id}.json'],
                                                          data_folders  = [f'test-service/data/temporal/{self.path_now}/{cache_id}/data']),
                                   file_type        = 'json',
                                   timestamp        = __SKIP__)

    def test_multiple_versions_same_hash(self):                            # Test multiple versions with same hash
        with self.cache_service as _:
            test_data = {"version": "test"}
            cache_hash = _.hash_from_json(test_data)

            # Store multiple versions
            cache_id1 = Random_Guid()
            cache_id2 = Random_Guid()

            _.store_with_strategy(storage_data = test_data,
                                 cache_hash   = cache_hash,
                                 cache_id     = cache_id1,
                                 strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                 namespace    = self.test_namespace)

            _.store_with_strategy(storage_data = test_data,
                                 cache_hash   = cache_hash,
                                 cache_id     = cache_id2,
                                 strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                 namespace    = self.test_namespace)

            self.created_ids.extend([cache_id1, cache_id2])

            # Retrieve by hash should get latest
            result = _.retrieve_by_hash(cache_hash, self.test_namespace)
            assert result is not None
            assert result['metadata']['cache_id'] == str(cache_id2)       # Latest version

    def test_binary_storage(self):                                         # Test binary data storage
        with self.cache_service as _:
            binary_data = b'\x00\x01\x02\x03\x04'
            cache_hash = _.hash_from_bytes(binary_data)
            cache_id = Random_Guid()

            result = _.store_with_strategy(storage_data = binary_data,
                                           cache_hash   = cache_hash,
                                           cache_id     = cache_id,
                                           strategy     = Enum__Cache__Store__Strategy.DIRECT,
                                           namespace    = self.test_namespace)

            self.created_ids.append(cache_id)

            # Retrieve and verify
            retrieved = _.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieved['data']      == binary_data
            assert retrieved['data_type'] == 'binary'

    def test_get_namespace__file_hashes__isolation(self):               # Test that file hashes are properly isolated by namespace
        with self.cache_service as _:
            ns1 = Safe_Str__Id("namespace1")
            ns2 = Safe_Str__Id("namespace2")

            # Store in namespace1
            data1 = {"ns": 1}
            hash1 = _.hash_from_json(data1)
            _.store_with_strategy(storage_data = data1   ,
                                  cache_hash   = hash1   ,
                                  namespace    = ns1     ,
                                  strategy     = "direct")

            # Store in namespace2
            data2 = {"ns": 2}
            hash2 = _.hash_from_json(data2)
            _.store_with_strategy(storage_data = data2   ,
                                  cache_hash   = hash2   ,
                                  namespace    = ns2     ,
                                  strategy     = "direct")

            # Get hashes from each namespace
            hashes_ns1 = _.get_namespace__file_hashes(ns1)
            hashes_ns2 = _.get_namespace__file_hashes(ns2)

            # Verify isolation
            assert str(hash1) in hashes_ns1
            assert str(hash1) not in hashes_ns2
            assert str(hash2) not in hashes_ns1
            assert str(hash2) in hashes_ns2