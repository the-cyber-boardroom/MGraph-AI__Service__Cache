import gzip
import json
import pytest
from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text        import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                   import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__Dict               import Type_Safe__Dict
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import random_string_short, str_to_base64
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_aws.AWS_Config                                                           import aws_config
from memory_fs.schemas.Safe_Str__Cache_Hash                                         import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service, DEFAULT__CACHE__SERVICE__BUCKET_NAME, DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler
from mgraph_ai_service_cache.service.cache.Cache__Hash__Config                      import Cache__Hash__Config
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator                   import Cache__Hash__Generator
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from mgraph_ai_service_cache.service.storage.Storage_FS__S3                         import Storage_FS__S3
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs


class test_Cache__Service(TestCase):                                                # Test main cache service orchestrator with all APIs

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        cls.test_objs   = setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-service-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        with Cache__Service() as _:
            cls.service          = _
            _.default_bucket     = cls.test_bucket
            _.default_ttl_hours  = 12

        # Test data shared across tests
        cls.test_namespace       = Safe_Str__Id("test-namespace")
        cls.test_data_string     = "test cache data"
        cls.test_data_json       = {"key": "value", "number": 123, "nested": {"inner": "data"}}
        cls.test_data__base_64   = str_to_base64(json_to_str(cls.test_data_json))
        cls.test_metadata        = {Safe_Str__Id("author" ): Safe_Str__Text("test-user"),
                                    Safe_Str__Id("version"): Safe_Str__Text("1.0")}
        cls.test_tags            = [Safe_Str__Id("test"), Safe_Str__Id("cache")]
        cls.path_now             = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    @classmethod
    def tearDownClass(cls):                                                             # ONE-TIME cleanup
        # Clean up all created handlers
        for handler in cls.service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)                         # todo: review this delete workflow

        # Delete test bucket
        if cls.service.cache_handlers:
            handler = list(cls.service.cache_handlers.values())[0]
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete(cls.test_bucket)                                   # todo: since this should be all we need (delete the test bucket)

    def test__init__(self):                                                          # Test auto-initialization
        with Cache__Service() as _:
            assert type(_)                is Cache__Service
            assert base_classes(_)        == [Type_Safe, object]
            assert type(_.cache_handlers) is Type_Safe__Dict                         # New API uses regular dict
            assert _.default_bucket       == DEFAULT__CACHE__SERVICE__BUCKET_NAME
            assert _.default_ttl_hours    == DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
            assert _.default_bucket       == "mgraph-ai-cache"                      # Default bucket
            assert _.default_ttl_hours    == 24                                     # Default TTL
            assert type(_.hash_config)    is Cache__Hash__Config
            assert type(_.hash_generator) is Cache__Hash__Generator

    def test_setup(self):                                                            # Test service setup initializes components
        with self.service as _:
            assert type(_.hash_config)    is Cache__Hash__Config
            assert type(_.hash_generator) is Cache__Hash__Generator
            assert _.hash_config.length   == 16                                     # Default hash length
            assert _.default_bucket       == self.test_bucket
            assert _.default_ttl_hours    == 12


    def test_get_or_create_handler(self):                                           # Test handler creation and retrieval
        namespace = Safe_Str__Id("handler-test")

        with self.service as _:
            # First call creates new handler
            handler1 = _.get_or_create_handler(namespace)
            assert type(handler1)        is Cache__Handler
            assert handler1.s3__bucket      == self.test_bucket
            assert handler1.s3__prefix      == str(namespace)
            assert handler1.cache_ttl_hours == 12

            # Second call retrieves same instance
            handler2 = _.get_or_create_handler(namespace)
            assert handler2 is handler1

            # Verify it's in the cache
            assert namespace in _.cache_handlers

    def test_store_with_strategy(self):                                                 # Test storing data with strategy tracking

        cache_hash           = self.service.hash_from_string(self.test_data_string)
        # response__cache_hash = self.service.retrieve_by_hash(cache_hash=cache_hash, namespace=self.test_namespace)
        # if response__cache_hash:                                                        # this happens when all tests are executed in this class
        #     cache_id_to_delete = response__cache_hash["metadata"]['cache_id']
        #     response__delete   = self.service.delete_by_id(cache_id=cache_id_to_delete, namespace=self.test_namespace)
        #     assert response__delete['status'] == 'success'

        cache_id   = Random_Guid()
        cache_id__path_1 = cache_id[0:2]
        cache_id__path_2 = cache_id[2:4]

        with self.service as _:
            response = _.store_with_strategy(storage_data   = self.test_data_string,
                                             cache_hash     = cache_hash,
                                             cache_id       = cache_id,
                                             strategy       = "temporal",
                                             namespace      = self.test_namespace)

            response__cache_hash = self.service.retrieve_by_hash(cache_hash=cache_hash, namespace=self.test_namespace)

            assert type(response)        is Schema__Cache__Store__Response
            assert response.cache_id     == cache_id
            assert response.hash         == cache_hash
            assert type(response.paths)  is Type_Safe__Dict                             # Now returns structured paths
            assert response.paths        == {'by_hash': [ Safe_Str__File__Path( 'refs/by-hash/1e/2f/1e2ff1555748f789.json'                                  ),
                                                          #Safe_Str__File__Path( 'refs/by-hash/1e/2f/1e2ff1555748f789.json.config'                           ),         # todo: find a better way to test this (this happens when all tests are executed)
                                                          Safe_Str__File__Path( 'refs/by-hash/1e/2f/1e2ff1555748f789.json.metadata'                         )],
                                             'by_id'  : [ Safe_Str__File__Path(f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json'          ),
                                                          Safe_Str__File__Path(f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.config'   ),
                                                          Safe_Str__File__Path(f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.metadata' )],
                                             'data'   : [ Safe_Str__File__Path(f'data/temporal/{self.path_now}/{cache_id}.json'                             ),
                                                          Safe_Str__File__Path(f'data/temporal/{self.path_now}/{cache_id}.json.config'                      ),
                                                          Safe_Str__File__Path(f'data/temporal/{self.path_now}/{cache_id}.json.metadata'                    )]}
            assert response.paths        == {'by_hash': [  'refs/by-hash/1e/2f/1e2ff1555748f789.json'                                   ,   # confirm the assert also works without the Safe_Str__File__Path
                                                           #'refs/by-hash/1e/2f/1e2ff1555748f789.json.config'                            ,
                                                           'refs/by-hash/1e/2f/1e2ff1555748f789.json.metadata'                          ],
                                             'by_id'  : [ f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json'           ,
                                                          f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.config'    ,
                                                          f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.metadata'  ],
                                             'data'   : [ f'data/temporal/{self.path_now}/{cache_id}.json'                              ,
                                                          f'data/temporal/{self.path_now}/{cache_id}.json.config'                       ,
                                                          f'data/temporal/{self.path_now}/{cache_id}.json.metadata'                     ]}


    def test_store_with_strategy__all_strategies(self):                                # Test all storage strategies
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                test_data = f"data for {strategy}"
                cache_hash = self.service.hash_from_string(test_data)
                cache_id   = Random_Guid()

                with self.service as _:
                    response = _.store_with_strategy(storage_data   = test_data,
                                                     cache_hash     = cache_hash,
                                                     cache_id       = cache_id,
                                                     strategy       = strategy,
                                                     namespace      = Safe_Str__Id(f"ns-{strategy}"))

                    assert response.cache_id == cache_id
                    assert response.hash     == cache_hash
                    assert len(response.paths["data"]) >= 3                            # At least 3 files (json, config, metadata)

    def test_retrieve_by_hash(self):                                                   # Test retrieval by hash works correctly
        # Store data first
        test_data = "retrieve by hash test"
        cache_hash = self.service.hash_from_string(test_data)
        cache_id   = Random_Guid()

        with self.service as _:
            _.store_with_strategy(storage_data   = test_data,
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            # Retrieve by hash
            result    = _.retrieve_by_hash(cache_hash, self.test_namespace)
            stored_at = result.get('metadata').get('stored_at')
            assert result == { 'content_encoding': None,
                               'data'            : 'retrieve by hash test',
                               'data_type'       : 'string',
                               'metadata': {'cache_hash'      : 'ef11cf6a121a582a'  ,
                                            'cache_id'        : cache_id            ,
                                            'cache_key'       : 'None'              ,
                                            'file_id'         : cache_id            ,
                                            'content_encoding': None                ,
                                            'file_type'       : 'json'              ,
                                            'namespace'       : 'test-namespace'    ,
                                            'stored_at'       : stored_at           ,
                                            'strategy'        : 'temporal'          }}
            assert result is not None
            assert "data" in result
            assert result["data"] == test_data
            assert "metadata" in result
            assert result["metadata"]["cache_hash"] == str(cache_hash)

    def test_retrieve_by_hash__not_found(self):                                        # Test retrieval of non-existent hash
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")

        with self.service as _:
            result = _.retrieve_by_hash(non_existent_hash, self.test_namespace)
            assert result is None

    def test_retrieve_by_id(self):                                                     # Test retrieval by cache ID
        test_data     = "retrieve by id test"
        cache_hash    = self.service.hash_from_string(test_data)
        cache_id      = Random_Guid()
        #cache_hash    = '042347b98515ab7f'
        deleted_paths = [f'data/direct/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                         f'data/direct/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                         f'data/direct/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata',
                         f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json',
                         f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config',
                         f'refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata',
                         f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                         f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                         f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata']
        with self.service as _:
            _.store_with_strategy(storage_data   = test_data,
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id,
                                  strategy       = "direct",
                                  namespace      = self.test_namespace)

            result__retrieve = _.retrieve_by_id(cache_id, self.test_namespace)          # Retrieve by ID
            result__delete   = _.delete_by_id(cache_id, self.test_namespace)            # Delete by ID

            assert result__retrieve == { 'content_encoding': None,
                                         'data'            : 'retrieve by id test',
                                         'data_type'       : 'string',
                                         'metadata'        : { 'cache_hash'      : '042347b98515ab7f',
                                                               'cache_id'        : cache_id          ,
                                                               'cache_key'       : 'None'            ,
                                                               'file_id'         : cache_id          ,
                                                               'content_encoding': None              ,
                                                               'file_type'       : 'json'            ,
                                                               'namespace'       : 'test-namespace'  ,
                                                               'stored_at'       : result__retrieve['metadata']['stored_at'],
                                                               'strategy'        : 'direct'}}

            assert result__retrieve                           is not None
            assert result__retrieve["data"]                   == test_data
            assert result__retrieve["metadata"]["cache_id"]   == str(cache_id)
            assert result__delete                             == { 'cache_id'     : cache_id     ,
                                                                   'deleted_count': 9            ,
                                                                   'deleted_paths': deleted_paths,
                                                                   'failed_count' : 0            ,
                                                                   'failed_paths' : []           ,
                                                                   'status'       : 'success'    }

    def test_delete_by_id(self):                                                       # Test deletion of cache entry by ID
        test_data = "data to delete"
        cache_hash = self.service.hash_from_string(test_data)
        cache_id   = Random_Guid()

        with self.service as _:
            # Store data
            store_response = _.store_with_strategy(storage_data   = test_data,
                                                   cache_hash     = cache_hash,
                                                   cache_id       = cache_id,
                                                   strategy       = "temporal",
                                                   namespace      = self.test_namespace)

            # Verify it exists
            assert _.retrieve_by_id(cache_id, self.test_namespace) is not None

            # Delete it
            delete_result = _.delete_by_id(cache_id, self.test_namespace)

            assert delete_result["status"] == "success"
            assert delete_result["cache_id"] == str(cache_id)
            assert delete_result["deleted_count"] > 0
            assert delete_result["failed_count"] == 0

            # Verify it's gone
            assert _.retrieve_by_id(cache_id, self.test_namespace) is None

    def test_delete_by_id__not_found(self):                                           # Test deletion of non-existent ID
        non_existent_id = Random_Guid()

        with self.service as _:
            result = _.delete_by_id(non_existent_id, self.test_namespace)

            assert result["status"] == "not_found"
            assert f"Cache ID {non_existent_id} not found" in result["message"]

    def test_delete_by_id__with_multiple_versions(self):                              # Test deletion when hash has multiple versions
        test_data = "shared data"
        cache_hash = self.service.hash_from_string(test_data)
        cache_id_1 = Random_Guid()
        cache_id_2 = Random_Guid()

        with self.service as _:
            # Store same data twice (creates versions)
            _.store_with_strategy(storage_data   = test_data,
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id_1,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            _.store_with_strategy(storage_data   = test_data,
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id_2,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            # Delete first version
            delete_result = _.delete_by_id(cache_id_1, self.test_namespace)
            assert delete_result["status"] == "success"

            # Second version should still exist
            assert _.retrieve_by_id(cache_id_2, self.test_namespace) is not None

            # Hash should still resolve to second version
            result = _.retrieve_by_hash(cache_hash, self.test_namespace)
            assert result is not None
            assert result["metadata"]["cache_id"] == str(cache_id_2)

    def test_hash_from_string(self):                                                  # Test hash generation from string
        with self.service as _:
            hash_value = _.hash_from_string(self.test_data_string)

            assert type(hash_value) is Safe_Str__Cache_Hash
            assert len(str(hash_value)) == 16                                         # Default length

            # Same string produces same hash
            hash_value_2 = _.hash_from_string(self.test_data_string)
            assert hash_value == hash_value_2

    def test_hash_from_bytes(self):                                                   # Test hash generation from bytes
        test_bytes = b"test binary data"

        with self.service as _:
            hash_value = _.hash_from_bytes(test_bytes)

            assert type(hash_value) is Safe_Str__Cache_Hash
            assert len(str(hash_value)) == 16

    def test_hash_from_json(self):                                                    # Test hash generation from JSON
        with self.service as _:
            hash_value = _.hash_from_json(self.test_data_json)

            assert type(hash_value) is Safe_Str__Cache_Hash

            # Order doesn't matter
            reordered = {"number": 123, "key": "value", "nested": {"inner": "data"}}
            hash_value_2 = _.hash_from_json(reordered)
            assert hash_value == hash_value_2

    def test_hash_from_json__with_exclusions(self):                                   # Test JSON hash with field exclusion
        data_with_timestamp = {"data": "value", "timestamp": "2024-01-01", "id": "123"}

        with self.service as _:
            hash_full = _.hash_from_json(data_with_timestamp)
            hash_no_timestamp = _.hash_from_json(data_with_timestamp, exclude_fields=["timestamp"])
            hash_minimal = _.hash_from_json(data_with_timestamp, exclude_fields=["timestamp", "id"])

            assert hash_full         != hash_no_timestamp
            assert hash_no_timestamp != hash_minimal
            assert hash_full         != hash_minimal

    def test_list_namespaces(self):                                                   # Test listing active namespaces
        with self.service as _:
            # Create handlers for multiple namespaces
            _.get_or_create_handler(Safe_Str__Id("ns1"))
            _.get_or_create_handler(Safe_Str__Id("ns2"))
            _.get_or_create_handler(Safe_Str__Id("ns3"))

            namespaces = _.list_namespaces()

            assert type(namespaces) is list
            assert Safe_Str__Id("ns1") in namespaces
            assert Safe_Str__Id("ns2") in namespaces
            assert Safe_Str__Id("ns3") in namespaces

    def test__path_tracking_in_id_reference(self):                                    # Test that ID reference contains all paths
        test_data = "path tracking test"
        cache_hash = self.service.hash_from_string(test_data)
        cache_id   = Random_Guid()
        all_paths = {'by_hash': [ 'refs/by-hash/c7/03/c703026d3a59fe62.json',
                                  'refs/by-hash/c7/03/c703026d3a59fe62.json.config',
                                  'refs/by-hash/c7/03/c703026d3a59fe62.json.metadata'],
                     'by_id'  : [f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                                 f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                                 f'refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata'],
                     'data'   : [f'data/temporal/{self.path_now}/{cache_id}.json',
                                 f'data/temporal/{self.path_now}/{cache_id}.json.config',
                                 f'data/temporal/{self.path_now}/{cache_id}.json.metadata']}
        content_paths = [f'data/temporal/{self.path_now}/{cache_id}.json']
        with self.service as _:
            # Store data
            _.store_with_strategy(storage_data   = test_data,
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            # Read the ID reference directly
            handler = _.get_or_create_handler(self.test_namespace)
            with handler.fs__refs_id.file__json(Safe_Str__Id(str(cache_id))) as ref_fs:
                id_ref_data = ref_fs.content()
                all_paths = id_ref_data["all_paths"]
                assert id_ref_data == { 'all_paths'     : all_paths              ,
                                        'cache_id'      : cache_id               ,
                                        'content_paths' : content_paths          ,
                                        'file_type'     : 'json'                 ,
                                        'hash'          : 'c703026d3a59fe62'     ,
                                        'namespace'     : 'test-namespace'       ,
                                        'strategy'      : 'temporal'             ,
                                        'timestamp'     : id_ref_data['timestamp']}

    def test_storage_fs(self):
        with self.service.storage_fs() as _:
            assert type(_) is Storage_FS__S3
            assert _.s3_bucket == self.service.default_bucket

    def test__retrieve_by_hash(self):                                               # Test retrieval by hash
        # Store data first
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = Random_Guid()
        with self.service as _:
            result__store  = _.store_with_strategy(storage_data   = self.test_data_string,
                                                   cache_hash     = cache_hash           ,
                                                   cache_id       = cache_id             ,
                                                   strategy       = "temporal"           ,
                                                   namespace      = self.test_namespace  )
            # Retrieve by hash
            result__retrieve = _.retrieve_by_hash(cache_hash, self.test_namespace)
            assert result__retrieve                         is not None
            assert result__retrieve["data"    ]             == self.test_data_string
            assert result__retrieve["metadata"]['cache_id'] == cache_id
            assert result__retrieve["metadata"]['cache_id'] == result__store.cache_id
            assert result__retrieve["metadata"]['file_id' ] == cache_id                             # in this case they match
            assert _.delete_by_id(cache_id=cache_id, namespace=self.test_namespace).get('deleted_count') == 9


    def test_retrieve_by_id__not_found(self):                                      # Test retrieval of non-existent ID
        non_existent_id = Random_Guid()

        with self.service as _:
            result = _.retrieve_by_id(non_existent_id, self.test_namespace)
            assert result is None


    def test_retrieve__selective_fields(self):                                     # Test selective field retrieval
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = Random_Guid()

        with self.service as _:
            # Store data
            _.store_with_strategy(storage_data   = self.test_data_json,  # Store JSON data
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            # Retrieve with different options
            result_full = _.retrieve_by_id(cache_id, self.test_namespace)
            stored_at   = result_full.get('metadata').get('stored_at')
            assert result_full is not None
            assert type(result_full ) is dict
            assert type(stored_at   ) is int
            assert result_full == { 'content_encoding': None                                         ,
                                    'data'            : self.test_data_json                          ,
                                    'data_type'       : 'json'                                       ,
                                    'metadata'        : { 'cache_hash'      : '1e2ff1555748f789'      ,
                                                          'cache_id'        : cache_id                ,
                                                          'cache_key'       : 'None'                  ,
                                                          'content_encoding': None                    ,
                                                          'file_id'         : cache_id                ,
                                                          'file_type'       : 'json'                  ,
                                                          'namespace'       : self.test_namespace     ,
                                                          'stored_at'       : stored_at               ,
                                                          'strategy'        : 'temporal'              }}


    def test__multiple_namespaces_isolated(self):                                  # Test namespace isolation
        ns1 = Safe_Str__Id("namespace1")
        ns2 = Safe_Str__Id("namespace2")

        data1_string = "namespace 1 data"
        data2_string = "namespace 2 data"

        with self.service as _:
            # Store in different namespaces
            hash1 = _.hash_from_string(data1_string)
            hash2 = _.hash_from_string(data2_string)
            id1   = Random_Guid()
            id2   = Random_Guid()

            _.store_with_strategy(storage_data   = data1_string ,
                                  cache_hash     = hash1        ,
                                  cache_id       = id1          ,
                                  strategy       = "direct"     ,
                                  namespace      = ns1          )

            _.store_with_strategy(storage_data   = data2_string,
                                  cache_hash     = hash2    ,
                                  cache_id       = id2      ,
                                  strategy       = "direct" ,
                                  namespace      = ns2      )

            # Retrieve from each namespace
            result1 = _.retrieve_by_id(id1, ns1)
            result2 = _.retrieve_by_id(id2, ns2)

            assert result1["data"] == data1_string
            assert result2["data"] == data2_string

            # Cross-namespace retrieve fails
            result_cross = _.retrieve_by_id(id1, ns2)
            assert result_cross is None

    def test__complex_data_storage(self):                                          # Test storing complex nested data
        complex_data = {
            "users": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"}
            ],
            "settings": {
                "theme": "dark",
                "lang": "en-US",
                "nested": {"deep": {"value": 42}}
            },
            "binary": "base64encodeddata=="
        }

        with self.service as _:
            # Generate hash from complex data
            cache_hash = _.hash_from_json(complex_data)
            cache_id   = Random_Guid()

            # Store complex data
            response = _.store_with_strategy(storage_data   = complex_data,
                                             cache_hash     = cache_hash,
                                             cache_id       = cache_id,
                                             strategy       = "temporal_latest",
                                             namespace      = self.test_namespace)

            assert response.cache_id == cache_id

            # Retrieve and verify
            result = _.retrieve_by_id(cache_id, self.test_namespace)
            assert result is not None
            assert result["data"] == complex_data

    def test__store_with_provided_hash(self):                                      # Test storing with custom hash
        custom_hash = Safe_Str__Cache_Hash("abc1234567890abcd")  # 16 chars to match config
        cache_id    = Random_Guid()

        with self.service as _:
            response = _.store_with_strategy(
                storage_data   = self.test_data_string,
                cache_hash     = custom_hash,
                cache_id       = cache_id,
                strategy       = "temporal",
                namespace      = self.test_namespace
            )

            assert response.hash == custom_hash
            assert response.cache_id == cache_id

    def test__hash_consistency_across_types(self):                                 # Test hash consistency
        test_string = "test data"
        test_bytes  = test_string.encode('utf-8')
        test_json   = {"data": test_string}

        with self.service as _:
            # String and bytes of same content should produce same hash
            hash_string = _.hash_from_string(test_string)
            hash_bytes  = _.hash_from_bytes(test_bytes)
            assert hash_string == hash_bytes

            # JSON should produce different hash
            hash_json = _.hash_from_json(test_json)
            assert hash_json != hash_string

    def test__concurrent_operations(self):                                         # Test concurrent operations on same namespace
        with self.service as _:
            cache_ids = []

            # Store multiple items
            for i in range(5):
                data = f"item_{i}"
                hash_val = _.hash_from_string(data)
                cache_id = Random_Guid()
                cache_ids.append(cache_id)

                _.store_with_strategy(
                    storage_data   = data,
                    cache_hash     = hash_val,
                    cache_id       = cache_id,
                    strategy       = "direct",
                    namespace      = self.test_namespace
                )

            # Retrieve all items
            for i, cache_id in enumerate(cache_ids):
                result = _.retrieve_by_id(cache_id, self.test_namespace)
                assert result["data"] == f"item_{i}"

    def test__error_handling__invalid_strategy(self):                              # Test error handling for invalid strategy
        with self.service as _:
            cache_hash = _.hash_from_string(self.test_data_string)
            cache_id   = Random_Guid()

            # Should raise error or handle gracefully
            with pytest.raises(ValueError, match="Unknown strategy"):
                _.store_with_strategy(
                    storage_data   = self.test_data_string,
                    cache_hash     = cache_hash,
                    cache_id       = cache_id,
                    strategy       = "invalid_strategy",
                    namespace      = self.test_namespace)



    def test_store_and_retrieve_binary_data(self):                                      # Test binary data storage and retrieval
        binary_data = b'\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR' + b'\x00' * 100            # Create test binary data (fake PNG header)

        with self.service as _:
            cache_hash = _.hash_from_bytes(binary_data)
            cache_id   = Random_Guid()
            cache_id_path = f"{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}"
            #cache_hash = '667eea27e9ab1776'
            # Store binary data
            response = _.store_with_strategy(storage_data   = binary_data      ,
                                             cache_hash     = cache_hash       ,
                                             cache_id       = cache_id         ,
                                             strategy       = "direct"         ,
                                             namespace      = self.test_namespace)
            assert response.json()   == {'cache_id' : cache_id,
                                         'hash'     : cache_hash,
                                         'namespace': 'test-namespace',
                                         'paths'    : {'by_hash': [ 'refs/by-hash/66/7e/667eea27e9ab1776.json',
                                                                    'refs/by-hash/66/7e/667eea27e9ab1776.json.config',
                                                                    'refs/by-hash/66/7e/667eea27e9ab1776.json.metadata'],
                                                       'by_id'  : [ f'refs/by-id/{cache_id_path}.json',
                                                                    f'refs/by-id/{cache_id_path}.json.config',
                                                                    f'refs/by-id/{cache_id_path}.json.metadata'],
                                                       'data'   : [ f'data/direct/{cache_id_path}.bin',
                                                                    f'data/direct/{cache_id_path}.bin.config',
                                                                    f'data/direct/{cache_id_path}.bin.metadata']},
                                         'size'     : 116   }
            assert response.cache_id == cache_id
            assert response.hash     == cache_hash
            assert response.size     > 100                                              # Binary data has size

            # Retrieve and verify
            result = _.retrieve_by_id(cache_id, self.test_namespace)

            assert result is not None
            assert result["data_type"]       == "binary"                                # Correctly identified as binary
            assert result["data"]            == binary_data                             # Data preserved exactly
            assert result["content_encoding"] is None                                   # No compression
            assert type(result["data"])       is bytes                                   # Remains as bytes

    def test_store_and_retrieve_compressed_binary(self):                                # Test compressed binary storage
        # Create and compress test data
        original_data   = b"This is test data that will be compressed" * 100            # Make it large enough to compress well
        compressed_data = gzip.compress(original_data)

        with self.service as _:
            # Hash should be of the original data
            cache_hash = _.hash_from_bytes(original_data)
            cache_id   = Random_Guid()

            # Store compressed data with encoding flag
            response = _.store_with_strategy(storage_data     = compressed_data       ,
                                             cache_hash       = cache_hash             ,
                                             cache_id         = cache_id               ,
                                             strategy         = "temporal"             ,
                                             namespace        = self.test_namespace    ,
                                             content_encoding = "gzip"                 )

            assert response.cache_id == cache_id
            assert response.size     < len(original_data)                               # Compressed size is smaller

            # Retrieve - should auto-decompress
            result    = _.retrieve_by_id(cache_id, self.test_namespace)
            stored_at = result.get('metadata').get('stored_at')

            assert result == {'content_encoding': 'gzip'                              ,
                             'data'            : original_data                        ,  # Automatically decompressed
                             'data_type'       : 'binary'                             ,
                             'metadata'        : { 'cache_hash'      : str(cache_hash)  ,
                                                   'cache_id'        : str(cache_id)    ,
                                                   'cache_key'       : 'None'           ,
                                                   'content_encoding': 'gzip'           ,
                                                   'file_type'       : 'binary'         ,
                                                   'file_id'         : cache_id         ,
                                                   'namespace'       : 'test-namespace' ,
                                                   'stored_at'       : stored_at        ,
                                                   'strategy'        : 'temporal'       }}

    def test_store_and_retrieve_compressed_json(self):                                                      # Test compressed JSON storage
        json_data = {"users": [{"id": i, "name": f"User_{i}", "data": "x" * 100} for i in range(50)]}       # Create large JSON data
        json_string     = json.dumps(json_data)
        compressed_data = gzip.compress(json_string.encode())

        with self.service as _:
            cache_hash = _.hash_from_json(json_data)
            cache_id   = Random_Guid()

            # Store compressed JSON
            response = _.store_with_strategy(storage_data     = compressed_data       ,
                                             cache_hash       = cache_hash             ,
                                             cache_id         = cache_id               ,
                                             strategy         = "temporal_latest"      ,
                                             namespace        = self.test_namespace    ,
                                             content_encoding = "gzip"                 )

            assert response.cache_id == cache_id

            # Retrieve - should auto-decompress and parse as JSON
            result = _.retrieve_by_id(cache_id, self.test_namespace)

            assert result["content_encoding"] == "gzip"
            assert result["data_type"]        == "json"                                 # Recognized as JSON after decompression
            assert result["data"]             == json_data                              # Fully reconstructed JSON
            assert type(result["data"])       is dict                                   # Parsed as dict
            assert len(result["data"]["users"]) == 50                                   # All data preserved

    def test_retrieve_with_data_type_detection(self):                                   # Test data type detection logic
        test_cases = [
            ("string_data", "test string", "string"),
            ("json_data", {"key": "value", "num": 123}, "json"),
            ("binary_data", b"\x00\x01\x02\x03", "binary"),
            ("list_data", ["item1", "item2"], "json"),                                  # Lists are JSON
        ]

        for description, data, expected_type in test_cases:
            with self.subTest(description):
                cache_id = Random_Guid()

                # Determine cache_hash based on data type
                if isinstance(data, bytes):
                    cache_hash = self.service.hash_from_bytes(data)
                elif isinstance(data, str):
                    cache_hash = self.service.hash_from_string(data)
                else:
                    cache_hash = self.service.hash_from_json(data)

                with self.service as _:
                    _.store_with_strategy(storage_data   = data                         ,
                                          cache_hash     = cache_hash                   ,
                                          cache_id       = cache_id                     ,
                                          strategy       = "direct"                     ,
                                          namespace      = self.test_namespace          )

                    result = _.retrieve_by_id(cache_id, self.test_namespace)

                    assert result["data_type"] == expected_type
                    assert result["data"]       == data

    def test_retrieve_handles_missing_content_encoding(self):                          # Test backward compatibility
        # Store data without content_encoding (simulating old data)
        test_data  = "backward compatible data"
        cache_hash = self.service.hash_from_string(test_data)
        cache_id   = Random_Guid()

        with self.service as _:
            _.store_with_strategy(storage_data   = test_data                          ,
                                  cache_hash     = cache_hash                         ,
                                  cache_id       = cache_id                           ,
                                  strategy       = "temporal"                         ,
                                  namespace      = self.test_namespace                )

            # Retrieve should handle missing content_encoding gracefully
            result = _.retrieve_by_id(cache_id, self.test_namespace)

            assert result["content_encoding"] is None
            assert result["data_type"]        == "string"
            assert result["data"]             == test_data

    def test_retrieve_by_hash_with_binary_data(self):                                  # Test hash retrieval with binary
        binary_data = b"Binary test data for hash retrieval"

        with self.service as _:
            cache_hash = _.hash_from_bytes(binary_data)
            cache_id   = Random_Guid()

            # Store binary data
            _.store_with_strategy(storage_data   = binary_data                        ,
                                  cache_hash     = cache_hash                         ,
                                  cache_id       = cache_id                           ,
                                  strategy       = "temporal_latest"                  ,
                                  namespace      = self.test_namespace                )

            # Retrieve by hash
            result = _.retrieve_by_hash(cache_hash, self.test_namespace)

            assert result is not None
            assert result["data_type"] == "binary"
            assert result["data"]      == binary_data
            assert type(result["data"]) is bytes

    def test_multiple_versions_with_different_encodings(self):                         # Test versioning with mixed encodings
        base_data = {"version": 1, "data": "test"}
        cache_hash = self.service.hash_from_json(base_data)

        with self.service as _:
            # Store v1: uncompressed
            cache_id_v1 = Random_Guid()
            _.store_with_strategy(storage_data   = base_data                          ,
                                  cache_hash     = cache_hash                         ,
                                  cache_id       = cache_id_v1                        ,
                                  strategy       = "temporal_versioned"               ,
                                  namespace      = self.test_namespace                )

            # Store v2: compressed (same hash, different storage)
            cache_id_v2     = Random_Guid()
            compressed_data = gzip.compress(json_to_str(base_data).encode())
            _.store_with_strategy(storage_data     = compressed_data                  ,
                                  cache_hash       = cache_hash                       ,
                                  cache_id         = cache_id_v2                      ,
                                  strategy         = "temporal_versioned"             ,
                                  namespace        = self.test_namespace              ,
                                  content_encoding = "gzip"                           )

            # Retrieve by hash should get latest (v2, compressed)
            result_latest = _.retrieve_by_hash(cache_hash, self.test_namespace)
            assert result_latest["data"]             == base_data
            assert result_latest["content_encoding"] == "gzip"
            assert result_latest["data_type"]        == "json"

            # Direct retrieval of v1 (uncompressed)
            result_v1 = _.retrieve_by_id(cache_id_v1, self.test_namespace)
            assert result_v1["data"]             == base_data
            assert result_v1["content_encoding"] is None
            assert result_v1["data_type"]        == "json"


    def test_binary_data_with_all_strategies(self):                                    # Test binary across all strategies
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]
        binary_data = b"Test binary \x00\x01\x02 data"

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                cache_hash = self.service.hash_from_bytes(binary_data)
                cache_id   = Random_Guid()
                namespace  = Safe_Str__Id(f"binary-{strategy}")

                with self.service as _:
                    # Store binary data with strategy
                    response = _.store_with_strategy(storage_data   = binary_data     ,
                                                     cache_hash     = cache_hash      ,
                                                     cache_id       = cache_id        ,
                                                     strategy       = strategy        ,
                                                     namespace      = namespace       )

                    assert response.cache_id == cache_id

                    # Retrieve and verify
                    result = _.retrieve_by_id(cache_id, namespace)
                    assert result["data"]      == binary_data
                    assert result["data_type"] == "binary"

                    # Clean up
                    _.delete_by_id(cache_id, namespace)

    def test_edge_cases_in_data_type_detection(self):                                  # Test edge cases for type detection
        edge_cases = [("empty_string"   , "", "string"),
                      ("empty_dict"     , {}, "json"),
                      ("empty_list"     , [], "json"),
                      ("null_bytes"     , b"\x00", "binary"),
                      ("unicode_string" , "Hello 世界 🌍", "string"),
                      ("nested_json"    , {"a": {"b": {"c": "d"}}}, "json"),
                      ("large_binary"   , b"x" * 10000, "binary")]

        for description, data, expected_type in edge_cases:
            with self.subTest(description):
                if isinstance(data, bytes):
                    cache_hash = self.service.hash_from_bytes(data)
                elif isinstance(data, str):
                    cache_hash = self.service.hash_from_string(data)
                else:
                    cache_hash = self.service.hash_from_json(data)

                cache_id = Random_Guid()

                with self.service as _:
                    _.store_with_strategy(storage_data   = data                        ,
                                          cache_hash     = cache_hash                  ,
                                          cache_id       = cache_id                    ,
                                          strategy       = "direct"                    ,
                                          namespace      = self.test_namespace         )

                    result = _.retrieve_by_id(cache_id, self.test_namespace)

                    assert result["data_type"] == expected_type
                    assert result["data"]       == data

                    # Clean up
                    _.delete_by_id(cache_id, self.test_namespace)