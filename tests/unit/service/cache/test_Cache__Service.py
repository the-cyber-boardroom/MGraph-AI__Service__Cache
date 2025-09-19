import gzip
import pytest
from unittest                                                                       import TestCase
from osbot_utils.helpers.duration.decorators.capture_duration                       import capture_duration
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text        import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__Dict               import Type_Safe__Dict
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_aws.AWS_Config                                                           import aws_config
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service, DEFAULT__CACHE__SERVICE__BUCKET_NAME, DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler
from mgraph_ai_service_cache.service.cache.Cache__Hash__Config                      import Cache__Hash__Config
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator                   import Cache__Hash__Generator
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs


class test_Cache__Service(TestCase):                                                # Test main cache service orchestrator with all APIs

    @classmethod
    def setUpClass(cls):
        with capture_duration() as duration:
            cls.test_objs          = setup__service_fast_api_test_objs()                # Reuse shared test objects
            cls.cache_fixtures     = cls.test_objs.cache_fixtures                       # Use shared fixtures


            cls.test_bucket = cls.cache_fixtures.fixtures_bucket                        # Use the fixtures bucket for everything


            with Cache__Service() as _:                                                 # Service that uses the fixtures bucket
                cls.service          = _
                _.default_bucket     = cls.test_bucket                                  # Use fixtures bucket
                _.default_ttl_hours  = 12

            cls.created_cache_ids = []                                                  # Track IDs we create for cleanup
            cls.test_namespace    = Safe_Str__Id("test-cache-service")                  # Different namespace from fixtures

            assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
            assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

            cls.test_data_string     = cls.cache_fixtures.get_fixture_data('string_simple'  )        # Test data from fixtures
            cls.test_data_json       = cls.cache_fixtures.get_fixture_data('json_simple'    )
            cls.test_data_binary     = cls.cache_fixtures.get_fixture_data('binary_small'   )
            cls.test_metadata        = { Safe_Str__Id("author" ): Safe_Str__Text("test-user"),
                                         Safe_Str__Id("version"): Safe_Str__Text("1.0")     }
            cls.test_tags            = [Safe_Str__Id("test"), Safe_Str__Id("cache")]
            cls.path_now             = Path__Handler__Temporal().path_now()

            cls.fixture_hash_string  = cls.cache_fixtures.get_fixture_hash('string_simple'  )       # Pre-calculated hashes for fixtures
            cls.fixture_hash_json    = cls.cache_fixtures.get_fixture_hash('json_simple'    )
            cls.fixture_hash_binary  = cls.cache_fixtures.get_fixture_hash('binary_small'   )

            cls.fixture_id_string    = cls.cache_fixtures.get_fixture_id('string_simple'    )
            cls.fixture_id_json      = cls.cache_fixtures.get_fixture_id('json_simple'      )
            cls.fixture_id_binary    = cls.cache_fixtures.get_fixture_id('binary_small'     )

            cls.fixtures_namespace   = cls.cache_fixtures.namespace
        assert duration.seconds < 1                                                                 # start up cost is about 400ms (on dev laptop)

    @classmethod
    def tearDownClass(cls):                                                         # Clean up only what we created
        with capture_duration() as duration:
            for cache_id in cls.created_cache_ids:                                      # Delete all cache entries we created during tests
                try:
                    cls.service.delete_by_id(cache_id, cls.test_namespace)
                except:
                    pass  # Ignore if already deleted

            # Clean up any test namespaces we created (not the fixtures namespace)
            test_namespaces = ["handler-test", "namespace1", "namespace2", "test-cache-service",
                              "binary-direct", "binary-temporal", "binary-temporal_latest", "binary-temporal_versioned"]

            for ns in test_namespaces:
                namespace = Safe_Str__Id(ns)
                if namespace in cls.service.cache_handlers:
                    handler = cls.service.cache_handlers[namespace]
                    # Clean up any files in this namespace
                    try:
                        with handler.s3__storage as storage:
                            prefix = str(namespace)
                            files = storage.s3.find_files(bucket=cls.test_bucket, prefix=prefix)
                            if files:
                                storage.s3.files_delete(bucket=cls.test_bucket, keys=files)
                    except:
                        pass
        assert duration.seconds < 0.5           # todo: check this when all tests are running

    def _track_cache_id(self, cache_id):                                           # Helper to track IDs for cleanup
        if cache_id not in self.created_cache_ids:
            self.created_cache_ids.append(cache_id)
        return cache_id

    def test__init__(self):                                                         # Test auto-initialization
        with Cache__Service() as _:
            assert type(_)                is Cache__Service
            assert base_classes(_)        == [Type_Safe, object]
            assert type(_.cache_handlers) is Type_Safe__Dict
            assert _.default_bucket       == DEFAULT__CACHE__SERVICE__BUCKET_NAME
            assert _.default_ttl_hours    == DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS

    def test_setup(self):                                                            # Test service setup initializes components
        with self.service as _:
            assert type(_.hash_config)    is Cache__Hash__Config
            assert type(_.hash_generator) is Cache__Hash__Generator
            assert _.hash_config.length   == 16
            assert _.default_bucket       == self.test_bucket
            assert _.default_ttl_hours    == 12

    def test_get_or_create_handler(self):                                           # Test handler creation and retrieval
        namespace = Safe_Str__Id("handler-test")

        with self.service as _:
            handler1 = _.get_or_create_handler(namespace)
            assert type(handler1)        is Cache__Handler
            assert handler1.s3__bucket      == self.test_bucket
            assert handler1.s3__prefix      == str(namespace)
            assert handler1.cache_ttl_hours == 12

            handler2 = _.get_or_create_handler(namespace)
            assert handler2 is handler1
            assert namespace in _.cache_handlers

    def test_store_with_strategy(self):                                             # Test storing data with strategy tracking
        cache_hash = self.service.hash_from_string(self.test_data_string)
        cache_id   = self._track_cache_id(Random_Guid())                            # Track for cleanup

        with self.service as _:
            response = _.store_with_strategy(storage_data   = self.test_data_string,
                                             cache_hash     = cache_hash,
                                             cache_id       = cache_id,
                                             strategy       = "temporal",
                                             namespace      = self.test_namespace)

            assert type(response)        is Schema__Cache__Store__Response
            assert response.cache_id     == cache_id
            assert response.hash         == cache_hash

    def test_retrieve_by_hash__using_fixtures(self):                                            #
        with self.service as _:
            result = _.retrieve_by_hash(self.fixture_hash_string, self.fixtures_namespace)      # Retrieve existing fixture (no need to store)

            assert result is not None
            assert result["data"] == self.test_data_string
            assert result["metadata"]["cache_hash"] == self.fixture_hash_string

    def test_retrieve_by_id__using_fixtures(self):
        with self.service as _:
            result = _.retrieve_by_id(self.fixture_id_string, self.fixtures_namespace)          # Retrieve existing fixture

            assert result is not None
            assert result["data"] == self.test_data_string
            assert result["metadata"]["cache_id"] == str(self.fixture_id_string)

    def test_delete_by_id(self):                                                    # Test deletion with new data
        test_data = "data to delete - not a fixture"
        cache_hash = self.service.hash_from_string(test_data)
        cache_id   = Random_Guid()                                                  # Don't track this one - we're deleting it

        with self.service as _:
            _.store_with_strategy(storage_data   = test_data,
                                  cache_hash     = cache_hash,
                                  cache_id       = cache_id,
                                  strategy       = "temporal",
                                  namespace      = self.test_namespace)

            assert _.retrieve_by_id(cache_id, self.test_namespace) is not None

            delete_result = _.delete_by_id(cache_id, self.test_namespace)
            assert delete_result["status"] == "success"
            assert _.retrieve_by_id(cache_id, self.test_namespace) is None

    def test_hash_consistency(self):                                                # Test hash calculation matches fixtures
        with self.service as _:
            # Calculate hash for fixture data
            hash_string = _.hash_from_string(self.test_data_string)
            hash_json   = _.hash_from_json(self.test_data_json)
            hash_binary = _.hash_from_bytes(self.test_data_binary)

            # Should match pre-calculated fixture hashes
            assert str(hash_string) == self.fixture_hash_string
            assert str(hash_json)   == self.fixture_hash_json
            assert str(hash_binary) == self.fixture_hash_binary

    def test_binary_fixture_operations(self):                                       # Use binary fixture
        with self.service as _:
            result = _.retrieve_by_id(self.fixture_id_binary, self.fixtures_namespace)      # Retrieve existing binary fixture

            assert result is not None
            assert result["data_type"] == "binary"
            assert result["data"] == self.test_data_binary

    def test_compression(self):                                                     # Test compression with new data
        original_data   = b"This is test data that will be compressed" * 100
        compressed_data = gzip.compress(original_data)

        with self.service as _:
            cache_hash = _.hash_from_bytes(original_data)
            cache_id   = self._track_cache_id(Random_Guid())                        # Track for cleanup

            response = _.store_with_strategy(storage_data     = compressed_data,
                                             cache_hash       = cache_hash,
                                             cache_id         = cache_id,
                                             strategy         = "temporal",
                                             namespace        = self.test_namespace,
                                             content_encoding = "gzip")

            assert response.cache_id == cache_id

            result = _.retrieve_by_id(cache_id, self.test_namespace)
            assert result["data"] == original_data
            assert result["content_encoding"] == "gzip"

    def test_namespace_isolation(self):                                             # Test namespace isolation
        ns1 = Safe_Str__Id("namespace1")
        ns2 = Safe_Str__Id("namespace2")

        with self.service as _:
            id1 = self._track_cache_id(Random_Guid())                               # Track both for cleanup
            id2 = self._track_cache_id(Random_Guid())

            _.store_with_strategy(storage_data   = self.test_data_string,
                                  cache_hash     = _.hash_from_string(self.test_data_string),
                                  cache_id       = id1,
                                  strategy       = "direct",
                                  namespace      = ns1)

            _.store_with_strategy(storage_data   = self.test_data_json,
                                  cache_hash     = _.hash_from_json(self.test_data_json),
                                  cache_id       = id2,
                                  strategy       = "direct",
                                  namespace      = ns2)

            # Verify isolation
            assert _.retrieve_by_id(id1, ns1) is not None
            assert _.retrieve_by_id(id2, ns2) is not None
            assert _.retrieve_by_id(id1, ns2) is None
            assert _.retrieve_by_id(id2, ns1) is None

            # Clean up
            _.delete_by_id(id1, ns1)
            _.delete_by_id(id2, ns2)

    def test_all_strategies(self):                                                  # Test all strategies with fixtures
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            with self.subTest(strategy=strategy):
                cache_id = self._track_cache_id(Random_Guid())                      # Track for cleanup

                with self.service as _:
                    response = _.store_with_strategy(
                        storage_data   = self.test_data_json,                       # Use fixture data
                        cache_hash     = _.hash_from_json(self.test_data_json),
                        cache_id       = cache_id,
                        strategy       = strategy,
                        namespace      = Safe_Str__Id(f"test-{strategy}"))

                    assert response.cache_id == cache_id

    def test_error_handling__invalid_strategy(self):                               # Test error condition
        with self.service as _:
            with pytest.raises(ValueError, match="Unknown strategy"):
                _.store_with_strategy(storage_data   = "test",
                                     cache_hash     = _.hash_from_string("test"),
                                     cache_id       = Random_Guid(),
                                     strategy       = "invalid_strategy",
                                     namespace      = self.test_namespace)

    def test_delete_by_id__not_found(self):                                        # Test delete non-existent
        with self.service as _:
            result = _.delete_by_id(Random_Guid(), self.test_namespace)
            assert result["status"] == "not_found"