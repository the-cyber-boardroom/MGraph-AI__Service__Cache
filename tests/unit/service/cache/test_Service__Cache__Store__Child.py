from unittest                                                                                 import TestCase
import pytest
from osbot_fast_api_serverless.utils.testing.skip_tests                                       import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                                   import __
from osbot_utils.type_safe.Type_Safe                                                          import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                         import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id               import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path             import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text                  import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash      import Safe_Str__Cache_Hash
from osbot_utils.utils.Objects                                                                import base_classes
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy                 import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response                     import Schema__Cache__Store__Response
from mgraph_ai_service_cache.service.cache.Cache__Service                                     import Cache__Service
from mgraph_ai_service_cache.service.cache.Service__Cache__Retrieve import Service__Cache__Retrieve
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                        import Cache__Service__Store
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store__Data                  import Cache__Service__Store__Data
from tests.unit.Service__Cache__Test_Objs                                                     import setup__service__cache__test_objs

from osbot_utils.utils.Dev import pprint

class test_Cache__Service__Store__Data(TestCase):

    @classmethod
    def setUpClass(cls):
        pytest.skip("needs refactoring of Cache__Service__Store__Child (namely how data files are stored)")
        cls.test_objs          = setup__service__cache__test_objs()                              # Reuse shared test objects
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.store_service      = Cache__Service__Store      (cache_service = cls.cache_service)
        cls.retrieve_service   = Service__Cache__Retrieve   (cache_service = cls.cache_service)
        cls.store_data_service = Cache__Service__Store__Data(cache_service = cls.cache_service)

        cls.test_namespace   = Safe_Str__Id("test-child-service")                              # Test data setup
        cls.test_cache_key   = Safe_Str__File__Path("logs/application")
        cls.test_string      = "test child string data"
        cls.test_json        = {"child": "data", "count": 42}
        cls.test_binary      = b"child binary data \x00\x01\x02"

        # Create parent file for testing
        cls.parent_response   = cls.store_service.store_string(data      = "parent data"                          ,
                                                               namespace = cls.test_namespace                      ,
                                                               strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                               cache_key = cls.test_cache_key                     ,
                                                               file_id   = Safe_Str__Id("parent-001")             )
        cls.parent_file_id   = cls.parent_response.cache_id

    def test__init__(self):                                                                     # Test auto-initialization
        with Cache__Service__Store__Data() as _:
            assert type(_) is Cache__Service__Store__Data
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

            assert _.obj() == __(cache_service = __(cache_config     = __(storage_mode     = 'memory',
                                                                          default_bucket    = None    ,
                                                                          default_ttl_hours = 24      ,
                                                                          local_disk_path   = None    ,
                                                                          sqlite_path       = None    ,
                                                                          zip_path          = None    ),
                                                    cache_handlers   = __()                                   ,
                                                    hash_config      = __(algorithm = 'sha256', length = 16) ,
                                                    hash_generator   = __(config = __(algorithm = 'sha256', length = 16))))

    def test_store_child__string(self):                                                        # Test string child storage
        with self.retrieve_service as _:
            #_.retrieve_by_id          (cache_id=self.parent_file_id, namespace=self.test_namespace).print()
            _.retrieve_by_id__config  (cache_id=self.parent_file_id, namespace=self.test_namespace).print()
            #_.retrieve_by_id__metadata(cache_id=self.parent_file_id, namespace=self.test_namespace).print()
            _.retrieve_by_id__refs    (cache_id=self.parent_file_id, namespace=self.test_namespace).print()

        with self.store_data_service as _:
            #self.parent_response.print()
            return
            pprint(_.cache_service.retrieve_by_id__config(self.parent_file_id, namespace=self.test_namespace))

            result = _.store_child(data      = self.test_string                               ,
                                  data_type = Safe_Str__Text('string')                        ,
                                  namespace = self.test_namespace                             ,
                                  strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE      ,
                                  cache_key = self.test_cache_key                             ,
                                  file_id   = self.parent_file_id                             ,
                                  child_id  = Safe_Str__Id("child-001")                       )
            assert type(result) is Schema__Cache__Store__Response
            result.print()
            return

            assert type(result)          is Schema__Cache__Store__Response
            assert type(result.cache_id) is Random_Guid
            assert type(result.hash)     is Safe_Str__Cache_Hash
            assert result.namespace      == self.test_namespace
            assert result.size           == len(self.test_string)

            # Verify paths structure for child
            paths = result.paths
            assert "child"          in paths
            assert "parent_file_id" in paths
            assert "child_id"       in paths
            assert paths["parent_file_id"] == str(self.parent_file_id)
            assert paths["child_id"]       == "child-001"

            self.created_children.append(Safe_Str__Id("child-001"))

    def test_store_child__json(self):                                                          # Test JSON child storage
        with self.child_service as _:
            result = _.store_child(data      = self.test_json                                ,
                                  data_type = Safe_Str__Text('json')                         ,
                                  namespace = self.test_namespace                            ,
                                  strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE     ,
                                  cache_key = self.test_cache_key                            ,
                                  file_id   = self.parent_file_id                            ,
                                  child_id  = Safe_Str__Id("child-json-001")                 )

            assert type(result)          is Schema__Cache__Store__Response
            assert type(result.cache_id) is Random_Guid
            assert result.size           > 0

            # Hash should be deterministic for same data
            expected_hash = _.cache_service.hash_from_json(self.test_json)
            assert result.cache_hash == expected_hash

    def test_store_child__binary(self):                                                        # Test binary child storage
        with self.child_service as _:
            result = _.store_child(data      = self.test_binary                              ,
                                  data_type = Safe_Str__Text('binary')                       ,
                                  namespace = self.test_namespace                            ,
                                  strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE     ,
                                  cache_key = self.test_cache_key                            ,
                                  file_id   = self.parent_file_id                            ,
                                  child_id  = Safe_Str__Id("child-bin-001")                  )

            assert type(result)          is Schema__Cache__Store__Response
            assert result.size           == len(self.test_binary)

            # Binary data should maintain exact size
            assert result.size == len(self.test_binary)

    def test_store_child__auto_generated_id(self):                                             # Test auto-generation of child_id
        with self.child_service as _:
            result = _.store_child(data      = self.test_string                              ,
                                  data_type = Safe_Str__Text('string')                       ,
                                  namespace = self.test_namespace                            ,
                                  strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE     ,
                                  cache_key = self.test_cache_key                            ,
                                  file_id   = self.parent_file_id                            ,
                                  child_id  = None                                           )  # No child_id provided

            assert type(result)          is Schema__Cache__Store__Response
            assert type(result.cache_id) is Random_Guid

            # Child ID should be auto-generated
            paths = result.paths
            assert "child_id" in paths
            assert paths["child_id"] != ""
            assert type(Safe_Str__Id(paths["child_id"])) is Safe_Str__Id

            self.created_children.append(Safe_Str__Id(paths["child_id"]))

    def test_store_child__missing_file_id(self):                                               # Test error handling for missing file_id
        with self.child_service as _:
            try:
                result = _.store_child(data      = self.test_string                          ,
                                      data_type = Safe_Str__Text('string')                   ,
                                      namespace = self.test_namespace                        ,
                                      strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE ,
                                      cache_key = self.test_cache_key                        ,
                                      file_id   = None                                       )  # Missing file_id
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "file_id is required for child storage"

    def test_store_child__missing_cache_key(self):                                             # Test error handling for missing cache_key
        with self.child_service as _:
            try:
                result = _.store_child(data      = self.test_string                          ,
                                      data_type = Safe_Str__Text('string')                   ,
                                      namespace = self.test_namespace                        ,
                                      strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE ,
                                      cache_key = None                                        ,  # Missing cache_key
                                      file_id   = self.parent_file_id                        )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "cache_key is required for child storage"

    def test_store_child__invalid_data_type(self):                                             # Test handling of unknown data type
        with self.child_service as _:
            try:
                result = _.store_child(data      = "some data"                               ,
                                      data_type = Safe_Str__Text('invalid')                  ,  # Invalid type
                                      namespace = self.test_namespace                        ,
                                      strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE ,
                                      cache_key = self.test_cache_key                        ,
                                      file_id   = self.parent_file_id                        )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "Unknown data type: invalid" in str(e)

    def test__get_extension_for_type(self):                                                    # Test file extension mapping
        with self.child_service as _:
            assert _._get_extension_for_type(Safe_Str__Text('string')) == 'txt'
            assert _._get_extension_for_type(Safe_Str__Text('json'))   == 'json'
            assert _._get_extension_for_type(Safe_Str__Text('binary')) == 'bin'
            assert _._get_extension_for_type(Safe_Str__Text('other'))  == 'data'              # Default

    def test__serialize_data(self):                                                            # Test data serialization
        with self.child_service as _:
            # String serialization
            string_bytes = _._serialize_data("test string", Safe_Str__Text('string'))
            assert string_bytes == b"test string"

            # JSON serialization
            json_data = {"key": "value"}
            json_bytes = _._serialize_data(json_data, Safe_Str__Text('json'))
            assert type(json_bytes) is bytes
            assert b'"key"' in json_bytes

            # Binary serialization
            binary_data = b"raw bytes"
            binary_bytes = _._serialize_data(binary_data, Safe_Str__Text('binary'))
            assert binary_bytes == binary_data

            # Invalid binary data
            try:
                _._serialize_data("not bytes", Safe_Str__Text('binary'))
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "Binary data must be bytes" in str(e)

    def test_retrieve_children(self):                                                          # Test retrieving list of children
        skip__if_not__in_github_actions()
        with self.child_service as _:
            # Store multiple children
            for i in range(3):
                child_id = Safe_Str__Id(f"list-child-{i:03d}")
                _.store_child(data      = f"child data {i}"                                  ,
                             data_type = Safe_Str__Text('string')                            ,
                             namespace = self.test_namespace                                 ,
                             strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE          ,
                             cache_key = self.test_cache_key                                 ,
                             file_id   = self.parent_file_id                                 ,
                             child_id  = child_id                                            )
                self.created_children.append(child_id)

            # Retrieve children list
            children = _.retrieve_children(namespace = self.test_namespace                   ,
                                          strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                          cache_key = self.test_cache_key                    ,
                                          file_id   = self.parent_file_id                    )

            assert type(children) is list
            assert len(children) >= 3                                                          # At least our 3 children

    def test_store_child__multiple_to_same_parent(self):                                       # Test multiple children for same parent
        skip__if_not__in_github_actions()
        with self.child_service as _:
            results = []

            for i in range(5):
                child_id = Safe_Str__Id(f"multi-child-{i:03d}")
                result = _.store_child(data      = f"child {i} data"                         ,
                                      data_type = Safe_Str__Text('string')                   ,
                                      namespace = self.test_namespace                        ,
                                      strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE ,
                                      cache_key = self.test_cache_key                        ,
                                      file_id   = self.parent_file_id                        ,
                                      child_id  = child_id                                   )
                results.append(result)
                self.created_children.append(child_id)

            # All should succeed
            assert len(results) == 5
            for result in results:
                assert type(result) is Schema__Cache__Store__Response

            # All should have same parent
            for result in results:
                assert result.paths["parent_file_id"] == str(self.parent_file_id)

    def test_store_child__no_config_or_metadata(self):                                         # Verify no config/metadata files
        skip__if_not__in_github_actions()
        with self.child_service as _:
            child_id = Safe_Str__Id("no-meta-child")
            result = _.store_child(data      = "test no metadata"                            ,
                                  data_type = Safe_Str__Text('string')                       ,
                                  namespace = self.test_namespace                            ,
                                  strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE     ,
                                  cache_key = self.test_cache_key                            ,
                                  file_id   = self.parent_file_id                            ,
                                  child_id  = child_id                                       )

            self.created_children.append(child_id)

            # Child path should only contain data file, no config/metadata
            paths = result.paths
            assert "child" in paths
            child_paths = paths["child"]
            assert len(child_paths) == 1                                                       # Only data file
            assert ".txt" in child_paths[0]                                                    # String extension
            assert ".config" not in child_paths[0]
            assert ".metadata" not in child_paths[0]

    def test_store_child__different_strategies(self):                                          # Test with different storage strategies
        skip__if_not__in_github_actions()
        strategies = [Enum__Cache__Store__Strategy.DIRECT          ,
                     Enum__Cache__Store__Strategy.TEMPORAL         ,
                     Enum__Cache__Store__Strategy.SEMANTIC_FILE    ]

        with self.child_service as _:
            for strategy in strategies:
                with self.subTest(strategy=strategy):
                    child_id = Safe_Str__Id(f"strat-child-{strategy.value}")
                    result = _.store_child(data      = f"data for {strategy.value}"          ,
                                          data_type = Safe_Str__Text('string')               ,
                                          namespace = Safe_Str__Id(f"ns-{strategy.value}")   ,
                                          strategy  = strategy                               ,
                                          cache_key = self.test_cache_key                    ,
                                          file_id   = Safe_Str__Id(f"parent-{strategy.value}"),
                                          child_id  = child_id                               )

                    assert type(result)          is Schema__Cache__Store__Response
                    assert result.namespace      == f"ns-{strategy.value}"
                    self.created_children.append(child_id)

    def test_store_child__performance(self):                                                   # Test lightweight nature of child storage
        skip__if_not__in_github_actions()
        import time

        with self.child_service as _:
            # Time parent storage
            start_parent = time.time()
            parent_result = self.store_service.store_string(data      = "parent performance test"                  ,
                                                           namespace = self.test_namespace                        ,
                                                           strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE ,
                                                           cache_key = Safe_Str__File__Path("perf/test")          ,
                                                           file_id   = Safe_Str__Id("perf-parent")                )
            parent_time = time.time() - start_parent

            # Time child storage
            start_child = time.time()
            child_result = _.store_child(data      = "child performance test"                ,
                                        data_type = Safe_Str__Text('string')                 ,
                                        namespace = self.test_namespace                      ,
                                        strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                        cache_key = Safe_Str__File__Path("perf/test")        ,
                                        file_id   = Safe_Str__Id("perf-parent")              ,
                                        child_id  = Safe_Str__Id("perf-child")               )
            child_time = time.time() - start_child

            self.created_children.append(Safe_Str__Id("perf-child"))

            # Child should be faster (no config/metadata overhead)
            assert child_time < parent_time * 2                                              # Allow some variance

            # Both should succeed
            assert type(parent_result) is Schema__Cache__Store__Response
            assert type(child_result)  is Schema__Cache__Store__Response