import re
import pytest
from unittest                                                                       import TestCase
from datetime                                                                       import datetime
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Storage_Mode          import Enum__Cache__Storage_Mode
from mgraph_ai_service_cache.service.cache.Cache__Config                            import Cache__Config
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.utils.Misc                                                         import random_string_short
from osbot_utils.utils.Objects                                                      import base_classes, __
from memory_fs.Memory_FS                                                            import Memory_FS
from memory_fs.file_fs.File_FS                                                      import File_FS
from memory_fs.file_types.Memory_FS__File__Type__Json                               import Memory_FS__File__Type__Json
from memory_fs.file_types.Memory_FS__File__Type__Text                               import Memory_FS__File__Type__Text
from memory_fs.helpers.Memory_FS__Temporal                                          import Memory_FS__Temporal
from memory_fs.helpers.Memory_FS__Latest_Temporal                                   import Memory_FS__Latest_Temporal
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler, CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_LATEST, CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL, CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED, CACHE__HANDLER__PREFIX_PATH__FS__REFS_HASH, CACHE__HANDLER__PREFIX_PATH__FS__REFS_ID, CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT
from tests.unit.Service__Cache__Test_Objs                                           import setup__service__cache__test_objs


class test_Cache__Handler(TestCase):                                                # Test cache handler with multiple storage strategies

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        cls.test_objs   = setup__service__cache__test_objs()                       # Setup LocalStack
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-handler-"))
        cls.namespace = "test-cache"

        # todo: move this check to the integration tests (since for the Unit tests we are running in memory)
        #assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        #assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.config          = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY)  # Use memory for fast tests
        cls.storage_backend = cls.config.create_storage_backend()       # todo: review the need to make this call (since we should be handling this in a tranparent way)
        with Cache__Handler() as _:
            cls.handler       = _
            _.storage_backend = cls.storage_backend
            _.namespace       = cls.namespace
            _.cache_ttl_hours = 12
            _.setup()

            # Test data shared across tests
            cls.test_file_id = Safe_Str__Id("test-document")
            cls.test_data    = {"content": "test data", "version": 1}

            # Get current temporal path for validation
            cls.path_now = _.fs__data_temporal.handler__temporal.path_now()


    def test__init__(self):                                                          # Test initialization and defaults
        with Cache__Handler() as _:
            assert type(_)                         is Cache__Handler
            assert base_classes(_)                 == [Type_Safe, object]

            # Test default values
            assert _.cache_ttl_hours               == 24                            # Default TTL

            # Test uninitialized state
            assert _.fs__data_direct               is None
            assert _.fs__data_temporal             is None
            assert _.fs__data_temporal_latest      is None
            assert _.fs__data_temporal_versioned   is None
            assert _.fs__refs_hash                 is None
            assert _.fs__refs_id                   is None

    def test_setup(self):                                                           # Test setup creates all required components
        with self.handler as _:
            # Verify all Memory_FS instances created
            assert type(_.fs__data_direct)             is Memory_FS
            assert type(_.fs__data_temporal)           is Memory_FS__Temporal
            assert type(_.fs__data_temporal_latest)    is Memory_FS__Latest_Temporal
            assert type(_.fs__data_temporal_versioned) is Memory_FS
            assert type(_.fs__refs_hash)               is Memory_FS
            assert type(_.fs__refs_id)                 is Memory_FS

            # Verify custom TTL
            assert _.cache_ttl_hours                  == 12

    def test_setup__path_configurations(self):                                      # Test path handler configurations
        with self.handler as _:
            # Test direct storage path configuration
            direct_handler = _.fs__data_direct.path_handlers[0]
            assert direct_handler.prefix_path         == Safe_Str__File__Path(f'{self.namespace}/data/direct')
            assert direct_handler.shard_depth         == 2

            # Test temporal path configuration
            temporal_handler = _.fs__data_temporal.handler__temporal
            assert temporal_handler.prefix_path       == Safe_Str__File__Path(f'{self.namespace}/data/temporal')

            # Test temporal-latest path configuration
            assert _.fs__data_temporal_latest.handler__latest.prefix_path == Safe_Str__File__Path(f'{self.namespace}/data/temporal-latest')

            # Test reference stores configuration
            refs_hash_handler = _.fs__refs_hash.path_handlers[0]
            assert refs_hash_handler.prefix_path      == Safe_Str__File__Path(f'{self.namespace}/refs/by-hash')
            assert refs_hash_handler.shard_depth      == 2

            refs_id_handler = _.fs__refs_id.path_handlers[0]
            assert refs_id_handler.prefix_path        == Safe_Str__File__Path(f'{self.namespace}/refs/by-id')
            assert refs_id_handler.shard_depth        == 2

    def test_get_fs_for_strategy(self):                                             # Test strategy selection
        with self.handler as _:
            assert _.get_fs_for_strategy("direct"            ) is _.fs__data_direct
            assert _.get_fs_for_strategy("temporal"          ) is _.fs__data_temporal
            assert _.get_fs_for_strategy("temporal_latest"   ) is _.fs__data_temporal_latest
            assert _.get_fs_for_strategy("temporal_versioned") is _.fs__data_temporal_versioned

    def test_get_fs_for_strategy__invalid(self):                                    # Test invalid strategy error
        with self.handler as _:
            error_message = "Unknown strategy: invalid"
            with pytest.raises(ValueError, match=re.escape(error_message)):
                _.get_fs_for_strategy("invalid")

    def test_get_fs_for_strategy__edge_cases(self):                                 # Test edge cases for strategy selection
        with self.handler as _:
            # Test None strategy
            error_message_none = "Unknown strategy: None"
            with pytest.raises(ValueError, match=re.escape(error_message_none)):
                _.get_fs_for_strategy(None)

            # Test empty string
            error_message_empty = "Unknown strategy: "
            with pytest.raises(ValueError, match=re.escape(error_message_empty)):
                _.get_fs_for_strategy("")

    # Tests for direct strategy operations
    def test__direct_strategy__workflow(self):                                      # Test complete direct storage workflow
        with self.handler as _:
            fs = _.get_fs_for_strategy("direct")
            file_fs = fs.file__json(self.test_file_id)

            assert type(file_fs)                      is File_FS
            assert type(file_fs.file__config.file_type) is Memory_FS__File__Type__Json
            assert file_fs.file__config.file_id       == self.test_file_id

            # Create and save data
            assert file_fs.create()                   == [Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json'         ),
                                                          Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json.config'  ),
                                                          Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json.metadata')]
            assert file_fs.update(self.test_data)     == [Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json'         ),
                                                          Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json.metadata')]
            assert file_fs.exists()                   is True

            # Verify content
            assert file_fs.content()                  == self.test_data

            # Clean up
            assert file_fs.delete()                   == [Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json'         ),
                                                          Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json.config'  ),
                                                          Safe_Str__File__Path(f'{self.namespace}/data/direct/te/st/test-document.json.metadata')]
            assert file_fs.exists()                   is False

    # Tests for temporal strategy operations
    def test__temporal_strategy__workflow(self):                                    # Test complete temporal storage workflow
        with self.handler as _:
            fs            = _.get_fs_for_strategy("temporal")
            file_fs       = fs.file__json(self.test_file_id)
            prefix_path   = CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL
            files_created = [Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json'          ),
                             Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.config'   ),
                             Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.metadata')]
            files_updated = [Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json'          ),
                             Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.metadata')]

            assert type(file_fs)                      is File_FS
            assert file_fs.file__config.file_id       == self.test_file_id
            assert self.path_now in file_fs.file__config.file_paths[0]              # Should have temporal path

            # Create and save data
            assert file_fs.create()                   == files_created
            assert file_fs.update(self.test_data)     == files_updated
            assert file_fs.exists()                   is True

            # Verify content
            assert file_fs.content()                  == self.test_data

            # Clean up
            assert file_fs.delete()                   == files_created
            assert file_fs.exists()                   is False

    # Tests for temporal_latest strategy operations
    def test__temporal_latest_strategy__workflow(self):                             # Test combined temporal+latest workflow
        with self.handler as _:
            fs = _.get_fs_for_strategy("temporal_latest")
            file_fs = fs.file__json(self.test_file_id)

            assert type(file_fs)                      is File_FS
            assert len(file_fs.file__config.file_paths) == 2                        # Both latest and temporal

            # Verify both path types present
            paths = file_fs.file__config.file_paths
            assert any('latest' in str(p) for p in paths)                          # Has latest path
            assert any(str(datetime.now().year) in str(p) for p in paths)          # Has temporal path

            # Create and save data
            created_paths = file_fs.create()
            assert len(created_paths)                 > 0

            updated_paths = file_fs.update(self.test_data)
            assert len(updated_paths)                 > 0
            assert file_fs.exists()                   is True

            # Verify content
            assert file_fs.content()                  == self.test_data

            # Verify exact paths created (from old test)
            prefix_path    = CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_LATEST
            expected_paths = [Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json'          ),
                              Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.config'   ),
                              Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.metadata' ),
                              Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/latest/test-document.json'                   ),
                              Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/latest/test-document.json.config'            ),
                              Safe_Str__File__Path(f'{self.namespace}/{prefix_path}/latest/test-document.json.metadata'          )]

            # Clean up
            assert file_fs.delete()     == expected_paths
            assert file_fs.exists()     is False

    # Tests for temporal_versioned strategy operations
    def test__temporal_versioned_strategy__workflow(self):                          # Test temporal+latest+versioned workflow
        with self.handler as _:
            fs = _.get_fs_for_strategy("temporal_versioned")
            file_fs = fs.file__json(self.test_file_id)
            prefix_path   = CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL_VERSIONED
            files_created = [f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json'         ,
                             f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.config'  ,
                             f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.metadata',
                             f'{self.namespace}/{prefix_path}/latest/test-document.json'                  ,
                             f'{self.namespace}/{prefix_path}/latest/test-document.json.config'           ,
                             f'{self.namespace}/{prefix_path}/latest/test-document.json.metadata'         ,
                             f'{self.namespace}/{prefix_path}/versions/v1/test-document.json'             ,
                             f'{self.namespace}/{prefix_path}/versions/v1/test-document.json.config'      ,
                             f'{self.namespace}/{prefix_path}/versions/v1/test-document.json.metadata'    ]
            files_updated = [f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json'         ,
                             f'{self.namespace}/{prefix_path}/{self.path_now}/test-document.json.metadata',
                             f'{self.namespace}/{prefix_path}/latest/test-document.json'                  ,
                             f'{self.namespace}/{prefix_path}/latest/test-document.json.metadata'         ,
                             f'{self.namespace}/{prefix_path}/versions/v1/test-document.json'             ,
                             f'{self.namespace}/{prefix_path}/versions/v1/test-document.json.metadata'    ]
            assert type(file_fs)                      is File_FS
            # Should have multiple handlers configured
            assert len(fs.path_handlers)              >= 3                          # temporal, latest, versioned

            # Create and save data
            assert file_fs.create()                   == files_created
            assert file_fs.update(self.test_data)     == files_updated
            assert file_fs.exists()                   is True

            # Verify content
            assert file_fs.content()                  == self.test_data

            # Clean up
            assert file_fs.delete()                   == files_created
            assert file_fs.exists()                   is False

    # Tests for reference stores
    def test__reference_stores__hash(self):                                         # Test hash-based reference store
        with self.handler as _:
            test_hash = "abc123def456"
            test_id   = "doc-001"

            path_prefix   = CACHE__HANDLER__PREFIX_PATH__FS__REFS_HASH
            files_created = [f'{self.namespace}/{path_prefix}/ab/c1/abc123def456.json'         ,
                             f'{self.namespace}/{path_prefix}/ab/c1/abc123def456.json.config'  ,
                             f'{self.namespace}/{path_prefix}/ab/c1/abc123def456.json.metadata']
            files_updated = [f'{self.namespace}/{path_prefix}/ab/c1/abc123def456.json'         ,
                             f'{self.namespace}/{path_prefix}/ab/c1/abc123def456.json.metadata']

            # Save hash->id mapping
            file_fs = _.fs__refs_hash.file__json(test_hash)
            assert file_fs.create()                   == files_created
            assert file_fs.update({"id": test_id})    == files_updated

            # Verify retrieval
            assert file_fs.content()                  == {"id": test_id}

            # Clean up
            assert file_fs.delete()                   == files_created

    def test__reference_stores__id(self):                                           # Test id-based reference store
        with self.handler as _:
            test_id   = "doc-001"
            test_hash = "abc123def456"
            path_prefix   = CACHE__HANDLER__PREFIX_PATH__FS__REFS_ID
            files_created = [f'{self.namespace}/{path_prefix}/do/c-/doc-001.json'         ,
                             f'{self.namespace}/{path_prefix}/do/c-/doc-001.json.config'  ,
                             f'{self.namespace}/{path_prefix}/do/c-/doc-001.json.metadata']
            files_updated = [f'{self.namespace}/{path_prefix}/do/c-/doc-001.json'         ,
                             f'{self.namespace}/{path_prefix}/do/c-/doc-001.json.metadata']
            # Save id->hash mapping
            file_fs = _.fs__refs_id.file__json(test_id)
            assert file_fs.create()                          == files_created
            assert file_fs.update({"cache_hash": test_hash}) == files_updated

            # Verify retrieval
            assert file_fs.content()                  == {"cache_hash": test_hash}

            # Clean up
            assert file_fs.delete()                   == files_created

    # Test with custom file types
    def test__with_custom_file_type(self):                                          # Test using text file type instead of JSON
        with self.handler as _:
            fs          = _.get_fs_for_strategy("direct")
            file_fs     = fs.file__text(self.test_file_id)                              # Create file with text type
            path_prefix = CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT
            files_created = [f'{self.namespace}/{path_prefix}/te/st/test-document.txt'         ,
                             f'{self.namespace}/{path_prefix}/te/st/test-document.txt.config'  ,
                             f'{self.namespace}/{path_prefix}/te/st/test-document.txt.metadata']
            files_updated = [f'{self.namespace}/{path_prefix}/te/st/test-document.txt'         ,
                             f'{self.namespace}/{path_prefix}/te/st/test-document.txt.metadata']
            assert type(file_fs.file__config.file_type) is Memory_FS__File__Type__Text
            assert file_fs.file__config.file_type.file_extension == 'txt'

            # Test with text data
            text_data = "This is plain text content"
            assert file_fs.create()                   == files_created
            assert file_fs.update(text_data)          == files_updated
            assert file_fs.content()                  == text_data

            # Clean up
            assert file_fs.delete()                   == files_created

    # Test concurrent operations
    def test__concurrent_strategies(self):                                          # Test using multiple strategies simultaneously
        with self.handler as _:
            test_id = Safe_Str__Id("concurrent-test")
            data_v1 = {"version": 1, "data": "initial"}
            data_v2 = {"version": 2, "data": "updated"}
            path_prefix__direct     = CACHE__HANDLER__PREFIX_PATH__FS__DATA_DIRECT
            path_prefix__temporal   = CACHE__HANDLER__PREFIX_PATH__FS__DATA_TEMPORAL

            files_created__direct   = [f'{self.namespace}/{path_prefix__direct}/co/nc/concurrent-test.json'         ,
                                       f'{self.namespace}/{path_prefix__direct}/co/nc/concurrent-test.json.config'  ,
                                       f'{self.namespace}/{path_prefix__direct}/co/nc/concurrent-test.json.metadata']
            files_updated__direct   = [f'{self.namespace}/{path_prefix__direct}/co/nc/concurrent-test.json'         ,
                                       f'{self.namespace}/{path_prefix__direct}/co/nc/concurrent-test.json.metadata']

            files_created__temporal = [f'{self.namespace}/{path_prefix__temporal}/{self.path_now}/concurrent-test.json'         ,
                                       f'{self.namespace}/{path_prefix__temporal}/{self.path_now}/concurrent-test.json.config'  ,
                                       f'{self.namespace}/{path_prefix__temporal}/{self.path_now}/concurrent-test.json.metadata']
            files_updated__temporal = [f'{self.namespace}/{path_prefix__temporal}/{self.path_now}/concurrent-test.json'         ,
                                       f'{self.namespace}/{path_prefix__temporal}/{self.path_now}/concurrent-test.json.metadata']
            # Save to direct strategy
            direct_fs = _.get_fs_for_strategy("direct")
            direct_file = direct_fs.file__json(test_id)
            assert direct_file.create()               == files_created__direct
            assert direct_file.update(data_v1)        == files_updated__direct

            # Save different data to temporal strategy
            temporal_fs = _.get_fs_for_strategy("temporal")
            temporal_file = temporal_fs.file__json(test_id)
            assert temporal_file.create()             == files_created__temporal
            assert temporal_file.update(data_v2)      == files_updated__temporal

            # Verify each strategy has its own data
            assert direct_file.content()              == data_v1
            assert temporal_file.content()            == data_v2

            # Clean up
            assert direct_file.delete()               == files_created__direct
            assert temporal_file.delete()             == files_created__temporal

    # Test error conditions
    def test__file_operations__not_found(self):                                     # Test operations on non-existent files
        with self.handler as _:
            fs = _.get_fs_for_strategy("direct")
            file_fs = fs.file__json(Safe_Str__Id("non-existent"))

            assert file_fs.exists()                   is False
            assert file_fs.content()                  is None
            assert file_fs.delete()                   == []                      # Can't delete non-existent

    # Test serialization round-trip
    def test__serialization_round_trip(self):                                       # Test JSON serialization preserves types
        with self.handler as _:
            fs = _.get_fs_for_strategy("temporal_latest")

            # Complex data with various types
            complex_data = { "id"  : self.test_file_id  ,
                             "count": 42                ,
                             "price": 99.99             ,
                             "items": ["item1", "item2" ],
                             "metadata": {"key": "value"},
                             "active": True             }
            file_fs = fs.file__json(Safe_Str__Id("complex-test"))
            files_created = [f'{self.namespace}/data/temporal-latest/{self.path_now}/complex-test.json'          ,
                             f'{self.namespace}/data/temporal-latest/{self.path_now}/complex-test.json.config'   ,
                             f'{self.namespace}/data/temporal-latest/{self.path_now}/complex-test.json.metadata'    ,
                             f'{self.namespace}/data/temporal-latest/latest/complex-test.json'                    ,
                             f'{self.namespace}/data/temporal-latest/latest/complex-test.json.config'             ,
                             f'{self.namespace}/data/temporal-latest/latest/complex-test.json.metadata'           ]
            files_updated = [f'{self.namespace}/data/temporal-latest/{self.path_now}/complex-test.json'          ,
                             f'{self.namespace}/data/temporal-latest/{self.path_now}/complex-test.json.metadata'    ,
                             f'{self.namespace}/data/temporal-latest/latest/complex-test.json'                    ,
                             f'{self.namespace}/data/temporal-latest/latest/complex-test.json.metadata'           ]
            assert file_fs.create()                   == files_created
            assert file_fs.update(complex_data)       == files_updated

            # Verify round-trip
            retrieved = file_fs.content()
            assert retrieved                          == complex_data
            assert type(retrieved["count"])           is int
            assert type(retrieved["price"])           is float
            assert type(retrieved["items"])           is list
            assert type(retrieved["metadata"])        is dict
            assert type(retrieved["active"])          is bool

            # Clean up
            assert file_fs.delete()                   == files_created