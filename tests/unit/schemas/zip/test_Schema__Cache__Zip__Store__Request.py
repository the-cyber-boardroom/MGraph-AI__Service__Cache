import pytest
from unittest                                                                              import TestCase
from osbot_utils.testing.__                                                                import __
from osbot_utils.utils.Objects                                                             import base_classes
from osbot_utils.type_safe.Type_Safe                                                       import Type_Safe
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Request          import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                           import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy              import Enum__Cache__Store__Strategy
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path          import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id            import Safe_Str__Id


class test_Schema__Cache__Zip__Store__Request(TestCase):

    def test__init__(self):                                                               # Test auto-initialization
        with Schema__Cache__Zip__Store__Request() as _:
            assert type(_)         is Schema__Cache__Zip__Store__Request
            assert base_classes(_) == [Type_Safe, object]

            # Verify field types
            assert type(_.zip_bytes) is bytes
            assert _.cache_key       is None
            assert _.file_id         is None
            assert type(_.namespace) is Safe_Str__Id
            assert type(_.strategy)  is Enum__Cache__Store__Strategy

            # Verify defaults
            assert _.obj() == __(zip_bytes = b''                                  ,  # Empty bytes
                                 cache_key = None                                 ,  # Optional
                                 file_id   = None                                 ,  # Optional
                                 namespace = DEFAULT_CACHE__NAMESPACE             ,  # Has default
                                 strategy  = Enum__Cache__Store__Strategy.TEMPORAL)  # Has enum default

    def test__init__with_values(self):                                                    # Test initialization with data
        test_zip = b"PK\x03\x04"                                                          # Minimal zip header

        with Schema__Cache__Zip__Store__Request(zip_bytes = test_zip           ,
                                                cache_key = "backups/2024.zip" ,
                                                file_id   = "custom-id"        ,
                                                namespace = "test-ns"          ,
                                                strategy  = "direct"           ) as _:                                                    # String auto-converts to enum
            assert _.zip_bytes == test_zip
            assert _.cache_key == "backups/2024.zip"
            assert _.file_id   == "custom-id"
            assert _.namespace == "test-ns"
            assert _.strategy  == Enum__Cache__Store__Strategy.DIRECT                     # Converted to enum

    def test_type_conversions(self):                                                        # Test Type_Safe auto-conversions
        with Schema__Cache__Zip__Store__Request() as _:
            _.cache_key = "path/with spaces/file.zip"                                       # Safe_Str__File__Path sanitizes special chars
            assert type(_.cache_key) is Safe_Str__File__Path

            _.file_id = "ID with spaces!"                                                   # Safe_Str__Id sanitizes identifiers
            assert type(_.file_id) is Safe_Str__Id                                          # Special chars replaced based on Safe_Str__Id rules
            assert _.file_id    == 'ID_with_spaces_'

            _.strategy = "temporal_latest"                                                  # Enum conversion from string
            assert _.strategy == Enum__Cache__Store__Strategy.TEMPORAL_LATEST
            assert isinstance(_.strategy, Enum__Cache__Store__Strategy)

    def test_invalid_strategy(self):                                                      # Test invalid enum value
        with Schema__Cache__Zip__Store__Request() as _:
            with pytest.raises(ValueError):                                               # Not TypeError!
                _.strategy = "invalid_strategy"                                           # Not a valid enum value

    def test_json_serialization(self):                                                    # Test JSON round-trip
        test_data = b"test zip content"

        with Schema__Cache__Zip__Store__Request(zip_bytes = test_data          ,
                                                cache_key = "test/path.zip"    ,
                                                file_id   = "test-id"           ,
                                                namespace = "test"              ,
                                                strategy  = "temporal_versioned") as original:
            json_data = original.json()         # Serialize

            assert "zip_bytes" in json_data                                                 # Bytes should be handled properly
            assert json_data["cache_key"] == "test/path.zip"
            assert json_data["file_id"  ]   == "test-id"
            assert json_data["namespace"] == "test"
            assert json_data["strategy" ]  == "temporal_versioned"                          # Enum serialized as string

            restored = Schema__Cache__Zip__Store__Request.from_json(json_data)              # Deserialize

            assert restored.zip_bytes == test_data
            assert restored.cache_key == original.cache_key
            assert restored.file_id   == original.file_id
            assert restored.namespace == original.namespace
            assert restored.strategy  == Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED  # Enum restored

    def test_optional_fields(self):                                                       # Test optional field behavior
        with Schema__Cache__Zip__Store__Request() as _:
            assert _.cache_key is None                                                   # Optional fields can be None
            assert _.file_id   is None

            # Can be set
            _.cache_key = "test/path"
            assert _.cache_key == "test/path"

    def test_namespace_default(self):                                                       # Test namespace default value
        with Schema__Cache__Zip__Store__Request() as _:
            assert _.namespace == DEFAULT_CACHE__NAMESPACE                                  # Should use constant from consts__Cache_Service
            assert type(_.namespace) is Safe_Str__Id

            _.namespace = "custom-namespace"                                                # Can be overridden
            assert _.namespace == "custom-namespace"

    def test_strategy_enum_values(self):                                                    # Test all strategy enum values
        with Schema__Cache__Zip__Store__Request() as _:
            strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]    # Test each valid strategy

            for strategy_str in strategies:
                _.strategy = strategy_str
                assert isinstance(_.strategy, Enum__Cache__Store__Strategy)
                assert _.strategy.value == strategy_str                                   # Enum value matches string

    def test_bytes_field(self):                                                           # Test bytes field handling
        with Schema__Cache__Zip__Store__Request() as _:
            # Empty bytes by default
            assert _.zip_bytes == b''
            assert type(_.zip_bytes) is bytes

            # Can set various byte values
            _.zip_bytes = b"PK\x03\x04"                                                   # Zip header
            assert _.zip_bytes == b"PK\x03\x04"

            # Large bytes
            large_data = b"x" * 10000
            _.zip_bytes = large_data
            assert len(_.zip_bytes) == 10000