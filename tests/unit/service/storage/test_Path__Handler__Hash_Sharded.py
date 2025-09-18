from unittest                                                                       import TestCase
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.utils.Objects                                                      import base_classes, __
from memory_fs.path_handlers.Path__Handler                                          import Path__Handler
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id

from mgraph_ai_service_cache.service.storage.path_handlers.Path__Handler__Hash_Sharded import Path__Handler__Hash_Sharded


class test_Path__Handler__Hash_Sharded(TestCase):

    def test__init__(self):                                                      # Test initialization
        with Path__Handler__Hash_Sharded() as _:
            assert type(_)          is Path__Handler__Hash_Sharded
            assert base_classes(_)  == [Path__Handler,Type_Safe, object]
            assert _.shard_depth    == 2
            assert _.shard_size     == 2

    def test_generate_path(self):                                                # Test sharded path generation
        with Path__Handler__Hash_Sharded() as _:
            # Test with 16-char hash
            file_id = Safe_Str__Id("a73f2e4b12345678")
            path    = _.generate_path(file_id)

            assert type(path) is Safe_Str__File__Path
            assert str(path)  == "a7/3f"                                         # First 4 chars as 2 levels

    def test_generate_path__with_prefix_suffix(self):                            # Test with prefix and suffix
        with Path__Handler__Hash_Sharded() as _:
            _.prefix_path = Safe_Str__File__Path("refs/by-hash")
            _.suffix_path = Safe_Str__File__Path("latest")

            file_id = Safe_Str__Id("a73f2e4b12345678")
            path    = _.generate_path(file_id)

            assert str(path) == "refs/by-hash/a7/3f/latest"

    def test_generate_path__short_hash(self):                                    # Test with hash shorter than sharding
        with Path__Handler__Hash_Sharded() as _:
            file_id = Safe_Str__Id("ab")                                              # Only 2 chars
            path    = _.generate_path(file_id)

            assert str(path) == "ab"                                             # Uses what's available

    def test_generate_path__custom_sharding(self):                               # Test configurable sharding
        with Path__Handler__Hash_Sharded() as _:
            _.shard_depth = 3
            _.shard_size  = 1

            file_id = Safe_Str__Id("abcdef123456")
            path    = _.generate_path(file_id)

            assert str(path) == "a/b/c"                                          # 3 levels, 1 char each