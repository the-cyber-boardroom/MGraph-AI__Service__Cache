from unittest                                                                                    import TestCase
from osbot_utils.testing.__                                                                      import __
from osbot_utils.utils.Objects                                                                   import base_classes
from osbot_utils.type_safe.Type_Safe                                                             import Type_Safe
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__List                            import Type_Safe__List
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Request                import (Schema__Cache__Zip__Batch__Request,
                                                                                                         Schema__Zip__Batch__Operation)
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                                 import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy                    import Enum__Cache__Store__Strategy
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                            import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                  import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.zip.safe_str.Safe_Str__Cache__Zip__Operation__Pattern import Safe_Str__Cache__Zip__Operation__Pattern


class test_Schema__Cache__Zip__Batch__Request(TestCase):

    def test__init__(self):                                                               # Test batch request initialization
        with Schema__Cache__Zip__Batch__Request() as _:
            assert type(_)         is Schema__Cache__Zip__Batch__Request
            assert base_classes(_) == [Type_Safe, object]

            # Verify field types
            assert _.cache_id            is None
            assert type(_.operations)    is Type_Safe__List                              # Not regular list!
            assert type(_.namespace)     is Safe_Str__Id
            assert _.strategy            is None

            # Verify defaults
            assert _.atomic        == True
            assert _.namespace     == DEFAULT_CACHE__NAMESPACE
            assert _.strategy      is None

            # Operations list is empty but Type_Safe
            assert _.operations == []
            assert type(_.operations) is Type_Safe__List

    def test_operations_list(self):                                                       # Test operations list handling
        with Schema__Cache__Zip__Batch__Request() as _:
            # Add operations
            op1 = Schema__Zip__Batch__Operation(action  = "add"       ,
                                                path    = "file1.txt" ,
                                                content = b"content1")

            op2 = Schema__Zip__Batch__Operation(action = "remove"    ,
                                                path   = "file2.txt" )

            _.operations.append(op1)
            _.operations.append(op2)

            assert len(_.operations) == 2
            assert _.operations[0].action == "add"
            assert _.operations[1].action == "remove"

    def test_operations_type_safety(self):                                                # Test operations list type checking
        with Schema__Cache__Zip__Batch__Request() as _:
            op = Schema__Zip__Batch__Operation(action="add", path="test.txt")             # Correct type works
            _.operations.append(op)
            assert len(_.operations) == 1

    def test_atomic_mode(self):                                                           # Test atomic flag behavior
        with Schema__Cache__Zip__Batch__Request() as _:
            # Default is atomic
            assert _.atomic == True

            # Can be changed
            _.atomic = False
            assert _.atomic == False

    def test_complex_batch(self):                                                         # Test complex batch configuration
        cache_id = Random_Guid()

        operations = [
            Schema__Zip__Batch__Operation(action    = "add"              ,
                                          path      = "new.txt"          ,
                                          content   = b"new"             ,
                                          condition = "if_not_exists"    ),
            Schema__Zip__Batch__Operation(
                action    = "remove"           ,
                pattern   = "*.tmp"            ,                                          # Pattern-based removal
                condition = "always"
            ),
            Schema__Zip__Batch__Operation(
                action   = "rename"            ,
                path     = "old.txt"           ,
                new_path = "renamed.txt"       ,
                condition = "if_exists"
            )
        ]

        with Schema__Cache__Zip__Batch__Request(cache_id      = cache_id              ,
                                                operations    = operations            ,
                                                atomic        = True                  ,
                                                namespace     = "prod"                ,
                                                strategy      = "temporal_versioned"  ) as _:
            assert _.cache_id == cache_id
            assert len(_.operations) == 3
            assert _.operations[0].action == "add"
            assert _.operations[1].pattern == "*.tmp"
            assert _.operations[2].new_path == "renamed.txt"
            assert _.atomic == True
            assert _.namespace == "prod"
            assert _.strategy == Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED

    def test_json_serialization(self):                                                    # Test JSON round-trip
        operations = [Schema__Zip__Batch__Operation(action  = "add"       ,
                                                    path    = "test.txt"  ,
                                                    content = b"test"     )]

        cache_id =  Random_Guid()
        with Schema__Cache__Zip__Batch__Request(cache_id   = cache_id           ,
                                                operations = operations         ,
                                                namespace  = "test") as original:
            # Serialize
            assert original.obj() == __(atomic     = True       ,
                                        cache_id   = cache_id   ,
                                        namespace  = 'test'     ,
                                        strategy   = None       ,
                                        operations = [__(content   = b'test'    ,
                                                         new_path  = None       ,
                                                         condition ='always'    ,
                                                         pattern   = None       ,
                                                         action    = 'add'      ,
                                                         path      = 'test.txt')])
            json_data = original.json()
            # Check structure
            assert "cache_id"                           in json_data
            assert "operations"                         in json_data
            assert len(json_data["operations"])         == 1
            assert json_data["operations"][0]["action"] == "add"
            assert json_data["atomic"   ]               == True
            assert json_data["strategy" ]               is None

            # Deserialize
            restored = Schema__Cache__Zip__Batch__Request.from_json(json_data)

            # Verify restoration
            assert restored.cache_id             == original.cache_id
            assert len(restored.operations)      == 1
            assert restored.operations[0].action == "add"
            assert restored.operations[0].path   == "test.txt"
            assert restored.atomic               == True
            assert restored.strategy             is None

    def test_pattern_operations(self):                                                    # Test pattern-based operations
        with Schema__Cache__Zip__Batch__Request() as _:
            # Pattern removal
            op = Schema__Zip__Batch__Operation(action  = "remove"   ,
                                               pattern = "*.log"    )                     # Remove all .log files

            _.operations.append(op)

            assert _.operations[0].pattern == "*.log"
            assert type(_.operations[0].pattern) is Safe_Str__Cache__Zip__Operation__Pattern

            # Pattern with directory
            op2 = Schema__Zip__Batch__Operation(action  = "remove"       ,
                                                pattern = "logs/*.tmp"   )               # Remove .tmp in logs/

            _.operations.append(op2)

            assert _.operations[1].pattern == "logs/*.tmp"

    def test_conditional_operations(self):                                                  # Test conditional execution
        with Schema__Cache__Zip__Batch__Request() as _:
            op1 = Schema__Zip__Batch__Operation(action    = "add"              ,            # Add only if not exists
                                                path      = "config.ini"       ,
                                                content   = b"[settings]"      ,
                                                condition = "if_not_exists"   )

            op2 = Schema__Zip__Batch__Operation(action    = "remove"           ,            # Remove only if exists
                                                path      = "temp.dat"         ,
                                                condition = "if_exists"        )

            _.operations = [op1, op2]

            assert _.operations[0].condition == "if_not_exists"
            assert _.operations[1].condition == "if_exists"

    def test_strategy_options(self):                                                        # Test storage strategy options
        with Schema__Cache__Zip__Batch__Request() as _:
            assert _.strategy is None                                                       # Default is temporal_versioned for safety

            _.strategy = "direct"                                                           # Can use direct for in-place update
            assert _.strategy == Enum__Cache__Store__Strategy.DIRECT

            _.strategy = "temporal"                                                         # Temporal for time-based
            assert _.strategy == Enum__Cache__Store__Strategy.TEMPORAL

    def test_empty_operations(self):                                                      # Test with no operations
        with Schema__Cache__Zip__Batch__Request(cache_id   = Random_Guid() ,
                                                operations = []            ,              # Empty list
                                                namespace  = "test"       ) as _:
            assert len(_.operations) == 0
            assert type(_.operations) is Type_Safe__List                                  # Still Type_Safe

            # Can add operations later
            _.operations.append(Schema__Zip__Batch__Operation(action = "add"      ,
                                                              path   = "test.txt" ))
            assert len(_.operations) == 1