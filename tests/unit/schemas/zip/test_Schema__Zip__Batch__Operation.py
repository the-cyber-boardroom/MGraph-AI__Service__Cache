import pytest
from unittest                                                                              import TestCase
from osbot_utils.utils.Objects                                                             import base_classes
from osbot_utils.type_safe.Type_Safe                                                       import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path          import Safe_Str__File__Path
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Zip__Batch__Operation               import Schema__Zip__Batch__Operation


class test_Schema__Zip__Batch__Operation(TestCase):

    def test__init__(self):                                                               # Test operation schema initialization
        with Schema__Zip__Batch__Operation() as _:
            assert type(_)         is Schema__Zip__Batch__Operation
            assert base_classes(_) == [Type_Safe, object]

            # Verify field types
            assert _.action     is None                                                    # Literal type
            assert type(_.path) is Safe_Str__File__Path
            assert _.content    is None
            assert _.new_path   is None
            assert _.condition  == "always"                                                # Default value
            assert _.pattern    is None

    def test_action_literals(self):                                                       # Test action literal validation
        with Schema__Zip__Batch__Operation() as _:
            # Valid actions
            valid_actions = ["add", "remove", "replace", "rename"]

            for action in valid_actions:
                _.action = action
                assert _.action == action

            # Invalid action
            with pytest.raises(ValueError):
                _.action = "invalid_action"                                               # Not in Literal set

    def test_condition_literals(self):                                                    # Test condition literal validation
        with Schema__Zip__Batch__Operation() as _:
            # Valid conditions
            valid_conditions = ["always", "if_exists", "if_not_exists"]

            for condition in valid_conditions:
                _.condition = condition
                assert _.condition == condition

            # Invalid condition
            with pytest.raises(ValueError):
                _.condition = "sometimes"                                                 # Not in Literal set

    def test_operation_requirements(self):                                                # Test field requirements per action
        # Add operation requires content
        with Schema__Zip__Batch__Operation(action="add", path="test.txt") as _:
            assert _.action  == "add"
            assert _.path    == "test.txt"
            assert _.content is None                                                       # Auto-initialized empty

        # Remove operation doesn't need content
        with Schema__Zip__Batch__Operation(action="remove", path="test.txt") as _:
            assert _.action  == "remove"
            assert _.path    == "test.txt"
            assert _.content is None                                                      # Optional

        # Rename needs new_path
        with Schema__Zip__Batch__Operation(
            action   = "rename"    ,
            path     = "old.txt"   ,
            new_path = "new.txt"
        ) as _:
            assert _.action   == "rename"
            assert _.path     == "old.txt"
            assert _.new_path == "new.txt"