import pytest
from unittest                                                                     import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                           import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                       import __
from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text      import Safe_Str__Text
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt
from osbot_utils.utils.Objects                                                    import base_classes
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy     import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                         import Cache__Service
from mgraph_ai_service_cache.service.cache.Cache__Service__Retrieve__Data         import Cache__Service__Retrieve__Data, Schema__Child__File__Info, Schema__Child__File__Data
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store            import Cache__Service__Store
from tests.unit.Service__Cache__Test_Objs                                         import setup__service__cache__test_objs


class test_Cache__Service__Retrieve__Data(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        pytest.skip("needs refactoring of Cache__Service__Store__Child (namely how data files are stored)")
        cls.test_objs        = setup__service__cache__test_objs()
        cls.cache_fixtures   = cls.test_objs.cache_fixtures
        cls.cache_service    = cls.cache_fixtures.cache_service
        cls.store_service    = Cache__Service__Store         (cache_service = cls.cache_service)
        cls.child_store      = Cache__Service__Store__Child  (cache_service = cls.cache_service)
        cls.child_retrieve   = Cache__Service__Retrieve__Data(cache_service = cls.cache_service)

        cls.test_namespace   = Safe_Str__Id("test-retrieve-child")                              # Test data
        cls.test_cache_key   = Safe_Str__File__Path("test/retrieve")
        cls.test_strategy    = Enum__Cache__Store__Strategy.SEMANTIC_FILE

        # Create parent file
        parent_response      = cls.store_service.store_string(data      = "parent for retrieval"              ,
                                                              namespace = cls.test_namespace                  ,
                                                              strategy  = cls.test_strategy                   ,
                                                              cache_key = cls.test_cache_key                  ,
                                                              file_id   = Safe_Str__Id("retrieve-parent")     )
        cls.parent_file_id   = parent_response.cache_id if parent_response else Safe_Str__Id("retrieve-parent")

        # Create test children
        cls.test_children = {}
        for child_type, data in [("string", "test child string"          ),
                                 ("json"  , {"child": "json", "id": 123}),
                                 ("binary", b"test child bytes\x00\x01" )]:
            child_id = Safe_Str__Id(f"test-{child_type}-child")
            cls.child_store.store_data(data      = data,
                                       data_type = Safe_Str__Text(child_type),
                                       namespace = cls.test_namespace,
                                       strategy  = cls.test_strategy,
                                       cache_key = cls.test_cache_key,
                                       file_id   = cls.parent_file_id,
                                       child_id  = child_id)
            cls.test_children[child_type] = {"id": child_id, "data": data}

    def test__init__(self):                                                                     # Test auto-initialization
        with Cache__Service__Retrieve__Data() as _:
            assert type(_) is Cache__Service__Retrieve__Data
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

    def test_retrieve_child__string(self):                                                      # Test retrieving string child
        with self.child_retrieve as _:
            child_info = self.test_children["string"]
            result = _.retrieve_child(child_id  = child_info["id"]                          ,
                                     namespace = self.test_namespace                        ,
                                     strategy  = self.test_strategy                         ,
                                     cache_key = self.test_cache_key                        ,
                                     file_id   = self.parent_file_id                        )

            assert type(result)       is Schema__Child__File__Data
            assert result.data        == child_info["data"]
            assert result.data_type   == Safe_Str__Text("string")
            assert result.child_id    == child_info["id"]
            assert type(result.path)  is Safe_Str__File__Path

            # Use .obj() for comprehensive comparison
            assert result.obj().contains(__(data      = child_info["data"]                  ,
                                           data_type = "string"                             ,
                                           child_id  = child_info["id"]                     ))

    def test_retrieve_child__json(self):                                                        # Test retrieving JSON child
        with self.child_retrieve as _:
            child_info = self.test_children["json"]
            result = _.retrieve_child(child_id  = child_info["id"]                          ,
                                     namespace = self.test_namespace                        ,
                                     strategy  = self.test_strategy                         ,
                                     cache_key = self.test_cache_key                        ,
                                     file_id   = self.parent_file_id                        )

            assert type(result)       is Schema__Child__File__Data
            assert result.data        == child_info["data"]
            assert result.data_type   == Safe_Str__Text("json")
            assert result.child_id    == child_info["id"]

    def test_retrieve_child__binary(self):                                                      # Test retrieving binary child
        with self.child_retrieve as _:
            child_info = self.test_children["binary"]
            result = _.retrieve_child(child_id  = child_info["id"]                          ,
                                     namespace = self.test_namespace                        ,
                                     strategy  = self.test_strategy                         ,
                                     cache_key = self.test_cache_key                        ,
                                     file_id   = self.parent_file_id                        )

            assert type(result)       is Schema__Child__File__Data
            assert result.data        == child_info["data"]
            assert result.data_type   == Safe_Str__Text("binary")
            assert result.child_id    == child_info["id"]

    def test_retrieve_child__not_found(self):                                                   # Test retrieving non-existent child
        with self.child_retrieve as _:
            result = _.retrieve_child(child_id  = Safe_Str__Id("non-existent-child")        ,
                                     namespace = self.test_namespace                        ,
                                     strategy  = self.test_strategy                         ,
                                     cache_key = self.test_cache_key                        ,
                                     file_id   = self.parent_file_id                        )

            assert result is None                                                               # Not found returns None

    def test_retrieve_child__missing_parameters(self):                                          # Test parameter validation
        with self.child_retrieve as _:
            # Missing file_id
            try:
                _.retrieve_child(child_id  = Safe_Str__Id("some-child")                     ,
                               namespace = self.test_namespace                             ,
                               strategy  = self.test_strategy                              ,
                               cache_key = self.test_cache_key                             ,
                               file_id   = None                                            )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "file_id is required to retrieve child"

            # Missing cache_key
            try:
                _.retrieve_child(child_id  = Safe_Str__Id("some-child")                     ,
                               namespace = self.test_namespace                             ,
                               strategy  = self.test_strategy                              ,
                               cache_key = None                                            ,
                               file_id   = self.parent_file_id                             )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "cache_key is required to retrieve child"

            # Missing child_id
            try:
                _.retrieve_child(child_id  = None                                          ,
                               namespace = self.test_namespace                             ,
                               strategy  = self.test_strategy                              ,
                               cache_key = self.test_cache_key                             ,
                               file_id   = self.parent_file_id                             )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "child_id is required to retrieve child"

    def test_list_children(self):                                                               # Test listing all children
        with self.child_retrieve as _:
            children = _.list_children(namespace = self.test_namespace                      ,
                                      strategy  = self.test_strategy                        ,
                                      cache_key = self.test_cache_key                       ,
                                      file_id   = self.parent_file_id                       )

            assert type(children) is list
            assert len(children) >= 3                                                           # At least our 3 test children

            for child in children:
                assert type(child)           is Schema__Child__File__Info
                assert type(child.child_id)  is Safe_Str__Id
                assert type(child.path)      is Safe_Str__File__Path
                assert type(child.data_type) is Safe_Str__Text
                assert type(child.extension) is Safe_Str__Text

                # Check our test children are in the list
                if child.child_id in [c["id"] for c in self.test_children.values()]:
                    assert child.data_type in ["string", "json", "binary"]
                    assert child.extension in ["txt", "json", "bin"]

    def test_list_children__empty_parent(self):                                                 # Test listing when no children exist
        skip__if_not__in_github_actions()
        with self.child_retrieve as _:
            # Create new parent with no children
            empty_parent = self.store_service.store_string(data      = "empty parent"                        ,
                                                          namespace = self.test_namespace                    ,
                                                          strategy  = self.test_strategy                     ,
                                                          cache_key = Safe_Str__File__Path("empty/parent")   ,
                                                          file_id   = Safe_Str__Id("empty-001")              )

            children = _.list_children(namespace = self.test_namespace                      ,
                                      strategy  = self.test_strategy                        ,
                                      cache_key = Safe_Str__File__Path("empty/parent")      ,
                                      file_id   = empty_parent.cache_id                     )

            assert type(children) is list
            assert len(children) == 0                                                           # No children

    def test_list_children__missing_parameters(self):                                           # Test parameter validation for list
        with self.child_retrieve as _:
            # Missing file_id
            try:
                _.list_children(namespace = self.test_namespace                             ,
                               strategy  = self.test_strategy                              ,
                               cache_key = self.test_cache_key                             ,
                               file_id   = None                                            )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "file_id is required to list children"

            # Missing cache_key
            try:
                _.list_children(namespace = self.test_namespace                             ,
                               strategy  = self.test_strategy                              ,
                               cache_key = None                                            ,
                               file_id   = self.parent_file_id                             )
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert str(e) == "cache_key is required to list children"

    def test_count_children(self):                                                              # Test counting children
        with self.child_retrieve as _:
            count = _.count_children(namespace = self.test_namespace                        ,
                                    strategy  = self.test_strategy                          ,
                                    cache_key = self.test_cache_key                         ,
                                    file_id   = self.parent_file_id                         )

            assert type(count) is Safe_UInt
            assert count >= 3                                                                   # At least our 3 test children

    def test_delete_child(self):                                                                # Test deleting a child
        skip__if_not__in_github_actions()
        with self.child_retrieve as _:
            # Create a child to delete
            delete_child_id = Safe_Str__Id("child-to-delete")
            self.child_store.store_data(data      ="data to delete",
                                        data_type = Safe_Str__Text("string"),
                                        namespace = self.test_namespace,
                                        strategy  = self.test_strategy,
                                        cache_key = self.test_cache_key,
                                        file_id   = self.parent_file_id,
                                        child_id  = delete_child_id)

            # Verify it exists
            exists = _.retrieve_child(child_id  = delete_child_id                           ,
                                     namespace = self.test_namespace                        ,
                                     strategy  = self.test_strategy                         ,
                                     cache_key = self.test_cache_key                        ,
                                     file_id   = self.parent_file_id                        )
            assert exists is not None

            # Delete it
            deleted = _.delete_child(child_id  = delete_child_id                            ,
                                    namespace = self.test_namespace                         ,
                                    strategy  = self.test_strategy                          ,
                                    cache_key = self.test_cache_key                         ,
                                    file_id   = self.parent_file_id                         )

            assert deleted is True                                                              # Successfully deleted

            # Verify it's gone
            gone = _.retrieve_child(child_id  = delete_child_id                             ,
                                   namespace = self.test_namespace                         ,
                                   strategy  = self.test_strategy                          ,
                                   cache_key = self.test_cache_key                         ,
                                   file_id   = self.parent_file_id                         )
            assert gone is None

    def test_delete_child__not_found(self):                                                     # Test deleting non-existent child
        with self.child_retrieve as _:
            deleted = _.delete_child(child_id  = Safe_Str__Id("never-existed")              ,
                                    namespace = self.test_namespace                         ,
                                    strategy  = self.test_strategy                          ,
                                    cache_key = self.test_cache_key                         ,
                                    file_id   = self.parent_file_id                         )

            assert deleted is False                                                             # Not found

    def test_delete_all_children(self):                                                         # Test deleting all children
        skip__if_not__in_github_actions()
        with self.child_retrieve as _:
            # Create parent with children to delete
            delete_parent = self.store_service.store_string(data      = "parent for delete all"                 ,
                                                           namespace = self.test_namespace                      ,
                                                           strategy  = self.test_strategy                       ,
                                                           cache_key = Safe_Str__File__Path("delete/all")       ,
                                                           file_id   = Safe_Str__Id("delete-parent")            )

            # Add multiple children
            for i in range(5):
                self.child_store.store_data(data      =f"delete child {i}",
                                            data_type = Safe_Str__Text("string"),
                                            namespace = self.test_namespace,
                                            strategy  = self.test_strategy,
                                            cache_key = Safe_Str__File__Path("delete/all"),
                                            file_id   = delete_parent.cache_id,
                                            child_id  = Safe_Str__Id(f"del-child-{i}"))

            # Verify they exist
            count_before = _.count_children(namespace = self.test_namespace                 ,
                                          strategy  = self.test_strategy                    ,
                                          cache_key = Safe_Str__File__Path("delete/all")    ,
                                          file_id   = delete_parent.cache_id                )
            assert count_before == 5

            # Delete all
            deleted_count = _.delete_all_children(namespace = self.test_namespace           ,
                                                 strategy  = self.test_strategy             ,
                                                 cache_key = Safe_Str__File__Path("delete/all"),
                                                 file_id   = delete_parent.cache_id         )

            assert type(deleted_count) is Safe_UInt
            assert deleted_count == 5

            # Verify they're gone
            count_after = _.count_children(namespace = self.test_namespace                  ,
                                         strategy  = self.test_strategy                     ,
                                         cache_key = Safe_Str__File__Path("delete/all")     ,
                                         file_id   = delete_parent.cache_id                 )
            assert count_after == 0

    def test__get_parent_base_path(self):                                                       # Test internal path calculation
        with self.child_retrieve as _:
            handler = self.cache_service.get_or_create_handler(self.test_namespace)

            parent_base = _._get_parent_base_path(handler    = handler                      ,
                                                 strategy   = self.test_strategy            ,
                                                 cache_key  = self.test_cache_key           ,
                                                 file_id    = self.parent_file_id           )

            assert type(parent_base) is str
            assert len(parent_base) > 0
            # Path should contain strategy and cache_key components
            assert str(self.test_strategy.value) in parent_base or "semantic" in parent_base
            assert "test/retrieve" in parent_base

    def test__get_child_path(self):                                                             # Test child path generation
        with self.child_retrieve as _:
            handler = self.cache_service.get_or_create_handler(self.test_namespace)

            child_path = _._get_child_path(handler   = handler                              ,
                                          strategy  = self.test_strategy                    ,
                                          cache_key = self.test_cache_key                   ,
                                          file_id   = self.parent_file_id                   ,
                                          child_id  = Safe_Str__Id("test-child")            )

            assert type(child_path) is str
            assert "test-child" in child_path
            assert str(self.parent_file_id) in child_path                                       # Should contain parent ID

    def test_schema__child__file__info(self):                                                   # Test Schema__Child__File__Info
        with Schema__Child__File__Info() as _:
            assert type(_)            is Schema__Child__File__Info
            assert base_classes(_)    == [Type_Safe, object]
            assert type(_.child_id)   is Safe_Str__Id
            assert type(_.path)       is Safe_Str__File__Path
            assert type(_.data_type)  is Safe_Str__Text
            assert type(_.extension)  is Safe_Str__Text

            # Test with data
            _.child_id  = Safe_Str__Id("info-child")
            _.path      = Safe_Str__File__Path("path/to/child.txt")
            _.data_type = Safe_Str__Text("string")
            _.extension = Safe_Str__Text("txt")

            assert _.obj() == __(child_id  = "info-child"                                   ,
                                path      = "path/to/child.txt"                            ,
                                data_type = "string"                                       ,
                                extension = "txt"                                          )

    def test_schema__child__file__data(self):                                                   # Test Schema__Child__File__Data
        with Schema__Child__File__Data() as _:
            assert type(_)            is Schema__Child__File__Data
            assert base_classes(_)    == [Type_Safe, object]
            # Note: 'data' is Any type, so no type checking
            assert type(_.data_type)  is Safe_Str__Text
            assert type(_.child_id)   is Safe_Str__Id
            assert type(_.path)       is Safe_Str__File__Path

            # Test with string data
            _.data      = "test string data"
            _.data_type = Safe_Str__Text("string")
            _.child_id  = Safe_Str__Id("data-child")
            _.path      = Safe_Str__File__Path("path/to/data.txt")

            assert _.obj() == __(data      = "test string data"                             ,
                                data_type = "string"                                       ,
                                child_id  = "data-child"                                   ,
                                path      = "path/to/data.txt"                            )