import pytest
from unittest                                                                           import TestCase
from osbot_utils.testing.__                                                             import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                   import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id         import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path       import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.core.Safe_UInt                                    import Safe_UInt
from osbot_utils.utils.Objects                                                          import base_classes
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                 import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy           import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Store__Request     import Schema__Cache__Data__Store__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request  import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Response import Schema__Cache__Data__Retrieve__Response
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Delete import Cache__Service__Data__Delete
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Store             import Cache__Service__Data__Store
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve          import Cache__Service__Data__Retrieve
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve            import Cache__Service__Retrieve
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                  import Cache__Service__Store
from tests.unit.Service__Cache__Test_Objs                                               import setup__service__cache__test_objs


class test_Cache__Service__Data_Delete(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs              = setup__service__cache__test_objs()
        cls.cache_fixtures         = cls.test_objs.cache_fixtures
        cls.service__cache         = cls.cache_fixtures.cache_service
        cls.service__store         = Cache__Service__Store        (cache_service = cls.service__cache)
        cls.service__retrieve      = Cache__Service__Retrieve     (cache_service = cls.service__cache)
        cls.service__store_data    = Cache__Service__Data__Store  (cache_service = cls.service__cache)
        cls.service__delete_data   = Cache__Service__Data__Delete(cache_service = cls.service__cache)
        cls.service__retrieve_data = Cache__Service__Data__Retrieve(cache_service = cls.service__cache)

        cls.test_namespace = Safe_Str__Id("test-data-retrieve")                                 # Test data setup
        cls.test_cache_key = Safe_Str__File__Path("app/data")

        # Create parent cache entry
        cls.parent_response = cls.service__store.store_string(data      = "parent for retrieval"                    ,
                                                              namespace = cls.test_namespace                        ,
                                                              strategy  = Enum__Cache__Store__Strategy.SEMANTIC_FILE,
                                                              cache_key = cls.test_cache_key                        ,
                                                              file_id   = Safe_Str__Id("parent-retrieve")           )
        cls.parent_cache_id = cls.parent_response.cache_id
        cls.test_data       = {}                                                                # Store test data files for retrieval

        # Store string data
        string_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id          ,
                                                             data         = "test string content"        ,
                                                             data_type    = Enum__Cache__Data_Type.STRING,
                                                             data_key     = Safe_Str__File__Path("logs") ,
                                                             data_file_id = Safe_Str__Id("log-001")      ,
                                                             namespace    = cls.test_namespace           )
        cls.test_data['string'] = cls.service__store_data.store_data(string_request)

        # Store JSON data
        json_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id,
                                                           data         = {"status": "active", "count": 42},
                                                           data_type    = Enum__Cache__Data_Type.JSON,
                                                           data_key     = Safe_Str__File__Path("configs"),
                                                           data_file_id = Safe_Str__Id("config-001"),
                                                           namespace    = cls.test_namespace)
        cls.test_data['json'] = cls.service__store_data.store_data(json_request)

        # Store binary data
        binary_request = Schema__Cache__Data__Store__Request(cache_id     = cls.parent_cache_id,
                                                             data         = b"binary content \x00\x01\x02",
                                                             data_type    = Enum__Cache__Data_Type.BINARY,
                                                             data_key     = Safe_Str__File__Path("attachments"),
                                                             data_file_id = Safe_Str__Id("binary-001"),
                                                             namespace    = cls.test_namespace)
        cls.test_data['binary'] = cls.service__store_data.store_data(binary_request)

    def test__init__(self):                                                                     # Test auto-initialization
        with Cache__Service__Data__Delete() as _:
            assert type(_)               is Cache__Service__Data__Delete
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

    def test_delete_data__string(self):                                                             # Test deleting string data
        with self.service__retrieve_data as _:
            store_request = Schema__Cache__Data__Store__Request(cache_id     = self.parent_cache_id              ,          # First store a file to delete
                                                                data         = "to be deleted"                   ,
                                                                data_type    = Enum__Cache__Data_Type.STRING     ,
                                                                data_key     = Safe_Str__File__Path("temp"     ) ,
                                                                data_file_id = Safe_Str__Id        ("delete-me") ,
                                                                namespace    = self.test_namespace               )
            self.service__store_data.store_data(store_request)

        with self.service__delete_data as _:

            delete_request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id         ,           # Now delete it
                                                                    data_type    = Enum__Cache__Data_Type.STRING,
                                                                    data_key     = Safe_Str__File__Path("temp") ,
                                                                    data_file_id = Safe_Str__Id("delete-me")    ,
                                                                    namespace    = self.test_namespace          )

            deleted = _.delete_data(delete_request)
            assert deleted is True

        with self.service__retrieve_data as _:
            result = _.retrieve_data(delete_request)                                                                        # Verify it's gone
            assert result.found is False

    def test_delete_data__not_found(self):                                                      # Test deleting non-existent data
        with self.service__delete_data as _:
            request = Schema__Cache__Data__Retrieve__Request(cache_id     = self.parent_cache_id,
                                                             data_type    = Enum__Cache__Data_Type.STRING,
                                                             data_key     = Safe_Str__File__Path("nonexistent"),
                                                             data_file_id = Safe_Str__Id("not-there"),
                                                             namespace    = self.test_namespace)

            deleted = _.delete_data(request)
            assert deleted is False