import pytest
from unittest                                                                                   import TestCase
from fastapi                                                                                    import HTTPException
from osbot_fast_api.api.routes.Fast_API__Routes                                                 import Fast_API__Routes
from osbot_utils.testing.__                                                                     import __
from osbot_utils.type_safe.Type_Safe                                                            import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path               import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                           import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                 import Safe_Str__Id
from osbot_utils.utils.Objects                                                                  import base_classes
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Exists                          import Routes__Data__Exists, TAG__ROUTES_EXISTS__DATA, PREFIX__ROUTES_EXISTS__DATA, ROUTES_PATHS__EXISTS__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store                           import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                           import Routes__File__Store
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Exists__Response    import Schema__Cache__Data__Exists__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                  import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Exists                    import Cache__Service__Data__Exists
from tests.unit.Service__Cache__Test_Objs                                                       import setup__service__cache__test_objs


class test_Routes__Data__Exists(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs            = setup__service__cache__test_objs()
        cls.cache_fixtures       = cls.test_objs.cache_fixtures
        cls.cache_service        = cls.cache_fixtures.cache_service

        cls.routes_store         = Routes__File__Store (cache_service = cls.cache_service)      # Parent entry routes
        cls.routes_data_store    = Routes__Data__Store (cache_service = cls.cache_service)      # For test data creation
        cls.routes_data_exists   = Routes__Data__Exists(cache_service = cls.cache_service)      # Route under test

        cls.test_namespace       = Safe_Str__Id("test-exists-data")                             # Test namespace
        cls.test_data_key        = Safe_Str__File__Path("configs/app")                          # Hierarchical path

        cls.parent_cache_id      = cls._create_parent_cache_entry(cls)                          # Create parent entry once
        cls._setup_test_data()                                                                  # Store test data for exists checks

    @classmethod
    def _create_parent_cache_entry(cls, self):                                                  # Helper to create parent cache entry
        response = cls.routes_store.store__string(data      = "parent for exists"                     ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL   ,
                                                  namespace = cls.test_namespace                      )
        return response.cache_id

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup test data files for exists checks
        # Store string data with ID only
        cls.string_response_1 = cls.routes_data_store.data__store_string__with__id(data         = "exists test string 1",
                                                                                   cache_id     = cls.parent_cache_id,
                                                                                   namespace    = cls.test_namespace,
                                                                                   data_file_id = "string-exists-001")

        # Store string data with ID and key
        cls.string_response_2 = cls.routes_data_store.data__store_string__with__id_and_key(data         = "exists test string 2",
                                                                                           cache_id     = cls.parent_cache_id,
                                                                                           namespace    = cls.test_namespace,
                                                                                           data_key     = cls.test_data_key,
                                                                                           data_file_id = "string-exists-002")

        # Store JSON data with ID only
        cls.json_response_1 = cls.routes_data_store.data__store_json__with__id(data         = {"exists": "test", "value": 42},
                                                                               cache_id     = cls.parent_cache_id,
                                                                               namespace    = cls.test_namespace,
                                                                               data_file_id = Safe_Str__Id("json-exists-001"))

        # Store JSON data with ID and key
        cls.json_response_2 = cls.routes_data_store.data__store_json__with__id_and_key(data         = {"nested": "exists", "items": [1, 2, 3]},
                                                                                       cache_id     = cls.parent_cache_id,
                                                                                       namespace    = cls.test_namespace,
                                                                                       data_key     = cls.test_data_key,
                                                                                       data_file_id = Safe_Str__Id("json-exists-002"))

        # Store binary data with ID only
        cls.binary_response_1 = cls.routes_data_store.data__store_binary__with__id(body         = b'EXISTS\x00TEST\x01BINARY',
                                                                                   cache_id     = cls.parent_cache_id,
                                                                                   namespace    = cls.test_namespace,
                                                                                   data_file_id = Safe_Str__Id("binary-exists-001"))

        # Store binary data with ID and key
        cls.binary_response_2 = cls.routes_data_store.data__store_binary__with__id_and_key(body         = b'exists binary with key \x00\x01\x02',
                                                                                           cache_id     = cls.parent_cache_id,
                                                                                           namespace    = cls.test_namespace,
                                                                                           data_key     = cls.test_data_key,
                                                                                           data_file_id = Safe_Str__Id("binary-exists-002"))

    def test__init__(self):                                                                     # Test initialization and structure
        with Routes__Data__Exists() as _:
            assert type(_)                     is Routes__Data__Exists
            assert base_classes(_)             == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                       == TAG__ROUTES_EXISTS__DATA
            assert _.prefix                    == PREFIX__ROUTES_EXISTS__DATA
            assert type(_.cache_service)       is Cache__Service
            assert type(_.exists_service())    is Cache__Service__Data__Exists
            assert _.obj()                     == __(  tag='data',
                                                       prefix='/{namespace}/cache/{cache_id}',
                                                       router='APIRouter',
                                                       route_registration=__(analyzer=__(),
                                                                             converter=__(),
                                                                             wrapper_creator=__(converter=__()),
                                                                             route_parser=__()),
                                                       cache_service=__(cache_config=__(storage_mode='memory',
                                                                                        default_bucket=None,
                                                                                        default_ttl_hours=24,
                                                                                        local_disk_path=None,
                                                                                        sqlite_path=None,
                                                                                        zip_path=None),
                                                                        cache_handlers=__(),
                                                                        hash_config=__(algorithm='sha256', length=16),
                                                                        hash_generator=__(config=__(algorithm='sha256', length=16))),
                                                       app=None,
                                                       filter_tag=True)

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_EXISTS__DATA       == 'data'
        assert PREFIX__ROUTES_EXISTS__DATA    == '/{namespace}/cache/{cache_id}'
        assert len(ROUTES_PATHS__EXISTS__DATA) == 2
        assert ROUTES_PATHS__EXISTS__DATA[0]  == '/{namespace}/cache/{cache_id}/data/exists/{data_type}/{data_file_id}'
        assert ROUTES_PATHS__EXISTS__DATA[1]  == '/{namespace}/cache/{cache_id}/data/exists/{data_type}/{data_key:path}/{data_file_id}'

    def test_data__exists__with__id__string_exists(self):                                       # Test string data exists with ID only
        with self.routes_data_exists as _:
            result = _.data__exists__with__id(cache_id     = self.parent_cache_id               ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.STRING      ,
                                              data_file_id = Safe_Str__Id("string-exists-001")  )

            assert type(result)        is Schema__Cache__Data__Exists__Response
            assert result.exists       is True
            assert result.cache_id     == self.parent_cache_id
            assert result.namespace    == self.test_namespace
            assert result.data_type    == Enum__Cache__Data_Type.STRING
            assert result.data_file_id == "string-exists-001"
            assert result.obj()        == __(data_key='',
                                             data_file_id='string-exists-001',
                                             cache_id=self.parent_cache_id,
                                             data_type='string',
                                             exists=True,
                                             namespace='test-exists-data')

    def test_data__exists__with__id__string_not_exists(self):                                   # Test string data not exists with ID only
        with self.routes_data_exists as _:
            result = _.data__exists__with__id(cache_id     = self.parent_cache_id               ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.STRING      ,
                                              data_file_id = Safe_Str__Id("non-existent-001")   )

            assert type(result)  is Schema__Cache__Data__Exists__Response
            assert result.exists is False

    def test_data__exists__with__id__json_exists(self):                                         # Test JSON data exists with ID only
        with self.routes_data_exists as _:
            result = _.data__exists__with__id(cache_id     = self.parent_cache_id               ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.JSON        ,
                                              data_file_id = Safe_Str__Id("json-exists-001")    )

            assert result.exists    is True
            assert result.data_type == Enum__Cache__Data_Type.JSON

    def test_data__exists__with__id__binary_exists(self):                                       # Test binary data exists with ID only
        with self.routes_data_exists as _:
            result = _.data__exists__with__id(cache_id     = self.parent_cache_id               ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.BINARY      ,
                                              data_file_id = Safe_Str__Id("binary-exists-001")  )

            assert result.exists    is True
            assert result.data_type == Enum__Cache__Data_Type.BINARY

    def test_data__exists__with__id_and_key__string_exists(self):                               # Test string data exists with ID and key
        with self.routes_data_exists as _:
            result = _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id               ,
                                                      namespace    = self.test_namespace                ,
                                                      data_type    = Enum__Cache__Data_Type.STRING      ,
                                                      data_key     = self.test_data_key                 ,
                                                      data_file_id = Safe_Str__Id("string-exists-002")  )

            assert type(result)       is Schema__Cache__Data__Exists__Response
            assert result.exists      is True
            assert result.data_key    == self.test_data_key
            assert result.data_file_id == "string-exists-002"

    def test_data__exists__with__id_and_key__json_exists(self):                                 # Test JSON data exists with ID and key
        with self.routes_data_exists as _:
            result = _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id               ,
                                                      namespace    = self.test_namespace                ,
                                                      data_type    = Enum__Cache__Data_Type.JSON        ,
                                                      data_key     = self.test_data_key                 ,
                                                      data_file_id = Safe_Str__Id("json-exists-002")    )

            assert result.exists    is True
            assert result.data_type == Enum__Cache__Data_Type.JSON
            assert result.data_key  == self.test_data_key

    def test_data__exists__with__id_and_key__binary_exists(self):                               # Test binary data exists with ID and key
        with self.routes_data_exists as _:
            result = _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id               ,
                                                      namespace    = self.test_namespace                ,
                                                      data_type    = Enum__Cache__Data_Type.BINARY      ,
                                                      data_key     = self.test_data_key                 ,
                                                      data_file_id = Safe_Str__Id("binary-exists-002")  )

            assert result.exists    is True
            assert result.data_type == Enum__Cache__Data_Type.BINARY
            assert result.data_key  == self.test_data_key

    def test_data__exists__with__id_and_key__not_exists(self):                                  # Test data not exists with ID and key
        with self.routes_data_exists as _:
            result = _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id               ,
                                                      namespace    = self.test_namespace                ,
                                                      data_type    = Enum__Cache__Data_Type.JSON        ,
                                                      data_key     = self.test_data_key                 ,
                                                      data_file_id = Safe_Str__Id("non-existent")       )

            assert result.exists is False

    def test_data__exists__wrong_data_type(self):                                               # Test checking with wrong data type
        with self.routes_data_exists as _:
            # File exists as STRING but checking as JSON
            result = _.data__exists__with__id(cache_id     = self.parent_cache_id               ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.JSON        ,
                                              data_file_id = Safe_Str__Id("string-exists-001")  )

            assert result.exists is False                                                        # Wrong type should return false

    def test_data__exists__wrong_data_key(self):                                                # Test checking with wrong data key
        with self.routes_data_exists as _:
            # File exists at test_data_key but checking at different key
            result = _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id               ,
                                                      namespace    = self.test_namespace                ,
                                                      data_type    = Enum__Cache__Data_Type.STRING      ,
                                                      data_key     = Safe_Str__File__Path("wrong/key")  ,
                                                      data_file_id = Safe_Str__Id("string-exists-002")  )

            assert result.exists is False

    def test_data__exists__missing_data_file_id(self):                                          # Test error when data_file_id missing
        with self.routes_data_exists as _:
            with pytest.raises(HTTPException) as exc_info:
                _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id        ,
                                                 namespace    = self.test_namespace         ,
                                                 data_type    = Enum__Cache__Data_Type.JSON ,
                                                 data_key     = self.test_data_key          ,
                                                 data_file_id = None                        )

            error = exc_info.value
            assert error.status_code          == 400
            assert error.detail["error_type"] == "INVALID_INPUT"
            assert "data_file_id"            in error.detail["message"]

    def test_data__exists__missing_data_type(self):                                             # Test error when data_type missing
        with self.routes_data_exists as _:
            with pytest.raises(HTTPException) as exc_info:
                _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id            ,
                                                 namespace    = self.test_namespace             ,
                                                 data_type    = None                            ,
                                                 data_key     = self.test_data_key              ,
                                                 data_file_id = Safe_Str__Id("string-exists-001"))

            error = exc_info.value
            assert error.status_code          == 400
            assert error.detail["error_type"] == "INVALID_INPUT"
            assert "data_type"               in error.detail["message"]

    def test_data__exists__non_existent_parent(self):                                           # Test with non-existent parent cache entry
        with self.routes_data_exists as _:
            non_existent_id = Random_Guid()

            result = _.data__exists__with__id(cache_id     = non_existent_id                    ,
                                              namespace    = self.test_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.STRING      ,
                                              data_file_id = Safe_Str__Id("any")                )

            assert result.exists is False                                                        # Non-existent parent means data doesn't exist

    def test_data__exists__namespace_isolation(self):                                           # Test namespace isolation
        with self.routes_data_exists as _:
            different_namespace = Safe_Str__Id("different-namespace")

            # Data exists in test_namespace but not in different_namespace
            result = _.data__exists__with__id(cache_id     = self.parent_cache_id               ,
                                              namespace    = different_namespace                ,
                                              data_type    = Enum__Cache__Data_Type.STRING      ,
                                              data_file_id = Safe_Str__Id("string-exists-001")  )

            assert result.exists   is False
            assert result.namespace == different_namespace

    def test_data__exists__delegates_to_with_key(self):                                         # Test with__id delegates to with__id_and_key
        with self.routes_data_exists as _:
            # Both should return same result for data without key
            result1 = _.data__exists__with__id(cache_id     = self.parent_cache_id              ,
                                               namespace    = self.test_namespace               ,
                                               data_type    = Enum__Cache__Data_Type.STRING     ,
                                               data_file_id = Safe_Str__Id("string-exists-001") )

            result2 = _.data__exists__with__id_and_key(cache_id     = self.parent_cache_id              ,
                                                       namespace    = self.test_namespace               ,
                                                       data_type    = Enum__Cache__Data_Type.STRING     ,
                                                       data_key     = ''                                ,
                                                       data_file_id = Safe_Str__Id("string-exists-001") )

            assert result1.exists == result2.exists

    def test__integration__store_and_check_exists(self):                                        # Full integration test
        with self.routes_data_exists as _:
            # Create new parent
            parent = self.routes_store.store__string(data      = "integration parent"                   ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            # Check data doesn't exist yet
            pre_check = _.data__exists__with__id(cache_id     = parent.cache_id                 ,
                                                 namespace    = self.test_namespace             ,
                                                 data_type    = Enum__Cache__Data_Type.JSON     ,
                                                 data_file_id = Safe_Str__Id("integ-check")     )
            assert pre_check.exists is False

            # Store data
            self.routes_data_store.data__store_json__with__id(
                data         = {"integration": "check"},
                cache_id     = parent.cache_id,
                namespace    = self.test_namespace,
                data_file_id = Safe_Str__Id("integ-check")
            )

            # Check data exists now
            post_check = _.data__exists__with__id(cache_id     = parent.cache_id                ,
                                                  namespace    = self.test_namespace            ,
                                                  data_type    = Enum__Cache__Data_Type.JSON    ,
                                                  data_file_id = Safe_Str__Id("integ-check")    )
            assert post_check.exists is True

    def test__all_data_types_exist(self):                                                       # Test all data types can be checked
        with self.routes_data_exists as _:
            # Check all types we stored in setup
            string_exists = _.data__exists__with__id(cache_id     = self.parent_cache_id            ,
                                                     namespace    = self.test_namespace             ,
                                                     data_type    = Enum__Cache__Data_Type.STRING   ,
                                                     data_file_id = Safe_Str__Id("string-exists-001"))

            json_exists = _.data__exists__with__id(cache_id     = self.parent_cache_id              ,
                                                   namespace    = self.test_namespace               ,
                                                   data_type    = Enum__Cache__Data_Type.JSON       ,
                                                   data_file_id = Safe_Str__Id("json-exists-001")   )

            binary_exists = _.data__exists__with__id(cache_id     = self.parent_cache_id            ,
                                                     namespace    = self.test_namespace             ,
                                                     data_type    = Enum__Cache__Data_Type.BINARY   ,
                                                     data_file_id = Safe_Str__Id("binary-exists-001"))

            assert string_exists.exists is True
            assert json_exists.exists   is True
            assert binary_exists.exists is True