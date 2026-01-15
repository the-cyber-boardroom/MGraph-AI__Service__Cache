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
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Update                          import Routes__Data__Update, TAG__ROUTES_UPDATE__DATA, PREFIX__ROUTES_UPDATE__DATA, ROUTES_PATHS__UPDATE__DATA
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Store                           import Routes__Data__Store
from mgraph_ai_service_cache.fast_api.routes.data.Routes__Data__Retrieve                        import Routes__Data__Retrieve
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                           import Routes__File__Store
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Update__Response    import Schema__Cache__Data__Update__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                  import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Update                    import Cache__Service__Data__Update
from tests.unit.Service__Cache__Test_Objs                                                       import setup__service__cache__test_objs


class test_Routes__Data__Update(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs            = setup__service__cache__test_objs()
        cls.cache_fixtures       = cls.test_objs.cache_fixtures
        cls.cache_service        = cls.cache_fixtures.cache_service

        cls.routes_store         = Routes__File__Store   (cache_service = cls.cache_service)    # Parent entry routes
        cls.routes_data_store    = Routes__Data__Store   (cache_service = cls.cache_service)    # For test data creation
        cls.routes_data_retrieve = Routes__Data__Retrieve(cache_service = cls.cache_service)    # For verification
        cls.routes_data_update   = Routes__Data__Update  (cache_service = cls.cache_service)    # Route under test

        cls.test_namespace       = Safe_Str__Id("test-update-data")                             # Test namespace
        cls.test_data_key        = Safe_Str__File__Path("configs/app")                          # Hierarchical path

        cls.parent_cache_id      = cls._create_parent_cache_entry(cls)                          # Create parent entry once
        cls._setup_test_data()                                                                  # Store test data for updates

    @classmethod
    def _create_parent_cache_entry(cls, self):                                                  # Helper to create parent cache entry
        response = cls.routes_store.store__string(data      = "parent for update"                     ,
                                                  strategy  = Enum__Cache__Store__Strategy.TEMPORAL   ,
                                                  namespace = cls.test_namespace                      )
        return response.cache_id

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup test data files for updates
        # Store string data with ID only
        cls.string_response_1 = cls.routes_data_store.data__store_string__with__id(
            data         = "original string 1",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("string-update-001")
        )

        # Store string data with ID and key
        cls.string_response_2 = cls.routes_data_store.data__store_string__with__id_and_key(
            data         = "original string 2",
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("string-update-002")
        )

        # Store JSON data with ID only
        cls.json_response_1 = cls.routes_data_store.data__store_json__with__id(
            data         = {"original": "json", "value": 1},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("json-update-001")
        )

        # Store JSON data with ID and key
        cls.json_response_2 = cls.routes_data_store.data__store_json__with__id_and_key(
            data         = {"original": "nested", "items": [1]},
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("json-update-002")
        )

        # Store binary data with ID only
        cls.binary_response_1 = cls.routes_data_store.data__store_binary__with__id(
            body         = b'ORIGINAL\x00BINARY',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_file_id = Safe_Str__Id("binary-update-001")
        )

        # Store binary data with ID and key
        cls.binary_response_2 = cls.routes_data_store.data__store_binary__with__id_and_key(
            body         = b'original binary with key',
            cache_id     = cls.parent_cache_id,
            namespace    = cls.test_namespace,
            data_key     = cls.test_data_key,
            data_file_id = Safe_Str__Id("binary-update-002")
        )

    def test__init__(self):                                                                     # Test initialization and structure
        with Routes__Data__Update() as _:
            assert type(_)                     is Routes__Data__Update
            assert base_classes(_)             == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                       == TAG__ROUTES_UPDATE__DATA
            assert _.prefix                    == PREFIX__ROUTES_UPDATE__DATA
            assert type(_.cache_service)       is Cache__Service
            assert type(_.update_service())    is Cache__Service__Data__Update

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_UPDATE__DATA       == 'data/update'
        assert PREFIX__ROUTES_UPDATE__DATA    == '/{namespace}/cache/{cache_id}'
        assert len(ROUTES_PATHS__UPDATE__DATA) == 6
        assert ROUTES_PATHS__UPDATE__DATA[0]  == '/{namespace}/cache/{cache_id}/data/update/string/{data_file_id}'
        assert ROUTES_PATHS__UPDATE__DATA[1]  == '/{namespace}/cache/{cache_id}/data/update/string/{data_key:path}/{data_file_id}'
        assert ROUTES_PATHS__UPDATE__DATA[2]  == '/{namespace}/cache/{cache_id}/data/update/json/{data_file_id}'
        assert ROUTES_PATHS__UPDATE__DATA[3]  == '/{namespace}/cache/{cache_id}/data/update/json/{data_key:path}/{data_file_id}'
        assert ROUTES_PATHS__UPDATE__DATA[4]  == '/{namespace}/cache/{cache_id}/data/update/binary/{data_file_id}'
        assert ROUTES_PATHS__UPDATE__DATA[5]  == '/{namespace}/cache/{cache_id}/data/update/binary/{data_key:path}/{data_file_id}'

    def test_data__update_string__with__id(self):                                               # Test string update with ID only
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "update test parent"                   ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            self.routes_data_store.data__store_string__with__id(data         = "original data"          ,
                                                                cache_id     = parent.cache_id          ,
                                                                namespace    = self.test_namespace      ,
                                                                data_file_id = Safe_Str__Id("upd-str-1"))

            # Update it
            result = _.data__update_string__with__id(data         = "updated data"                      ,
                                                     cache_id     = parent.cache_id                     ,
                                                     namespace    = self.test_namespace                 ,
                                                     data_file_id = Safe_Str__Id("upd-str-1")           )

            assert type(result)       is Schema__Cache__Data__Update__Response
            assert result.success     is True
            assert result.cache_id    == parent.cache_id
            assert result.namespace   == self.test_namespace
            assert result.data_type   == Enum__Cache__Data_Type.STRING
            assert result.data_file_id == "upd-str-1"
            assert result.file_size   == len("updated data".encode('utf-8'))

            # Verify the update
            retrieved = self.routes_data_retrieve.data__string__with__id(cache_id     = parent.cache_id     ,
                                                                         namespace    = self.test_namespace ,
                                                                         data_file_id = Safe_Str__Id("upd-str-1"))
            assert retrieved.body == b"updated data"

    def test_data__update_string__with__id_and_key(self):                                       # Test string update with ID and key
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "update key parent"                    ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            test_key = Safe_Str__File__Path("update/path")

            self.routes_data_store.data__store_string__with__id_and_key(data         = "original keyed"         ,
                                                                        cache_id     = parent.cache_id          ,
                                                                        namespace    = self.test_namespace      ,
                                                                        data_key     = test_key                 ,
                                                                        data_file_id = Safe_Str__Id("upd-str-k"))

            # Update it
            result = _.data__update_string__with__id_and_key(data         = "updated keyed"                     ,
                                                             cache_id     = parent.cache_id                     ,
                                                             namespace    = self.test_namespace                 ,
                                                             data_key     = test_key                            ,
                                                             data_file_id = Safe_Str__Id("upd-str-k")           )

            assert result.success  is True
            assert result.data_key == test_key

            # Verify the update
            retrieved = self.routes_data_retrieve.data__string__with__id_and_key(cache_id     = parent.cache_id     ,
                                                                                 namespace    = self.test_namespace ,
                                                                                 data_key     = test_key            ,
                                                                                 data_file_id = Safe_Str__Id("upd-str-k"))
            assert retrieved.body == b"updated keyed"

    def test_data__update_json__with__id(self):                                                 # Test JSON update with ID only
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "json update parent"                   ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            self.routes_data_store.data__store_json__with__id(data         = {"original": True}         ,
                                                              cache_id     = parent.cache_id            ,
                                                              namespace    = self.test_namespace        ,
                                                              data_file_id = Safe_Str__Id("upd-json-1") )

            # Update it
            result = _.data__update_json__with__id(data         = {"updated": True, "version": 2}       ,
                                                   cache_id     = parent.cache_id                       ,
                                                   namespace    = self.test_namespace                   ,
                                                   data_file_id = Safe_Str__Id("upd-json-1")            )

            assert result.success   is True
            assert result.data_type == Enum__Cache__Data_Type.JSON

            # Verify the update
            retrieved = self.routes_data_retrieve.data__json__with__id(cache_id     = parent.cache_id       ,
                                                                       namespace    = self.test_namespace   ,
                                                                       data_file_id = Safe_Str__Id("upd-json-1"))
            assert retrieved == {"updated": True, "version": 2}

    def test_data__update_json__with__id_and_key(self):                                         # Test JSON update with ID and key
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "json key parent"                      ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            test_key = Safe_Str__File__Path("json/update")

            self.routes_data_store.data__store_json__with__id_and_key(data         = {"original": "nested"}     ,
                                                                      cache_id     = parent.cache_id            ,
                                                                      namespace    = self.test_namespace        ,
                                                                      data_key     = test_key                   ,
                                                                      data_file_id = Safe_Str__Id("upd-json-k") )

            # Update it
            result = _.data__update_json__with__id_and_key(data         = {"updated": "nested", "new": 123}     ,
                                                           cache_id     = parent.cache_id                       ,
                                                           namespace    = self.test_namespace                   ,
                                                           data_key     = test_key                              ,
                                                           data_file_id = Safe_Str__Id("upd-json-k")            )

            assert result.success  is True
            assert result.data_key == test_key

            # Verify the update
            retrieved = self.routes_data_retrieve.data__json__with__id_and_key(cache_id     = parent.cache_id       ,
                                                                               namespace    = self.test_namespace   ,
                                                                               data_key     = test_key              ,
                                                                               data_file_id = Safe_Str__Id("upd-json-k"))
            assert retrieved == {"updated": "nested", "new": 123}

    def test_data__update_binary__with__id(self):                                               # Test binary update with ID only
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "binary update parent"                 ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            self.routes_data_store.data__store_binary__with__id(body         = b'ORIGINAL\x00'          ,
                                                                cache_id     = parent.cache_id          ,
                                                                namespace    = self.test_namespace      ,
                                                                data_file_id = Safe_Str__Id("upd-bin-1"))

            # Update it
            result = _.data__update_binary__with__id(body         = b'UPDATED\x01\x02'                  ,
                                                     cache_id     = parent.cache_id                     ,
                                                     namespace    = self.test_namespace                 ,
                                                     data_file_id = Safe_Str__Id("upd-bin-1")           )

            assert result.success   is True
            assert result.data_type == Enum__Cache__Data_Type.BINARY
            assert result.file_size == len(b'UPDATED\x01\x02')

            # Verify the update
            retrieved = self.routes_data_retrieve.data__binary__with__id(cache_id     = parent.cache_id     ,
                                                                         namespace    = self.test_namespace ,
                                                                         data_file_id = Safe_Str__Id("upd-bin-1"))
            assert retrieved.body == b'UPDATED\x01\x02'

    def test_data__update_binary__with__id_and_key(self):                                       # Test binary update with ID and key
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "binary key parent"                    ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            test_key = Safe_Str__File__Path("binary/update")

            self.routes_data_store.data__store_binary__with__id_and_key(body         = b'original keyed binary'     ,
                                                                        cache_id     = parent.cache_id              ,
                                                                        namespace    = self.test_namespace          ,
                                                                        data_key     = test_key                     ,
                                                                        data_file_id = Safe_Str__Id("upd-bin-k")    )

            # Update it
            result = _.data__update_binary__with__id_and_key(body         = b'updated keyed binary'                 ,
                                                             cache_id     = parent.cache_id                         ,
                                                             namespace    = self.test_namespace                     ,
                                                             data_key     = test_key                                ,
                                                             data_file_id = Safe_Str__Id("upd-bin-k")               )

            assert result.success  is True
            assert result.data_key == test_key

            # Verify the update
            retrieved = self.routes_data_retrieve.data__binary__with__id_and_key(cache_id     = parent.cache_id     ,
                                                                                 namespace    = self.test_namespace ,
                                                                                 data_key     = test_key            ,
                                                                                 data_file_id = Safe_Str__Id("upd-bin-k"))
            assert retrieved.body == b'updated keyed binary'

    def test_data__update_string__empty_data(self):                                             # Test error when string data is empty
        with self.routes_data_update as _:
            with pytest.raises(HTTPException) as exc_info:
                _.data__update_string__with__id(data         = ""                               ,
                                                cache_id     = self.parent_cache_id             ,
                                                namespace    = self.test_namespace              ,
                                                data_file_id = Safe_Str__Id("any")              )

            error = exc_info.value
            assert error.status_code          == 400
            assert error.detail["error_type"] == "INVALID_INPUT"
            assert "empty"                   in error.detail["message"].lower()

    def test_data__update_binary__empty_data(self):                                             # Test error when binary data is empty
        with self.routes_data_update as _:
            with pytest.raises(HTTPException) as exc_info:
                _.data__update_binary__with__id(body         = b""                              ,
                                                cache_id     = self.parent_cache_id             ,
                                                namespace    = self.test_namespace              ,
                                                data_file_id = Safe_Str__Id("any")              )

            error = exc_info.value
            assert error.status_code          == 400
            assert error.detail["error_type"] == "INVALID_INPUT"

    def test_data__update__non_existent_file(self):                                             # Test updating non-existent data file
        with self.routes_data_update as _:
            with pytest.raises(HTTPException) as exc_info:
                _.data__update_string__with__id(data         = "new data"                       ,
                                                cache_id     = self.parent_cache_id             ,
                                                namespace    = self.test_namespace              ,
                                                data_file_id = Safe_Str__Id("non-existent")     )

            error = exc_info.value
            assert error.status_code          == 404
            assert error.detail["error_type"] == "NOT_FOUND"

    def test_data__update__non_existent_parent(self):                                           # Test updating with non-existent parent
        with self.routes_data_update as _:
            non_existent_id = Random_Guid()

            with pytest.raises(HTTPException) as exc_info:
                _.data__update_string__with__id(data         = "new data"                       ,
                                                cache_id     = non_existent_id                  ,
                                                namespace    = self.test_namespace              ,
                                                data_file_id = Safe_Str__Id("any")              )

            error = exc_info.value
            assert error.status_code          == 404
            assert error.detail["error_type"] == "NOT_FOUND"

    def test_data__update__delegates_to_with_key(self):                                         # Test with__id delegates to with__id_and_key
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "delegate test"                        ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            self.routes_data_store.data__store_string__with__id(data         = "for delegate test"      ,
                                                                cache_id     = parent.cache_id          ,
                                                                namespace    = self.test_namespace      ,
                                                                data_file_id = Safe_Str__Id("delegate") )

            # Update using with__id (should delegate to with__id_and_key with empty key)
            result = _.data__update_string__with__id(data         = "delegated update"                  ,
                                                     cache_id     = parent.cache_id                     ,
                                                     namespace    = self.test_namespace                 ,
                                                     data_file_id = Safe_Str__Id("delegate")            )

            assert result.success  is True
            assert result.data_key == ''                                                                 # Empty key from delegation

    def test_data__update__special_characters(self):                                            # Test updating with special characters
        with self.routes_data_update as _:
            # Create fresh data
            parent = self.routes_store.store__string(data      = "special char parent"                  ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            self.routes_data_store.data__store_string__with__id(data         = "original"               ,
                                                                cache_id     = parent.cache_id          ,
                                                                namespace    = self.test_namespace      ,
                                                                data_file_id = Safe_Str__Id("special")  )

            special_data = "Updated with 中文 and émojis 🚀"

            result = _.data__update_string__with__id(data         = special_data                        ,
                                                     cache_id     = parent.cache_id                     ,
                                                     namespace    = self.test_namespace                 ,
                                                     data_file_id = Safe_Str__Id("special")             )

            assert result.success   is True
            assert result.file_size > len(special_data)                                                  # UTF-8 encoding

            # Verify
            retrieved = self.routes_data_retrieve.data__string__with__id(cache_id     = parent.cache_id     ,
                                                                         namespace    = self.test_namespace ,
                                                                         data_file_id = Safe_Str__Id("special"))
            assert retrieved.body.decode('utf-8') == special_data

    def test__integration__store_update_retrieve_cycle(self):                                   # Full integration test
        with self.routes_data_update as _:
            # Create parent
            parent = self.routes_store.store__string(data      = "integration update parent"            ,
                                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL  ,
                                                     namespace = self.test_namespace                    )

            test_key = Safe_Str__File__Path("integration/update")

            # Store original data
            self.routes_data_store.data__store_string__with__id_and_key(data         = "original string"        ,
                                                                        cache_id     = parent.cache_id          ,
                                                                        namespace    = self.test_namespace      ,
                                                                        data_key     = test_key                 ,
                                                                        data_file_id = Safe_Str__Id("integ-str"))

            self.routes_data_store.data__store_json__with__id_and_key(data         = {"original": True}         ,
                                                                      cache_id     = parent.cache_id            ,
                                                                      namespace    = self.test_namespace        ,
                                                                      data_key     = test_key                   ,
                                                                      data_file_id = Safe_Str__Id("integ-json") )

            self.routes_data_store.data__store_binary__with__id_and_key(body         = b'original binary'       ,
                                                                        cache_id     = parent.cache_id          ,
                                                                        namespace    = self.test_namespace      ,
                                                                        data_key     = test_key                 ,
                                                                        data_file_id = Safe_Str__Id("integ-bin"))

            # Update all
            _.data__update_string__with__id_and_key(data         = "updated string"                     ,
                                                    cache_id     = parent.cache_id                      ,
                                                    namespace    = self.test_namespace                  ,
                                                    data_key     = test_key                             ,
                                                    data_file_id = Safe_Str__Id("integ-str")            )

            _.data__update_json__with__id_and_key(data         = {"updated": True, "version": 2}        ,
                                                  cache_id     = parent.cache_id                        ,
                                                  namespace    = self.test_namespace                    ,
                                                  data_key     = test_key                               ,
                                                  data_file_id = Safe_Str__Id("integ-json")             )

            _.data__update_binary__with__id_and_key(body         = b'updated binary'                    ,
                                                    cache_id     = parent.cache_id                      ,
                                                    namespace    = self.test_namespace                  ,
                                                    data_key     = test_key                             ,
                                                    data_file_id = Safe_Str__Id("integ-bin")            )

            # Verify all updates
            str_data = self.routes_data_retrieve.data__string__with__id_and_key(cache_id     = parent.cache_id          ,
                                                                                namespace    = self.test_namespace      ,
                                                                                data_key     = test_key                 ,
                                                                                data_file_id = Safe_Str__Id("integ-str"))
            assert str_data.body == b"updated string"

            json_data = self.routes_data_retrieve.data__json__with__id_and_key(cache_id     = parent.cache_id           ,
                                                                               namespace    = self.test_namespace       ,
                                                                               data_key     = test_key                  ,
                                                                               data_file_id = Safe_Str__Id("integ-json"))
            assert json_data == {"updated": True, "version": 2}

            bin_data = self.routes_data_retrieve.data__binary__with__id_and_key(cache_id     = parent.cache_id          ,
                                                                                namespace    = self.test_namespace      ,
                                                                                data_key     = test_key                 ,
                                                                                data_file_id = Safe_Str__Id("integ-bin"))
            assert bin_data.body == b'updated binary'