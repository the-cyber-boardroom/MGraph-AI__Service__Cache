from typing                                                                              import Dict, List
from unittest                                                                            import TestCase
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                              import CACHE__TEST__FIXTURES__NAMESPACE
from osbot_fast_api_serverless.utils.testing.skip_tests                                  import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                              import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Hash                    import Random_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now         import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.utils.Misc                                                              import list_set
from osbot_utils.utils.Objects                                                           import base_classes, obj
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Metadata                       import Schema__Cache__Metadata
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Success              import Schema__Cache__Retrieve__Success
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                  import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Gone                   import Schema__Cache__Error__Gone
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Not_Found              import Schema__Cache__Error__Not_Found
from mgraph_ai_service_cache.service.cache.Service__Cache__Retrieve                      import Service__Cache__Retrieve
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache.service.cache.Service__Cache__Store                         import Service__Cache__Store
from tests.unit.Service__Cache__Test_Objs                                                import setup__service__cache__test_objs


class test_Service__Cache__Retrieve(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_objs          = setup__service__cache__test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.retrieve_service   = Service__Cache__Retrieve(cache_service  = cls.cache_service  )
        cls.store_service      = Service__Cache__Store   (cache_service  = cls.cache_service  )
        cls.namespace          = cls.fixtures_namespace
        cls.test_string        = "test retrieve string data"
        cls.test_json          = {"key": "value", "number": 42}

        #assert cls.test_objs.duration < 0.6                                # setup tool less than 600ms


    def test__init__(self):                                                           # Test auto-initialization
        with Service__Cache__Retrieve() as _:
            assert type(_)               is Service__Cache__Retrieve
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

            assert _.obj() == __( cache_service=__(cache_config=__(storage_mode='memory',
                                                   default_bucket    = None ,
                                                   default_ttl_hours = 24   ,
                                                   local_disk_path   = None ,
                                                   sqlite_path       = None ,
                                                   zip_path          = None ),
                                  cache_handlers    = __()                      ,
                                  hash_config       = __(algorithm = 'sha256', length=16),
                                  hash_generator    = __(config    = __(algorithm='sha256', length=16))))

    def test_retrieve_by_hash__not_found(self):                                      # Test retrieval of non-existent hash
        with self.retrieve_service as _:
            non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")
            result = _.retrieve_by_hash(non_existent_hash, self.namespace)

            assert result is None                                                     # Service returns None for not found

    def test_retrieve_by_id__not_found(self):                                        # Test retrieval of non-existent ID
        with self.retrieve_service as _:
            non_existent_id = Random_Guid()
            result = _.retrieve_by_id(non_existent_id, self.namespace)

            assert result is None                                                     # Service returns None for not found

    def test_check_exists(self):                                                     # Test existence check
        with self.retrieve_service as _:
            non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")            # Check non-existent
            exists = _.check_exists(non_existent_hash, self.namespace)

            assert exists is False

    def test__regression__type_safe_assigment__not_handling_dict_with_List(self):
        class An_Class_1(Type_Safe):
            all_paths      : Dict[Safe_Str__Id, List[Safe_Str__File__Path]]

        data = { 'by_hash': [ 'refs/by-hash/e1/5b/e15b31f87df1896e.json',
                               'refs/by-hash/e1/5b/e15b31f87df1896e.json.config',
                               'refs/by-hash/e1/5b/e15b31f87df1896e.json.metadata'],
                  'by_id': [ 'refs/by-id/be/40/be40eef6-9785-4be1-a6b1-b8da6cee51a4.json',
                             'refs/by-id/be/40/be40eef6-9785-4be1-a6b1-b8da6cee51a4.json.config',
                             'refs/by-id/be/40/be40eef6-9785-4be1-a6b1-b8da6cee51a4.json.metadata'],
                  'data': [ 'data/direct/be/40/be40eef6-9785-4be1-a6b1-b8da6cee51a4.json',
                            'data/direct/be/40/be40eef6-9785-4be1-a6b1-b8da6cee51a4.json.config',
                            'data/direct/be/40/be40eef6-9785-4be1-a6b1-b8da6cee51a4.json.metadata']}

        # error_message_1 = "Type List cannot be instantiated; use list() instead"                      # BUG 1 with List
        # with pytest.raises(TypeError, match=re.escape(error_message_1)):
        #     An_Class_1().all_paths = data
        An_Class_1().all_paths = data                                                                   # FIXED
        an_class_1 = An_Class_1()
        an_class_1.all_paths = data                                                                     # FIXED:
        assert an_class_1.json()    == {"all_paths": data }                                             # confirm roundtrip with .json()
        assert an_class_1.obj()     == __(all_paths = obj(data))                                        # confirm roundtrip with obj().

    def test_get_entry_details(self):
        cache_id  = self.cache_fixtures.get_fixture_id('string_simple')
        with obj(self.cache_service.retrieve_by_id__config(cache_id, self.namespace)) as _:
            all_paths     = _.all_paths
            timestamp     = _.timestamp
            content_paths = _.content_paths

        with self.retrieve_service as _:
            entry_details = _.get_entry_details(cache_id, self.namespace)
            assert entry_details.obj() == __(cache_id      = cache_id                        ,
                                             cache_hash    = 'e15b31f87df1896e'              ,
                                             namespace     = CACHE__TEST__FIXTURES__NAMESPACE,
                                             strategy      = 'direct'                        ,
                                             all_paths     = all_paths                       ,
                                             content_paths = content_paths                   ,
                                             file_type     = 'json'                          ,
                                             timestamp     = str(timestamp)                  )
    def test_get_entry_details__all(self):
        skip__if_not__in_github_actions()                                               # this takes a bit since this will load 10x docs from storage
        cache_id      = self.cache_fixtures.get_fixture_id('string_simple')
        with self.retrieve_service as _:
            entry_details_all = _.get_entry_details__all(cache_id, self.namespace)
            assert len(list_set(entry_details_all.get('details'))) == 8                 # todo: add better tests here

    def test_get_not_found_error(self):                                             # Test error response building
        with self.retrieve_service as _:
            cache_id   = Random_Guid()
            cache_hash = Safe_Str__Cache_Hash("abc123def456789")

            error = _.get_not_found_error(cache_id   = cache_id,
                                          cache_hash = cache_hash,
                                          namespace  = self.namespace)

            assert type(error)         is Schema__Cache__Error__Not_Found
            assert error.error_type    == "NOT_FOUND"
            assert error.resource_type == "cache_entry"
            assert error.cache_id      == cache_id
            assert error.request_id    != cache_id                                      # confirm these two are not the same
            assert error.cache_hash    == cache_hash
            assert error.namespace     == self.namespace
            assert error.message       == "The requested cache entry was not found"
            assert error.obj()         == __(resource_id    =  None                                     ,
                                             cache_hash     = 'abc123def456789'                         ,
                                             cache_id       = cache_id                                  ,
                                             namespace      = self.namespace                            ,
                                             error_type     = 'NOT_FOUND'                               ,
                                             message        = 'The requested cache entry was not found' ,
                                             timestamp      = __SKIP__                                  ,
                                             request_id     = __SKIP__                                  ,
                                             resource_type  = 'cache_entry'                             )

    def test_get_expired_error(self):                                                # Test expired error response
        with self.retrieve_service as _:
            cache_id   = Random_Guid  ()
            expired_at = Timestamp_Now()
            error = _.get_expired_error(cache_id   = cache_id,
                                        expired_at = expired_at,
                                        ttl_hours  = 24,
                                        namespace  = self.namespace)

            assert type(error)      is Schema__Cache__Error__Gone
            assert error.error_type == "EXPIRED"
            assert error.cache_id   == cache_id
            assert error.expired_at == expired_at
            assert error.ttl_hours  == 24
            assert error.namespace  == self.namespace

    def test__build_metadata(self):                                                  # Test metadata building
        cache_id   = Random_Guid()
        cache_hash = Random_Hash()
        stored_at  = Timestamp_Now()
        with self.retrieve_service as _:
            cache_result = {"metadata": { "cache_id"        : cache_id      ,
                                          "cache_hash"      : cache_hash    ,
                                          "namespace"       : "test-ns"     ,
                                          "strategy"        : "temporal"    ,
                                          "stored_at"       : stored_at     ,
                                          "file_type"       : "json"        ,
                                          "content__size"   : 1024          }}

            metadata = _._build_metadata(cache_result)

            assert type(metadata)          is Schema__Cache__Metadata
            assert metadata.cache_id       == cache_id
            assert metadata.cache_hash     == cache_hash
            assert metadata.namespace      == "test-ns"
            assert metadata.strategy       == "temporal"
            assert metadata.stored_at      == stored_at
            assert metadata.file_type      == "json"
            assert metadata.content_size   == 1024

    def test__determine_data_type(self):                                             # Test data type determination
        with self.retrieve_service as _:
            # Test binary type
            binary_result = {"data_type": "binary"}
            assert _._determine_data_type(binary_result) == Enum__Cache__Data_Type.BINARY

            # Test JSON type
            json_result = {"data_type": "json"}
            assert _._determine_data_type(json_result) == Enum__Cache__Data_Type.JSON

            # Test string type (default)
            string_result = {"data_type": "string"}
            assert _._determine_data_type(string_result) == Enum__Cache__Data_Type.STRING

            # Test missing type (defaults to string)
            no_type_result = {}
            assert _._determine_data_type(no_type_result) == Enum__Cache__Data_Type.STRING

    def test__is_expired(self):                                                      # Test expiration check
        with self.retrieve_service as _:
            # Currently always returns False (TODO implementation)
            id_ref = {"ttl_expiry": "2020-01-01T00:00:00Z"}                        # Old date

            assert _._is_expired(id_ref) is False                                   # Not implemented yet

    def test_retrieve_workflow_with_real_data(self):                                 # Test with actual cache service
        with self.retrieve_service as _:
            cache_id   = self.cache_fixtures.get_fixture_id  ('string_simple')
            cache_hash = self.cache_fixtures.get_fixture_hash('string_simple')


            result_by_hash = _.retrieve_by_hash(cache_hash, self.namespace)                 # Now retrieve by hash
            assert cache_hash                       == 'e15b31f87df1896e'                   # this should not change
            assert type(result_by_hash)             is Schema__Cache__Retrieve__Success
            assert result_by_hash.data              == self.test_string
            assert result_by_hash.data_type         == Enum__Cache__Data_Type.STRING
            assert type(result_by_hash.metadata)    is Schema__Cache__Metadata
            assert result_by_hash.metadata.cache_id == cache_id

            # Retrieve by ID
            result_by_id = _.retrieve_by_id(cache_id, self.namespace)
            assert result_by_id.data  == self.test_string
            assert result_by_id.obj() == __(data      = 'test retrieve string data'             ,
                                            metadata  = __(cache_id         = cache_id          ,
                                                           cache_hash       = cache_hash        ,
                                                           cache_key        = 'None'            ,
                                                           file_id          = cache_id          ,
                                                           namespace        = self.namespace    ,
                                                           strategy         = 'direct'          ,
                                                           stored_at        =  __SKIP__         ,
                                                           file_type        = 'json'            ,
                                                           content_encoding = None              ,
                                                           content_size     = 0                 ),      # BUG : we should be returning len(data) here
                                            data_type = 'string'                                 )
            assert result_by_hash.obj()  == result_by_id.obj()                           # these should be the same

    def test_retrieve_json_data(self):                                               # Test JSON data retrieval
        with self.retrieve_service as _:
            cache_id   = self.cache_fixtures.get_fixture_id    ('json_simple')
            cache_hash = self.cache_fixtures.get_fixture_hash('json_simple')

            result = _.retrieve_by_hash(cache_hash, self.namespace)                    # Retrieve
            assert type(result)              is Schema__Cache__Retrieve__Success
            assert type(result.metadata)     is Schema__Cache__Metadata
            assert result.data               == self.test_json
            assert result.data_type          == Enum__Cache__Data_Type.JSON
            assert result.metadata.cache_id  == cache_id

    def test_retrieve_binary_data(self):                                                # Test binary data retrieval
        with self.retrieve_service as _:
            binary_data  = b'\x01\x02\x03\x04\x05'
            cache_id     = self.cache_fixtures.get_fixture_id    ('binary_small')
            result       = _.retrieve_by_id(cache_id, self.namespace)                    # Retrieve

            assert result.data      == binary_data
            assert result.data_type == Enum__Cache__Data_Type.BINARY
            assert result.obj()     == __(data=b'\x01\x02\x03\x04\x05',
                                          metadata=__(cache_id         = cache_id           ,
                                                      cache_hash       = '74f81fe167d99b4c' ,
                                                      cache_key        = 'None'             ,
                                                      file_id          = cache_id           ,
                                                      namespace        = self.namespace     ,
                                                      strategy         = 'direct'           ,
                                                      stored_at        = __SKIP__           ,
                                                      file_type        = 'binary'           ,
                                                      content_encoding = None               ,
                                                      content_size     = 0                  ),  # BUG: the content_size should not be 0
                                          data_type='binary')


    def test_default_namespace(self):                                                # Test default namespace handling
        with self.retrieve_service as _:
            result = _.retrieve_by_hash(Safe_Str__Cache_Hash("0000000000000000"), None) # Test with None namespace (should use "default")
            assert result is None                                                       # Not found, but namespace was handled