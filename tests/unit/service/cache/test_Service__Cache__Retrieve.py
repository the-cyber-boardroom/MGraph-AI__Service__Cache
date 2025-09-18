from unittest                                                                           import TestCase

from osbot_utils.helpers.duration.decorators.capture_duration import capture_duration
from osbot_utils.testing.__                                                             import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                   import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.Timestamp_Now                 import Timestamp_Now
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id         import Safe_Str__Id
from osbot_utils.utils.Objects                                                          import base_classes
from memory_fs.schemas.Safe_Str__Cache_Hash                                             import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Metadata                      import Schema__Cache__Metadata
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Success             import Schema__Cache__Retrieve__Success
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                 import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Gone                  import Schema__Cache__Error__Gone
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Not_Found             import Schema__Cache__Error__Not_Found
from mgraph_ai_service_cache.service.cache.Service__Cache__Retrieve                     import Service__Cache__Retrieve
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache.utils.for_osbot_utils.Random_Hash                          import Random_Hash
from tests.unit.Service__Fast_API__Test_Objs                                            import setup__service_fast_api_test_objs


class test_Service__Cache__Retrieve(TestCase):

    @classmethod
    def setUpClass(cls):
        with capture_duration() as duration:
            setup__service_fast_api_test_objs()

            cls.temp_bucket_name = "test-retrieve-service"
            cls.cache_service    = Cache__Service          (default_bucket = cls.temp_bucket_name)
            cls.retrieve_service = Service__Cache__Retrieve(cache_service  = cls.cache_service  )
            cls.test_namespace   = Safe_Str__Id("test-retrieve")
            cls.test_string      = "test retrieve string data"
            cls.test_json        = {"key": "value", "number": 42}
        assert duration.seconds < 0.15

        with capture_duration() as duration_2nd:
            setup__service_fast_api_test_objs()
        assert duration_2nd.seconds == 0

    @classmethod
    def tearDownClass(cls):                                                          # ONE-TIME cleanup
        # Clean up test bucket
        for handler in cls.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.temp_bucket_name):
                    s3.bucket_delete_all_files(cls.temp_bucket_name)
                    s3.bucket_delete(cls.temp_bucket_name)

    def test__init__(self):                                                           # Test auto-initialization
        with Service__Cache__Retrieve() as _:
            assert type(_)               is Service__Cache__Retrieve
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

            assert _.obj() == __(cache_service=__(default_bucket    = 'mgraph-ai-cache',
                                                  default_ttl_hours = 24,
                                                  cache_handlers    = __(),
                                                  hash_config       = __(algorithm = 'sha256', length=16),
                                                  hash_generator    = __(config    = __(algorithm='sha256', length=16))))

    def test_retrieve_by_hash__not_found(self):                                      # Test retrieval of non-existent hash
        with self.retrieve_service as _:
            non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")
            result = _.retrieve_by_hash(non_existent_hash, self.test_namespace)

            assert result is None                                                     # Service returns None for not found

    def test_retrieve_by_id__not_found(self):                                        # Test retrieval of non-existent ID
        with self.retrieve_service as _:
            non_existent_id = Random_Guid()
            result = _.retrieve_by_id(non_existent_id, self.test_namespace)

            assert result is None                                                     # Service returns None for not found

    def test_check_exists(self):                                                     # Test existence check
        with self.retrieve_service as _:
            non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")            # Check non-existent
            exists = _.check_exists(non_existent_hash, self.test_namespace)

            assert exists is False

    def test_get_not_found_error(self):                                             # Test error response building
        with self.retrieve_service as _:
            cache_id   = Random_Guid()
            cache_hash = Safe_Str__Cache_Hash("abc123def456789")

            error = _.get_not_found_error(cache_id   = cache_id           ,
                                          cache_hash = cache_hash         ,
                                          namespace  = self.test_namespace)

            assert type(error)         is Schema__Cache__Error__Not_Found
            assert error.error_type    == "NOT_FOUND"
            assert error.resource_type == "cache_entry"
            assert error.cache_id      == cache_id
            assert error.request_id    != cache_id                                      # confirm these two are not the same
            assert error.cache_hash    == cache_hash
            assert error.namespace     == self.test_namespace
            assert error.message       == "The requested cache entry was not found"
            assert error.obj()         == __(resource_id    =  None                                     ,
                                             cache_hash     = 'abc123def456789'                         ,
                                             cache_id       = cache_id                                  ,
                                             namespace      = 'test-retrieve'                           ,
                                             error_type     = 'NOT_FOUND'                               ,
                                             message        = 'The requested cache entry was not found' ,
                                             timestamp      = __SKIP__                                  ,
                                             request_id     = __SKIP__                                  ,
                                             resource_type  = 'cache_entry'                             )

    def test_get_expired_error(self):                                                # Test expired error response
        with self.retrieve_service as _:
            cache_id   = Random_Guid  ()
            expired_at = Timestamp_Now()
            error = _.get_expired_error(cache_id   = cache_id           ,
                                        expired_at = expired_at         ,
                                        ttl_hours  = 24                 ,
                                        namespace  = self.test_namespace)

            assert type(error)      is Schema__Cache__Error__Gone
            assert error.error_type == "EXPIRED"
            assert error.cache_id   == cache_id
            assert error.expired_at == expired_at
            assert error.ttl_hours  == 24
            assert error.namespace  == self.test_namespace

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
            cache_hash = self.cache_service.hash_from_string(self.test_string)      # Store some data first
            cache_id   = Random_Guid()

            store_result = self.cache_service.store_with_strategy(storage_data = self.test_string   ,
                                                                  cache_hash   = cache_hash         ,
                                                                  cache_id     = cache_id           ,
                                                                  strategy     = "temporal"         ,
                                                                  namespace    = self.test_namespace)
            assert store_result.obj() == __(cache_id = cache_id           ,
                                            hash      = 'e15b31f87df1896e',
                                            namespace = 'test-retrieve'   ,
                                            paths     =__SKIP__           ,
                                            size      = 27                )
            result_by_hash = _.retrieve_by_hash(cache_hash, self.test_namespace)                # Now retrieve by hash

            assert type(result_by_hash)             is Schema__Cache__Retrieve__Success
            assert result_by_hash.data              == self.test_string
            assert result_by_hash.data_type         == Enum__Cache__Data_Type.STRING
            assert type(result_by_hash.metadata)    is Schema__Cache__Metadata
            assert result_by_hash.metadata.cache_id == cache_id

            # Retrieve by ID
            result_by_id = _.retrieve_by_id(cache_id, self.test_namespace)
            assert result_by_id.data  == self.test_string
            assert result_by_id.obj() == __(data      = 'test retrieve string data'       ,
                                            metadata  = __(cache_id         = cache_id          ,
                                                           cache_hash       = 'e15b31f87df1896e',
                                                           cache_key        = 'None'            ,
                                                           file_id          = cache_id          ,
                                                           namespace        = 'test-retrieve'   ,
                                                           strategy         = 'temporal'        ,
                                                           stored_at        =  __SKIP__         ,
                                                           file_type        = 'json'            ,
                                                           content_encoding = None              ,
                                                           content_size     = 0                 ),
                                            data_type = 'string'                                 )
            assert result_by_hash.obj()  == result_by_id.obj()                           # these should be the same

    def test_retrieve_json_data(self):                                               # Test JSON data retrieval
        with self.retrieve_service as _:
            cache_hash = self.cache_service.hash_from_json(self.test_json)          # Store JSON data
            cache_id   = Random_Guid()

            self.cache_service.store_with_strategy(storage_data = self.test_json     ,
                                                   cache_hash   = cache_hash         ,
                                                   cache_id     = cache_id           ,
                                                   strategy     = "direct"           ,
                                                   namespace    = self.test_namespace)

            result = _.retrieve_by_hash(cache_hash, self.test_namespace)                    # Retrieve

            assert result.data      == self.test_json
            assert result.data_type == Enum__Cache__Data_Type.JSON

    def test_retrieve_binary_data(self):                                                # Test binary data retrieval
        with self.retrieve_service as _:
            binary_data = b'\x01\x02\x03\x04\x05'                                       # Store binary data
            cache_hash  = self.cache_service.hash_from_bytes(binary_data)
            cache_id    = Random_Guid()

            self.cache_service.store_with_strategy(storage_data = binary_data        ,
                                                   cache_hash   = cache_hash         ,
                                                   cache_id     = cache_id           ,
                                                   strategy     = "temporal_latest"  ,
                                                   namespace    = self.test_namespace)

            result = _.retrieve_by_id(cache_id, self.test_namespace)                    # Retrieve

            assert result.data      == binary_data
            assert result.data_type == Enum__Cache__Data_Type.BINARY
            assert result.obj()     == __(data=b'\x01\x02\x03\x04\x05',
                                          metadata=__(cache_id         = cache_id           ,
                                                      cache_hash       = '74f81fe167d99b4c' ,
                                                      cache_key        = 'None'             ,
                                                      file_id          = cache_id           ,
                                                      namespace        = 'test-retrieve'    ,
                                                      strategy         = 'temporal_latest'  ,
                                                      stored_at        = __SKIP__           ,
                                                      file_type        = 'binary'           ,
                                                      content_encoding = None               ,
                                                      content_size     = 0                  ),  # BUG: the content_size should not be 0
                                          data_type='binary')


    def test_default_namespace(self):                                                # Test default namespace handling
        with self.retrieve_service as _:
            result = _.retrieve_by_hash(Safe_Str__Cache_Hash("0000000000000000"), None) # Test with None namespace (should use "default")
            assert result is None                                                       # Not found, but namespace was handled