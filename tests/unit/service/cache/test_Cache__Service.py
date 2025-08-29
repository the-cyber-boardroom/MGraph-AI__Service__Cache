from unittest                                                                       import TestCase
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.testing.__                                                         import __
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import Safe_Str__Hash
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path      import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.http.Safe_Str__Http__Content_Type    import Safe_Str__Http__Content_Type
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_utils.type_safe.primitives.safe_str.text.Safe_Str__Text                  import Safe_Str__Text
from osbot_utils.type_safe.type_safe_core.collections.Type_Safe__Dict               import Type_Safe__Dict
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import random_string_short, str_to_base64, list_set
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_aws.AWS_Config                                                           import aws_config
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service, DEFAULT__CACHE__SERVICE__BUCKET_NAME, DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
from mgraph_ai_service_cache.service.cache.Cache__Handler                           import Cache__Handler
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request            import Schema__Cache__Store__Request
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Retrieve__Request         import Schema__Cache__Retrieve__Request
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__SHA1__Short                   import Safe_Str__SHA1__Short
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs


class test_Cache__Service(TestCase):                                                 # Test main cache service orchestrator

    @classmethod
    def setUpClass(cls):                                                             # ONE-TIME expensive setup
        cls.test_objs   = setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-service-"))

        assert aws_config.account_id () == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        with Cache__Service() as _:
            cls.service          = _
            _.default_bucket     = cls.test_bucket
            _.default_ttl_hours  = 12

        # Test data
        cls.test_namespace       = Safe_Id("test-namespace")
        cls.test_data            = { "key"   : "value" ,
                                     "nested": {"inner": "data"}}
        cls.test_metadata        = { Safe_Id("author" ): Safe_Str__Text("test-user"),
                                     Safe_Id("version"): Safe_Str__Text("1.0"      )}
        cls.test_tags            = [ Safe_Id("test"   ), Safe_Id       ("cache"    )]
        cls.test_data__base_64   = str_to_base64(json_to_str(cls.test_data))


    @classmethod
    def tearDownClass(cls):                                                          # ONE-TIME cleanup
        # Clean up all created handlers
        for handler in cls.service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)

        # Delete test bucket
        if cls.service.cache_handlers:
            handler = list(cls.service.cache_handlers.values())[0]
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete(cls.test_bucket)

    # def tearDown(self):                                                              # PER-TEST cleanup
    #     for namespace in self.service.list_namespaces():
    #         self.service.clear_namespace(namespace)

    def test__init__(self):                                                          # Test auto-initialization
        with Cache__Service() as _:
            assert type(_)                is Cache__Service
            assert base_classes(_)        == [Type_Safe, object]
            assert type(_.cache_handlers) is Type_Safe__Dict
            assert _.default_bucket       == DEFAULT__CACHE__SERVICE__BUCKET_NAME
            assert _.default_ttl_hours    == DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
            assert _.default_bucket       == "mgraph-ai-cache"                       # Default bucket
            assert _.default_ttl_hours    == 24                                      # Default TTL

    def test_setup(self):                                                            # Test service setup
        handlers_created_in_tests = [Safe_Id('handler-test'  ),
                                     Safe_Id('namespace1'    ),
                                     Safe_Id('namespace2'    ),
                                     Safe_Id('ns1'           ),
                                     Safe_Id('ns2'           ),
                                     Safe_Id('ns3'           ),
                                     Safe_Id('test-namespace')]
        with self.service as _:
            assert list_set(_.cache_handlers)     == handlers_created_in_tests       # Empty initially
            assert _.default_bucket               == self.test_bucket
            assert _.default_ttl_hours            == 12

    def test_get_or_create_handler(self):                                            # Test handler creation and retrieval
        namespace = Safe_Id("handler-test")

        with self.service as _:
            handler1 = _.get_or_create_handler(namespace)                           # First call creates
            assert type(handler1) is Cache__Handler
            assert handler1.s3__bucket      == self.test_bucket
            assert handler1.s3__prefix      == str(namespace)
            assert handler1.cache_ttl_hours == 12

            handler2 = _.get_or_create_handler(namespace)                           # Second call retrieves same instance
            assert handler2 is handler1

    def test_generate_cache_hash(self):                                                 # Test hash generation from data
        with self.service as _:
            hash1 = _.generate_cache_hash(self.test_data)                               # Test dict hashing
            assert type(hash1) is Safe_Str__Hash
            assert len(str(hash1)) == 10

            hash2 = _.generate_cache_hash(self.test_data)                               # Same data generates same hash
            assert hash1 == hash2
            assert hash1 == '8f07050afb'

            different_data = {"different": "data"}                                      # Different data generates different hash
            hash3 = _.generate_cache_hash(different_data)
            assert hash3 != hash1
            assert hash3 == '7e12defa19'

            hash_str = _.generate_cache_hash("simple string")                           # Test string hashing
            assert type(hash_str) is Safe_Str__Hash
            assert hash_str       == '8da13ece33'

    def test_store(self):                                                            # Test storing data in cache
        request = Schema__Cache__Store__Request( data         = self.test_data__base_64                         ,
                                                 content_type = Safe_Str__Http__Content_Type("application/json"),
                                                 metadata     = self.test_metadata                              ,
                                                 tags         = self.test_tags                                  )

        with self.service as _:
            response = _.store(request, self.test_namespace)

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert type(response.hash)     is Safe_Str__Hash
            assert type(response.path)     is Safe_Str__File__Path
            assert response.size           > 0

    def test_store__with_provided_hash(self):                                        # Test storing with custom hash
        custom_hash = Safe_Str__Hash("abc1234678")
        request     = Schema__Cache__Store__Request( data = self.test_data__base_64 ,
                                                    hash = custom_hash             )

        with self.service as _:
            handler   = _.get_or_create_handler(self.test_namespace)
            path_now  = handler.fs__temporal.handler__temporal.path_now()           # capture the current temporal path from the handler
            response  = _.store(request, self.test_namespace)
            cache_id  = response.cache_id
            assert type(response) is Schema__Cache__Store__Response
            assert response.obj() == __(hash     = custom_hash                  ,
                                        cache_id = cache_id                     ,
                                        path     = f'{path_now}/{cache_id}.json',
                                        size     =  284                         )

    def test_retrieve(self):                                                         # Test retrieving data from cache
        # Store first
        store_request = Schema__Cache__Store__Request(data = self.test_data__base_64)

        with self.service as _:
            store_response = _.store(store_request, self.test_namespace)

            # Retrieve by cache_id
            retrieve_request = Schema__Cache__Retrieve__Request( cache_id         = store_response.cache_id,
                                                                 include_data     = True                   ,
                                                                 include_metadata = True                   ,
                                                                 include_config   = False                  )

            result = _.retrieve(retrieve_request, self.test_namespace)
            stored_at = result.get('data').get('stored_at')
            assert type(result) is dict
            assert result == {'data'    : { 'content_type': ''                      ,
                                            'data'        : self.test_data__base_64 ,           # todo: add endpoint that just returns the data
                                            'hash'        : '1830c8ec23'            ,
                                            'metadata'    : {}                      ,           # todo: add endpoint that just returns the metadata (from the Memory_FS .metadata file). also add endpoint to return the .config() file
                                            'namespace'   : 'test-namespace'        ,
                                            'stored_at'   : stored_at               ,
                                            'tags'        : []                      ,
                                            'ttl_hours'   : 12                      },
                              'metadata': { }}

    def test_retrieve__by_hash(self):                                                   # Test retrieving by hash
        #store_request = Schema__Cache__Store__Request(data = self.test_data__base_64)   # todo since we use this one a lot, we should add this to the setUpClass of this test

        with self.service as _:
            #store_response = _.store(store_request, self.test_namespace)
            custom_hash = Safe_Str__Hash("abc1234678")
            # # Extract short hash
            # short_hash = _.generate_cache_id_short(store_response.hash)

            # Retrieve by hash
            retrieve_request = Schema__Cache__Retrieve__Request( hash            = custom_hash,
                                                                 include_data     = True      ,
                                                                 include_metadata = True      )
            assert retrieve_request.json()  == { 'cache_id'        : None        ,
                                                 'hash'            : 'abc1234678',
                                                 'include_config'  : True        ,
                                                 'include_data'    : True        ,
                                                 'include_metadata': True        }
            result = _.retrieve(retrieve_request, self.test_namespace)

            assert result is not None
            assert result == {'message': 'retrieval by hash not supported', 'status': 'error'}

    def test_retrieve__not_found(self):                                              # Test retrieving non-existent entry
        retrieve_request = Schema__Cache__Retrieve__Request(cache_id = Random_Guid())

        with self.service as _:
            result = _.retrieve(retrieve_request, self.test_namespace)
            assert result == {'message': 'cache entry not found', 'status': 'error'}

    def test_retrieve__selective_fields(self):                                       # Test selective field retrieval
        store_request = Schema__Cache__Store__Request(data = self.test_data__base_64)

        with self.service as _:
            store_response = _.store(store_request, self.test_namespace)

            # Only data
            request_data_only = Schema__Cache__Retrieve__Request( cache_id         = store_response.cache_id,
                                                                  include_data     = True                   ,
                                                                  include_metadata = False                  ,
                                                                  include_config   = False                  )

            result    = _.retrieve(request_data_only, self.test_namespace)
            stored_at = result.get('data').get('stored_at')

            assert result == {'data': { 'content_type': ''                      ,
                                        'data'        : self.test_data__base_64 ,
                                        'hash'        : '1830c8ec23',
                                        'metadata'    : {},
                                        'namespace'   : 'test-namespace',
                                        'stored_at'   : stored_at,
                                        'tags'        : [],
                                        'ttl_hours'   : 12}}
            assert 'data'         in result
            assert 'metadata' not in result
            assert 'config'   not in result

    def test_list_namespaces(self):                                                  # Test listing active namespaces
        namespaces = [Safe_Id("ns1"), Safe_Id("ns2"), Safe_Id("ns3")]

        with self.service as _:
            # Create handlers for multiple namespaces
            for ns in namespaces:
                _.get_or_create_handler(ns)

            active = _.list_namespaces()
            assert len(active) >= 3
            assert all(ns in active for ns in namespaces)

    def test__multiple_namespaces_isolated(self):                                    # Test namespace isolation
        ns1 = Safe_Id("namespace1")
        ns2 = Safe_Id("namespace2")

        data1 = str_to_base64(json_to_str({"namespace": 1}))
        data2 = str_to_base64(json_to_str({"namespace": 2}))

        with self.service as _:
            # Store in different namespaces
            request1 = Schema__Cache__Store__Request(data = data1)
            request2 = Schema__Cache__Store__Request(data = data2)

            response1 = _.store(request1, ns1)
            response2 = _.store(request2, ns2)

            # Retrieve from each namespace
            retrieve_request1 = Schema__Cache__Retrieve__Request(cache_id = response1.cache_id)
            retrieve_request2 = Schema__Cache__Retrieve__Request(cache_id = response2.cache_id)

            result1 = _.retrieve(retrieve_request1, ns1).get('data')
            result2 = _.retrieve(retrieve_request2, ns2).get('data')

            assert result1['data'] == data1
            assert result2['data'] == data2

            # Cross-namespace retrieve fails
            result_cross = _.retrieve(retrieve_request1, ns2)
            assert result_cross == {'message': 'cache entry not found', 'status': 'error'}

    def test__complex_data_storage(self):                                            # Test storing complex nested data
        complex_data = { "users"    : [ {"id": 1, "name": "Alice"}  ,
                                        {"id": 2, "name": "Bob"}    ],
                         "settings" : { "theme"   : "dark"          ,
                                       "lang"    : "en-US"          ,
                                       "nested"  : {"deep": {"value": 42}}},
                         "binary"   : "base64encodeddata=="         }
        complex_data_base_64 = str_to_base64(json_to_str(complex_data))

        request = Schema__Cache__Store__Request(data = complex_data_base_64)

        with self.service as _:
            response = _.store(request, self.test_namespace)

            retrieve_request = Schema__Cache__Retrieve__Request(cache_id = response.cache_id)
            result = _.retrieve(retrieve_request, self.test_namespace).get('data')

            assert result['data'] == complex_data_base_64