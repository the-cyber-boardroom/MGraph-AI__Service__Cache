from unittest                                                                       import TestCase
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_fast_api.api.routes.Fast_API__Routes                                     import Fast_API__Routes
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import Safe_Str__Hash
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import random_string_short, str_to_base64
from osbot_utils.utils.Objects                                                      import base_classes
from osbot_aws.AWS_Config                                                           import aws_config
from mgraph_ai_service_cache.fast_api.routes.Routes__Cache                          import Routes__Cache
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request            import Schema__Cache__Store__Request
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs


class test_Routes__Cache(TestCase):                                                  # Test FastAPI cache routes

    @classmethod
    def setUpClass(cls):                                                                # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()                                             # setup Local_Stack
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-routes-"))

        assert aws_config.account_id () == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service = Cache__Service(default_bucket = cls.test_bucket)
        cls.routes        = Routes__Cache (cache_service=cls.cache_service )
        # with Routes__Cache() as _:
        #     #cls.routes = _
        #     _.cache_service = Cache__Service( default_bucket = cls.test_bucket ).setup()
        #     _.setup()

        # Test data
        cls.test_namespace     = Safe_Id("test-api")
        cls.test_data          = {"api": "test", "value": 123}
        cls.test_tags          = [Safe_Id("api"), Safe_Id("test")]
        cls.test_data__base_64 = str_to_base64(json_to_str(cls.test_data))

    @classmethod
    def tearDownClass(cls):                                                          # ONE-TIME cleanup
        # Clean up buckets
        for handler in cls.routes.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    # def tearDown(self):                                                              # PER-TEST cleanup
    #     for namespace in self.routes.cache_service.list_namespaces():
    #         self.routes.cache_service.clear_namespace(namespace)

    def test__init__(self):                                                          # Test auto-initialization
        with Routes__Cache() as _:
            assert type(_)              is Routes__Cache
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == 'cache'
            assert type(_.cache_service) is Cache__Service


    def test_store(self):                                                            # Test store endpoint
        request = Schema__Cache__Store__Request( data         = self.test_data__base_64 ,
                                                 tags         = self.test_tags          )

        with self.routes as _:
            response = _.store(request, self.test_namespace)

            assert type(response.cache_id) is Random_Guid
            assert type(response.hash)     is Safe_Str__Hash
            assert response.size            > 0

    def test_store__default_namespace(self):                                         # Test store with default namespace
        request = Schema__Cache__Store__Request(data = self.test_data__base_64)

        with self.routes as _:
            response = _.store(request)                                              # No namespace provided

            assert type(response.cache_id) is Random_Guid

            # Verify stored in default namespace
            assert Safe_Id("default") in _.cache_service.list_namespaces()

    def test_retrieve(self):                                                         # Test retrieve endpoint
        # Store first
        store_request = Schema__Cache__Store__Request(data = self.test_data__base_64)

        with self.routes as _:
            store_response = _.store(store_request, self.test_namespace)

            # Retrieve by cache_id
            result = _.retrieve( cache_id  = store_response.cache_id ,
                                namespace = self.test_namespace     ).get('data')

            assert 'data' in result
            assert result['data'] == self.test_data__base_64
            assert 'metadata' in result

    def test_retrieve__not_found(self):                                              # Test retrieve non-existent entry
        with self.routes as _:
            result = _.retrieve( cache_id = Random_Guid()        ,
                                namespace = self.test_namespace  )

            assert result == { "status"  : "error"                  ,
                               "message" : "cache entry not found"  }

    def test_retrieve_by_hash(self):                                                 # Test retrieve by hash endpoint
        store_request = Schema__Cache__Store__Request(data = self.test_data__base_64)

        with self.routes as _:
            store_response = _.store(store_request, self.test_namespace)

            # Extract short hash
            cache_hash = _.cache_service.generate_cache_hash('some string')
            assert type(cache_hash) is Safe_Str__Hash

            # Retrieve by hash
            result = _.retrieve_by_hash(hash      = cache_hash         ,
                                        namespace = self.test_namespace)

            assert result == {'message': 'retrieval by hash not supported', 'status': 'error'}

    def test_namespaces(self):                                                       # Test list namespaces endpoint
        namespaces = [Safe_Id("ns-api-1"), Safe_Id("ns-api-2"), Safe_Id("ns-api-3")]

        with self.routes as _:
            # Create entries in multiple namespaces
            for ns in namespaces:
                data_base64 = str_to_base64(json_to_str({"ns": str(ns)}))
                request = Schema__Cache__Store__Request(data = data_base64)
                _.store(request, ns)

            # List namespaces
            result = _.namespaces()

            assert 'namespaces' in result
            assert 'count' in result
            assert result['count'] >= 3
            assert all(str(ns) in result['namespaces'] for ns in namespaces)

    def test_stats(self):                                                            # Test stats endpoint
        namespace = Safe_Id("stats-test")

        with self.routes as _:
            # Store multiple entries
            for i in range(5):
                data_base64 = str_to_base64(json_to_str({"item": i}))
                request = Schema__Cache__Store__Request(data = data_base64)
                _.store(request, namespace)

            # Get stats
            result = _.stats(namespace)

            assert result['namespace']     == str(namespace)
            assert result['total_entries']  >= 5                                     # At least 5 files
            assert result['s3_bucket']     == self.test_bucket
            assert result['s3_prefix']     == str(namespace)
            assert result['ttl_hours']     == 24                                     # Default TTL

    def test_stats__default_namespace(self):                                         # Test stats for default namespace
        with self.routes as _:
            result = _.stats()                                                       # No namespace provided

            assert result['namespace'] == "default"
            assert 'total_entries'     in result
            assert 's3_bucket'         in result

    def test_stats__error_handling(self):                                            # Test stats error handling
        with self.routes as _:
            # Mock an error condition
            original_files_paths = _.cache_service.get_or_create_handler(Safe_Id("error-test")).s3__storage.files__paths

            def mock_error():
                raise Exception("Test error")

            _.cache_service.get_or_create_handler(Safe_Id("error-test")).s3__storage.files__paths = mock_error

            result = _.stats(Safe_Id("error-test"))

            assert 'error' in result
            assert result['error'] == "Test error"
            assert result['namespace'] == "error-test"

            # Restore original
            _.cache_service.get_or_create_handler(Safe_Id("error-test")).s3__storage.files__paths = original_files_paths

    def test__workflow_store_retrieve_clear(self):                                   # Test complete workflow
        namespace = Safe_Id("workflow-test")

        with self.routes as _:
            # Store
            store_request = Schema__Cache__Store__Request( data = self.test_data__base_64 ,
                                                           tags = self.test_tags         )
            store_response = _.store(store_request, namespace)

            # Retrieve by ID
            retrieve_result = _.retrieve( cache_id  = store_response.cache_id ,
                                         namespace = namespace                ).get('data')
            assert retrieve_result['data'] == self.test_data__base_64

            # # Retrieve by hash
            # short_hash = _.cache_service.generate_cache_id_short(store_response.hash)
            # hash_result = _.retrieve_by_hash( hash      = short_hash ,
            #                                  namespace = namespace   )
            # assert hash_result['data'] == self.test_data

            # Stats
            stats_result = _.stats(namespace)
            assert stats_result['total_entries'] >= 1

            # # Clear
            # clear_result = _.clear(namespace)
            # assert clear_result['success'] is True
            #
            # # Verify cleared
            # final_result = _.retrieve( cache_id  = store_response.cache_id ,
            #                           namespace = namespace                )
            # assert final_result['status'] == "not_found"