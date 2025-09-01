from unittest                                                                       import TestCase
from fastapi                                                                        import Request
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.testing.__ import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.utils.Objects import base_classes, obj
from osbot_utils.utils.Misc                                                         import random_string_short
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import Safe_Str__Hash
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                  import Safe_Id
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_fast_api.api.routes.Fast_API__Routes                                     import Fast_API__Routes
from mgraph_ai_service_cache.fast_api.routes.Routes__Cache                          import Routes__Cache
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                    import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response           import Schema__Cache__Store__Response
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs

class test_Routes__Cache(TestCase):

    @classmethod
    def setUpClass(cls):                                                            # ONE-TIME expensive setup
        setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-routes-"))

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        cls.cache_service  = Cache__Service(default_bucket=cls.test_bucket)
        cls.routes         = Routes__Cache(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace = Safe_Id("test-api")
        cls.test_string    = "test cache string"
        cls.test_json      = {"api": "test", "value": 123}
        cls.path_now       = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    def setUp(self):
        self.request = Request(scope={ "type": "http"})                             # simple Request object that we can use to add data to the state.body object

    @classmethod
    def tearDownClass(cls):                                                         # ONE-TIME cleanup
        for handler in cls.routes.cache_service.cache_handlers.values():
            with handler.s3__storage.s3 as s3:
                if s3.bucket_exists(cls.test_bucket):
                    s3.bucket_delete_all_files(cls.test_bucket)
                    s3.bucket_delete(cls.test_bucket)

    def test__init__(self):                                                         # Test initialization
        with Routes__Cache() as _:
            assert type(_)               is Routes__Cache
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == 'cache'
            assert type(_.cache_service) is Cache__Service

    def test_store_string(self):                                                    # Test string storage endpoint

        self.request.state.body = self.test_string.encode()


        response__store = self.routes.store__string__strategy__namespace(request   = self.request       ,
                                                        strategy  = "temporal"         ,
                                                        namespace = self.test_namespace)
        with response__store as _:
            cache_id         = _.cache_id
            cache_id__path_1 = cache_id[0:2]
            cache_id__path_2 = cache_id[2:4]
            cache_hash       = '36e2b79ffcdbc847'                                             # this should always be the same
            files_created    = [f'data/temporal/{self.path_now}/{cache_id}.json'                           ,
                                f'data/temporal/{self.path_now}/{cache_id}.json.config'                    ,
                                f'data/temporal/{self.path_now}/{cache_id}.json.metadata'                  ,
                                f'refs/by-hash/36/e2/{cache_hash}.json'                                    ,
                                f'refs/by-hash/36/e2/{cache_hash}.json.config'                             ,
                                f'refs/by-hash/36/e2/{cache_hash}.json.metadata'                           ,
                                f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json'        ,
                                f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.config' ,
                                f'refs/by-id/{cache_id__path_1}/{cache_id__path_2}/{cache_id}.json.metadata']
            assert _.obj() == __(hash     = cache_hash    ,
                                cache_id  = cache_id      ,
                                paths     = files_created ,
                                size      = 19            )
            assert type(_)          is Schema__Cache__Store__Response
            assert type(_.cache_id) is Random_Guid
            assert type(_.hash)     is Safe_Str__Cache_Hash

        response__retrieve = self.routes.retrieve__by_id__cache_id__namespace(cache_id=cache_id, namespace=self.test_namespace)

        stored_at          = response__retrieve.get('metadata').get(Safe_Id('stored_at'))
        assert type(stored_at)         is int
        assert response__retrieve == {'data': 'test cache string',
                                      'metadata': {Safe_Id('cache_hash'      ): '36e2b79ffcdbc847',
                                                   Safe_Id('cache_id'        ): cache_id,
                                                   Safe_Id('cache_key_data'  ): 'test cache string',
                                                   Safe_Id('content_encoding'): None,
                                                   Safe_Id('namespace'       ): 'test-api',
                                                   Safe_Id('stored_at'       ): stored_at,
                                                   Safe_Id('strategy'        ): 'temporal'}}





    def test_store_json(self):                                                      # Test JSON storage endpoint
        with self.routes as _:
            response = _.store_json(data      = self.test_json,
                                   strategy  = "temporal",
                                   namespace = self.test_namespace)

            assert type(response)          is Schema__Cache__Store__Response
            assert type(response.cache_id) is Random_Guid
            assert type(response.hash)     is Safe_Str__Hash

    def test_store_json__with_exclusions(self):                                    # Test JSON with field exclusion
        data_with_timestamp = {"data": "value", "timestamp": "2024-01-01"}

        with self.routes as _:
            response1 = _.store_json(data      = data_with_timestamp,
                                   namespace = self.test_namespace)

            response2 = _.store_json(data           = data_with_timestamp,
                                   exclude_fields = ["timestamp"],
                                   namespace      = self.test_namespace)

            # Different hashes due to exclusion
            assert response1.hash != response2.hash

    def test_retrieve_by_hash(self):                                                # Test retrieval by hash
        with self.routes as _:
            # Store first
            store_response = _.store_string(self.test_string, namespace=self.test_namespace)

            # Retrieve
            result = _.retrieve_by_hash(store_response.hash, self.test_namespace)

            assert result is not None
            assert "data" in result
            assert result["data"] == self.test_string

    def test_retrieve_by_hash__not_found(self):                                     # Test retrieval of non-existent
        with self.routes as _:
            result = _.retrieve_by_hash(Safe_Str__Hash("0000000000000000"), self.test_namespace)

            assert result == {"status": "not_found", "message": "Cache entry not found"}

    def test_retrieve_by_id(self):                                                  # Test retrieval by cache ID
        with self.routes as _:
            # Store first
            store_response = _.store_string(self.test_string, namespace=self.test_namespace)

            # Retrieve
            result = _.retrieve_by_id(store_response.cache_id, self.test_namespace)

            assert result is not None
            assert "data" in result
            assert result["data"] == self.test_string

    def test_hash_calculate(self):                                                  # Test hash calculation endpoint
        with self.routes as _:
            # From string
            result = _.hash_calculate(data="test string")
            assert "hash" in result
            assert len(result["hash"]) == 16

            # From JSON
            result = _.hash_calculate(json_data={"key": "value"})
            assert "hash" in result

            # With exclusions
            result = _.hash_calculate(json_data      = {"key": "value", "id": "123"},
                                    exclude_fields = ["id"])
            assert "hash" in result

            # No data provided
            result = _.hash_calculate()
            assert result == {"error": "No data provided"}

    def test_exists(self):                                                          # Test existence check
        with self.routes as _:
            # Store data
            store_response = _.store_string(self.test_string, namespace=self.test_namespace)

            # Check exists
            result = _.exists(store_response.hash, self.test_namespace)
            assert result == {"exists": True, "hash": str(store_response.hash)}

            # Check non-existent
            result = _.exists(Safe_Str__Hash("0000000000000000"), self.test_namespace)
            assert result == {"exists": False, "hash": "0000000000000000"}

    def test_namespaces(self):                                                      # Test namespace listing
        with self.routes as _:
            # Create some namespaces
            _.store_string("data1", namespace=Safe_Id("ns1"))
            _.store_string("data2", namespace=Safe_Id("ns2"))

            result = _.namespaces()

            assert "namespaces" in result
            assert "count" in result
            assert result["count"] >= 2

    def test_stats(self):                                                           # Test statistics endpoint
        with self.routes as _:
            # Store some data
            for i in range(3):
                _.store_string(f"data_{i}", namespace=self.test_namespace)

            result = _.stats(self.test_namespace)

            assert result["namespace"] == str(self.test_namespace)
            assert result["s3_bucket"] == self.test_bucket
            assert result["ttl_hours"] == 24
            assert "direct_files" in result
            assert "temporal_files" in result