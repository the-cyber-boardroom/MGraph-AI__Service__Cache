from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.utils.Misc                                                         import is_guid
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                    import Safe_Str__Cache_Hash
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Cache__client(TestCase):                                         # Test cache routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        with setup__service_fast_api_test_objs() as _:
            cls.client = _.fast_api__client                                         # Reuse TestClient
            cls.app    = _.fast_api__app                                            # Reuse FastAPI app
            cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_namespace    = "test-http"
        cls.test_string       = "test string data"
        cls.test_json         = {"test": "data", "number": 123}
        cls.path_now          = Path__Handler__Temporal().path_now()                # get the current temporal path from the handler

    def test__cache__store__string(self):                                           # Test string storage via HTTP
        # Store string directly in body
        response = self.client.post(f'/cache/store/string/temporal/{self.test_namespace}',
                                    content = self.test_string,
                                    headers = {"Content-Type": "text/plain"})

        assert response.status_code == 200
        result     = response.json()
        cache_id   = result.get('cache_id')
        cache_hash = result.get('hash')

        assert is_guid(cache_id) is True
        assert type(cache_hash) is str
        assert len(cache_hash) == 16                                                # Default hash length

        # Verify paths structure
        assert 'paths' in result
        assert 'data' in result['paths']
        assert 'by_hash' in result['paths']
        assert 'by_id' in result['paths']

        return cache_id, cache_hash

    def test__cache__store__json(self):                                             # Test JSON storage via HTTP
        # Store JSON directly in body
        response = self.client.post(f'/cache/store/json/temporal/{self.test_namespace}',
                                    json=self.test_json )

        assert response.status_code == 200
        result = response.json()
        cache_id = result.get('cache_id')
        cache_hash = result.get('hash')

        assert is_guid(cache_id) is True
        assert type(cache_hash) is str
        assert len(cache_hash) == 16

        return cache_id, cache_hash

    def test__cache__retrieve__by_id(self):                                                     # Test retrieve by ID via HTTP
        cache_id, cache_hash = self.test__cache__store__string()                                # Store first

        response = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{self.test_namespace}')   # Retrieve by ID

        assert response.status_code == 200
        result = response.json()

        assert result == { 'data'    : 'test string data'                                    , # Verify data
                           'metadata': { 'cache_hash'       : cache_hash                     , # Verify metadata
                                         'cache_id'         : cache_id                       ,
                                         'cache_key_data'   : self.test_string               ,
                                         'content_encoding' : None                           ,
                                         'namespace'        : 'test-http'                    ,
                                         'stored_at'        : result['metadata']['stored_at'],
                                         'strategy'         : 'temporal'                     }}

        assert 'metadata' in result
        assert result['data'] == self.test_string

    def test__cache__retrieve__by_hash(self):                                       # Test retrieve by hash via HTTP
        # Store first
        cache_id, cache_hash = self.test__cache__store__string()

        # Retrieve by hash
        response = self.client.get(f'/cache/retrieve/by-hash/{cache_hash}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()

        # Hash retrieval should work
        assert 'data' in result
        assert result['data'] == self.test_string

        assert result == { 'data'    : 'test string data'                                    , # Verify data
                           'metadata': { 'cache_hash'       : cache_hash                     , # Verify metadata
                                         'cache_id'         : cache_id                       ,
                                         'cache_key_data'   : self.test_string               ,
                                         'content_encoding' : None                           ,
                                         'namespace'        : 'test-http'                    ,
                                         'stored_at'        : result['metadata']['stored_at'],
                                         'strategy'         : 'temporal'                     }}

    def test__cache__retrieve__not_found(self):                                     # Test retrieve non-existent entry
        non_existent_id = Random_Guid()
        response = self.client.get(f'/cache/retrieve/by-id/{non_existent_id}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"status": "not_found", "message": 'Cache entry not found'}

    def test__cache__delete__by_id(self):                                           # Test delete by ID via HTTP
        # Store first
        target_string   = "another string"
        response__store = self.client.post(f'/cache/store/string/temporal/{self.test_namespace}',
                                          content = target_string,
                                          headers = {"Content-Type": "text/plain"})
        cache_id   = response__store.json().get('cache_id')


        # Delete by ID
        response = self.client.delete(f'/cache/delete/by-id/{cache_id}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()

        assert result['status'] == 'success'
        assert result['cache_id'] == cache_id
        assert result['deleted_count'] == 9                                         # 3 data + 3 by_hash + 3 by_id files
        assert result['failed_count'] == 0
        assert len(result['deleted_paths']) == 9

        # Verify deletion
        retrieve_response = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{self.test_namespace}')
        retrieve_result = retrieve_response.json()
        assert retrieve_result['status'] == 'not_found'

    # def test__cache__hash__calculate(self):                                         # Test hash calculation endpoint
    #     # Calculate hash from string
    #     response = self.client.get('/cache/hash/calculate', params={"data": self.test_string})
    #
    #     assert response.status_code == 200
    #     result = response.json()
    #     assert 'hash' in result
    #     assert len(result['hash']) == 16
    #
    #     # Calculate hash from JSON
    #     response = self.client.post('/cache/hash/calculate', json={"json_data": self.test_json})
    #
    #     assert response.status_code == 200
    #     result = response.json()
    #     assert 'hash' in result
    #
    #     # With field exclusion
    #     response = self.client.post(
    #         '/cache/hash/calculate',
    #         json={"json_data": self.test_json, "exclude_fields": ["number"]}
    #     )
    #
    #     assert response.status_code == 200
    #     result = response.json()
    #     assert 'hash' in result

    def test__cache__exists(self):                                                  # Test existence check via HTTP
        # Store first
        cache_id, cache_hash = self.test__cache__store__string()

        # Check exists
        response = self.client.get(f'/cache/exists/{cache_hash}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists": True, "hash": cache_hash}

        # Check non-existent
        non_existent_hash = Safe_Str__Cache_Hash("0000000000000000")
        response = self.client.get(f'/cache/exists/{non_existent_hash}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists": False, "hash": str(non_existent_hash)}

    def test__cache__namespaces(self):                                              # Test list namespaces endpoint
        # Create entries in multiple namespaces
        namespaces = ["ns1", "ns2", "ns3"]
        for ns in namespaces:
            response = self.client.post(
                f'/cache/store/string/temporal/{ns}',
                content=f"data for {ns}",
                headers={"Content-Type": "text/plain"}
            )
            assert response.status_code == 200

        # List namespaces
        response = self.client.get('/cache/namespaces')

        assert response.status_code == 200
        result = response.json()
        assert 'namespaces' in result
        assert 'count' in result
        assert all(ns in result['namespaces'] for ns in namespaces)

    def test__cache__stats(self):                                                   # Test stats endpoint
        # Store some data first
        for i in range(3):
            response = self.client.post(f'/cache/store/string/temporal/{self.test_namespace}',
                                        content=f"test data {i}",
                                        headers={"Content-Type": "text/plain"})
            assert response.status_code == 200

        # Get stats
        response = self.client.get(f'/cache/stats/namespaces/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()

        assert result['namespace'] == self.test_namespace
        assert 's3_bucket' in result
        assert 'ttl_hours' in result
        assert 'direct_files' in result
        assert 'temporal_files' in result

    def test__workflow_complete(self):                                              # Test complete workflow via HTTP
        namespace = "workflow-test"
        test_data = {"workflow": "test", "step": 1}

        # 1. Store JSON
        store_response = self.client.post(f'/cache/store/json/temporal/{namespace}',
                                          json=test_data                          )
        assert store_response.status_code == 200
        store_result = store_response.json()
        cache_id = store_result['cache_id']
        cache_hash = store_result['hash']

        # 2. Check exists
        exists_response = self.client.get(f'/cache/exists/{cache_hash}/{namespace}')
        assert exists_response.status_code == 200
        exists_result = exists_response.json()
        assert exists_result['exists'] is True

        # 3. Retrieve by ID
        retrieve_response = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{namespace}')
        assert retrieve_response.status_code == 200
        retrieve_result = retrieve_response.json()
        assert retrieve_result['data'] == test_data

        # 4. Check stats
        stats_response = self.client.get(f'/cache/stats/namespaces/{namespace}')
        assert stats_response.status_code == 200
        stats_result = stats_response.json()
        assert stats_result['namespace'] == namespace

        # 5. List namespaces
        namespaces_response = self.client.get('/cache/namespaces')
        assert namespaces_response.status_code == 200
        namespaces_result = namespaces_response.json()
        assert namespace in namespaces_result['namespaces']

        # 6. Delete
        delete_response = self.client.delete(f'/cache/delete/by-id/{cache_id}/{namespace}')
        assert delete_response.status_code == 200
        delete_result = delete_response.json()
        assert delete_result['status'       ] == 'success'
        assert delete_result['deleted_count'] == 6                      # todo: figure out why this is 6 instead of 9, the refs/by-cache/ are not being deleted
        # from osbot_utils.utils.Dev import pprint
        # pprint(delete_result)

        # 7. Verify deleted
        final_response = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{namespace}')
        assert final_response.status_code == 200
        final_result = final_response.json()
        assert final_result['status'] == 'not_found'

    def test__cache__store__multiple_strategies(self):                              # Test different storage strategies
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            response = self.client.post(
                f'/cache/store/string/{strategy}/{self.test_namespace}',
                content=f"test data for {strategy}",
                headers={"Content-Type": "text/plain"}
            )

            assert response.status_code == 200
            result = response.json()
            assert 'cache_id' in result
            assert 'hash' in result
            assert 'paths' in result

            # Verify appropriate paths are created for each strategy
            paths = result['paths']
            assert 'data' in paths
            assert 'by_hash' in paths
            assert 'by_id' in paths