import pytest
from unittest                                           import TestCase
from osbot_aws.testing.Temp__Random__AWS_Credentials    import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.AWS_Config                               import aws_config
from osbot_utils.utils.Json                             import json_to_str
from osbot_utils.utils.Misc                             import str_to_base64
from tests.unit.Service__Fast_API__Test_Objs            import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Cache__client(TestCase):                                          # Test cache routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        pytest.skip("tests need fixing")
        with setup__service_fast_api_test_objs() as _:
            cls.client = _.fast_api__client                                          # Reuse TestClient
            cls.app    = _.fast_api__app                                             # Reuse FastAPI app
            cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        assert aws_config.account_id () == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_namespace    = "test-http"
        cls.test_data         = {"test": "data", "number": 123}
        cls.test_data_base_64 = str_to_base64(json_to_str(cls.test_data))

    # def tearDown(self):                                                              # PER-TEST cleanup
    #     # Clear test namespace after each test
    #     self.client.post(f'/cache/clear?namespace={self.test_namespace}')
    # todo: the /cache/clear doesn't exist, so we will need to manually delete the extra files created in the tests

    def test__cache__store(self):                                                    # Test store endpoint via HTTP
        payload = { "data"         : self.test_data_base_64        ,
                    "content_type" : "application/json"    ,
                    "tags"         : ["test", "http"]      }

        response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                     json = payload                                 )

        assert response.status_code == 200
        result = response.json()
        assert 'cache_id' in result
        assert 'hash' in result
        assert result['version'] == 1
        assert result['size'] > 0

    def test__cache__store__validation_error(self):                                  # Test store with invalid data
        payload = {}                                                                 # Missing required 'data' field

        response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                     json = payload                                 )

        assert response.status_code == 422                                           # Unprocessable Entity
        error = response.json()
        assert 'detail' in error
        assert error['detail'][0]['msg'] == 'Field required'
        assert error['detail'][0]['loc'] == ['body', 'data']

    def test__cache__retrieve(self):                                                 # Test retrieve endpoint via HTTP
        # Store first
        store_payload = {"data": self.test_data_base_64}
        store_response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                           json = store_payload                           )
        cache_id = store_response.json()['cache_id']

        # Retrieve
        response = self.client.get( f'/cache/retrieve?cache_id={cache_id}&namespace={self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert 'data' in result
        assert result['data'] == self.test_data_base_64
        assert 'metadata' in result

    def test__cache__retrieve__not_found(self):                                      # Test retrieve non-existent entry
        response = self.client.get( f'/cache/retrieve?cache_id=non-existent&namespace={self.test_namespace}')

        assert response.status_code == 200                                           # Still 200 but with not_found status
        result = response.json()
        assert result == { "status"  : "not_found"              ,
                          "message" : "Cache entry not found"   }

    def test__cache__retrieve_by_hash(self):                                         # Test retrieve by hash endpoint
        # Store first
        store_payload = {"data": self.test_data_base_64}
        store_response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                           json = store_payload                           )
        store_result = store_response.json()

        # Extract short hash (first 7 chars)
        hash_short = store_result['hash'][:7]

        # Retrieve by hash
        response = self.client.get( f'/cache/retrieve_by_hash?hash={hash_short}&namespace={self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result['data'] == self.test_data_base_64

    def test__cache__clear(self):                                                    # Test clear namespace endpoint
        # Store some data
        store_payload = {"data": self.test_data_base_64}
        self.client.post( f'/cache/store?namespace={self.test_namespace}',
                         json = store_payload                             )

        # Clear
        response = self.client.post(f'/cache/clear?namespace={self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result == { "success"   : True                ,
                          "namespace" : self.test_namespace  }

    def test__cache__namespaces(self):                                               # Test list namespaces endpoint
        # Create entries in multiple namespaces
        namespaces = ["ns1", "ns2", "ns3"]
        for ns in namespaces:
            payload = {"data": {"namespace": ns}}
            self.client.post( f'/cache/store?namespace={ns}',
                             json = payload                  )

        # List namespaces
        response = self.client.get('/cache/namespaces')

        assert response.status_code == 200
        result = response.json()
        assert 'namespaces' in result
        assert 'count' in result
        assert all(ns in result['namespaces'] for ns in namespaces)

        # Cleanup
        for ns in namespaces:
            self.client.post(f'/cache/clear?namespace={ns}')

    def test__cache__stats(self):                                                    # Test stats endpoint
        # Store multiple entries
        for i in range(3):
            payload = {"data": {"item": i}}
            self.client.post( f'/cache/store?namespace={self.test_namespace}',
                             json = payload                                  )

        # Get stats
        response = self.client.get(f'/cache/stats?namespace={self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result['namespace'] == self.test_namespace
        assert result['total_entries'] >= 3
        assert 's3_bucket' in result
        assert 's3_prefix' in result
        assert result['ttl_hours'] == 24

    def test__cache__stats__default_namespace(self):                                 # Test stats without namespace
        response = self.client.get('/cache/stats')

        assert response.status_code == 200
        result = response.json()
        assert result['namespace'] == "default"
        assert 'total_entries' in result

    def test__workflow_complete(self):                                               # Test complete workflow via HTTP
        namespace = "workflow-test"

        # 1. Store
        store_payload = { "data"         : self.test_data_base_64       ,
                         "tags"         : ["workflow", "test"] ,
                         "metadata"     : {"author": "tester"}  }

        store_response = self.client.post( f'/cache/store?namespace={namespace}',
                                           json = store_payload                  )
        assert store_response.status_code == 200
        store_result = store_response.json()
        cache_id = store_result['cache_id']
        hash_short = store_result['hash'][:7]

        # 2. Retrieve by ID
        retrieve_response = self.client.get(f'/cache/retrieve?cache_id={cache_id}&namespace={namespace}')
        assert retrieve_response.status_code == 200
        retrieve_result = retrieve_response.json()
        assert retrieve_result['data'] == self.test_data_base_64

        # 3. Retrieve by hash
        hash_response = self.client.get(f'/cache/retrieve_by_hash?hash={hash_short}&namespace={namespace}')
        assert hash_response.status_code == 200
        hash_result = hash_response.json()
        assert hash_result['data'] == self.test_data_base_64

        # 4. Check stats
        stats_response = self.client.get(f'/cache/stats?namespace={namespace}')
        assert stats_response.status_code == 200
        stats_result = stats_response.json()
        assert stats_result['total_entries'] >= 1

        # 5. List namespaces
        namespaces_response = self.client.get('/cache/namespaces')
        assert namespaces_response.status_code == 200
        namespaces_result = namespaces_response.json()
        assert namespace in namespaces_result['namespaces']

        # 6. Clear
        clear_response = self.client.post(f'/cache/clear?namespace={namespace}')
        assert clear_response.status_code == 200
        clear_result = clear_response.json()
        assert clear_result['success'] is True

        # 7. Verify cleared
        final_response = self.client.get(f'/cache/retrieve?cache_id={cache_id}&namespace={namespace}')
        assert final_response.status_code == 200
        final_result = final_response.json()
        assert final_result['status'] == "not_found"

    def test__concurrent_namespaces(self):                                           # Test multiple namespaces concurrently
        namespaces = [f"concurrent-{i}" for i in range(5)]
        cache_ids = {}

        # Store in each namespace
        for ns in namespaces:
            payload = {"data": {"namespace": ns, "value": ns}}
            response = self.client.post( f'/cache/store?namespace={ns}',
                                         json = payload                 )
            assert response.status_code == 200
            cache_ids[ns] = response.json()['cache_id']

        # Verify isolation - retrieve from each
        for ns in namespaces:
            response = self.client.get(f'/cache/retrieve?cache_id={cache_ids[ns]}&namespace={ns}')
            assert response.status_code == 200
            result = response.json()
            assert result['data']['namespace'] == ns
            assert result['data']['value'] == ns

        # Clear one namespace shouldn't affect others
        self.client.post(f'/cache/clear?namespace={namespaces[0]}')

        # First namespace cleared
        response = self.client.get(f'/cache/retrieve?cache_id={cache_ids[namespaces[0]]}&namespace={namespaces[0]}')
        assert response.json()['status'] == "not_found"

        # Others still exist
        for ns in namespaces[1:]:
            response = self.client.get(f'/cache/retrieve?cache_id={cache_ids[ns]}&namespace={ns}')
            assert response.status_code == 200
            assert response.json()['data']['namespace'] == ns

        # Cleanup
        for ns in namespaces[1:]:
            self.client.post(f'/cache/clear?namespace={ns}')