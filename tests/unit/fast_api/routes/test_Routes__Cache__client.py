import pytest
from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_utils.type_safe.primitives.safe_str.cryptography.hashes.Safe_Str__Hash   import safe_str_hash

from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.utils.Json                                                         import json_to_str
from osbot_utils.utils.Misc                                                         import str_to_base64, is_guid, list_set
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Request            import Schema__Cache__Store__Request
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Cache__client(TestCase):                                          # Test cache routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
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
        cls.path_now          = Path__Handler__Temporal().path_now()                      # get the current temporal path from the handler

    # todo: note: I removed the namesapce clean since I think they are quite dangerous, I think this is better done by providing a way to delete multiple files
    # def tearDown(self):                                                               # PER-TEST cleanup
    #     # Clear test namespace after each test
    #     self.client.post(f'/cache/clear?namespace={self.test_namespace}')
    # todo: the /cache/clear doesn't exist, so we will need to manually delete the extra files created in the tests

    def test__cache__store(self):                                                    # Test store endpoint via HTTP
        payload = { "data"         : self.test_data_base_64 ,
                    "content_type" : "application/json"     ,
                    "tags"         : ["test", "http"]       ,
                    "metadata"     : {}                     ,
                    "hash"         : ''                     }

        response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                     json = payload                                 )

        result = response.json()
        cache_id = result.get('cache_id')
        assert response.status_code == 200
        assert is_guid(cache_id)    is True
        assert response.json()      == { 'cache_id': cache_id       ,
                                         'hash'    : 'a7681e31c5'   ,
                                         'path'    : f'{self.path_now}/{cache_id}.json',        # todo: review this logic, since the file should be stored by hash, not by cache_id, which always be unique (although this will create an issue that our granularity to store unique cached versions will be the precision of the temporal path)
                                         'size'    : 295            }


    def test__cache__store__validation_lack_of_error(self):                                 # Test store with empty data
        payload = {}                                                                        # todo: review this lack of validation since with the current functionality (where the Schema__Cache__Store__Request is able to create valid values for its elements), this is a valid request

        response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                     json = payload                                 )

        assert response.status_code == 200
        result = response.json()
        cache_id = result.get('cache_id')
        assert result == { 'cache_id': cache_id ,
                           'hash'    : 'e3b0c44298',
                           'path'    : f'{self.path_now}/{cache_id}.json',
                           'size'    : 187}
        # error = response.json()
        # assert 'detail' in error
        # assert error['detail'][0]['msg'] == 'Field required'
        # assert error['detail'][0]['loc'] == ['body', 'content_type']

    def test__cache__retrieve(self):                                                 # Test retrieve endpoint via HTTP
        # Store first
        store_payload = {"data": self.test_data_base_64}
        store_response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                           json = store_payload                           )
        assert store_response.status_code == 200

        cache_id = store_response.json()['cache_id']

        # Retrieve
        response = self.client.get( f'/cache/retrieve?cache_id={cache_id}&namespace={self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        stored_at = result.get('data').get('stored_at')
        assert result == {'data': {'content_type': ''                    ,
                                   'data'        : self.test_data_base_64,
                                   'hash'        : 'a7681e31c5'          ,
                                   'metadata'    : {}                    ,
                                   'namespace'   : 'test-http'           ,
                                   'stored_at'   : stored_at             ,
                                   'tags'        : []                    ,
                                   'ttl_hours'   : 24                    },
                         'metadata': {}}
        # assert 'data' in result
        # assert result['data'] == self.test_data_base_64
        # assert 'metadata' in result

    def test__cache__retrieve__not_found(self):                                      # Test retrieve non-existent entry
        cache_id = Random_Guid()
        response = self.client.get( f'/cache/retrieve?cache_id={cache_id}&namespace={self.test_namespace}')

        assert response.status_code == 200                                           # Still 200 but with not_found status
        result = response.json()
        assert result == { "status" : "error"                   ,
                          "message" : "cache entry not found"   }

    def test__cache__retrieve_by_hash(self):                                         # Test retrieve by hash endpoint
        # Store first
        store_payload = {"data": self.test_data_base_64}
        store_response = self.client.post( f'/cache/store?namespace={self.test_namespace}',
                                           json = store_payload                           )
        store_result = store_response.json()

        # Extract short hash (first 7 chars)
        hash_short = safe_str_hash('an value')

        # Retrieve by hash
        response = self.client.get( f'/cache/retrieve-by-hash?hash={hash_short}&namespace={self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result == {'message': 'retrieval by hash not supported',
                          'status' : 'error'                          }
        #assert result['data'] == self.test_data_base_64

    # def test__cache__clear(self):                                                    # Test clear namespace endpoint
    #     # Store some data
    #     store_payload = {"data": self.test_data_base_64}
    #     self.client.post( f'/cache/store?namespace={self.test_namespace}',
    #                      json = store_payload                             )
    #
    #     # Clear
    #     response = self.client.post(f'/cache/clear?namespace={self.test_namespace}')
    #
    #     assert response.status_code == 200
    #     result = response.json()
    #     assert result == { "success"   : True                ,
    #                       "namespace" : self.test_namespace  }

    def test__cache__namespaces(self):                                               # Test list namespaces endpoint
        # Create entries in multiple namespaces
        namespaces = ["ns1", "ns2", "ns3"]
        for ns in namespaces:
            store_request = Schema__Cache__Store__Request(data=str_to_base64(json_to_str({"data": {"namespace": ns}})))
            payload = store_request.json()
            response = self.client.post( f'/cache/store?namespace={ns}',
                                         json = payload                )
            assert response.status_code == 200

        # List namespaces
        response = self.client.get('/cache/namespaces')

        assert response.status_code == 200
        result = response.json()
        assert 'namespaces' in result
        assert 'count' in result
        assert all(ns in result['namespaces'] for ns in namespaces)

        # todo: add clean up step here
        # Cleanup
        # for ns in namespaces:
        #     self.client.post(f'/cache/clear?namespace={ns}')

    def test__cache__stats(self):                                                    # Test stats endpoint
        def get_stats():
            response = self.client.get(f'/cache/stats?namespace={self.test_namespace}')
            assert response.status_code == 200
            result = response.json()                                                 # todo: we should be able to use a Type_Safe class here
            assert list_set(result) == ['namespace', 's3_bucket', 's3_prefix', 'total_entries', 'ttl_hours']
            return result

        # Store multiple entries
        stats_before = get_stats()
        files_to_create = 3
        expected_new_files = files_to_create * 6            # todo: map these 6 new files (which should be 3 in latest and 3 in the temporal location
        for i in range(files_to_create):
            store_request = Schema__Cache__Store__Request(data=str_to_base64(json_to_str({"data": {"data": {"item": i}}})))
            payload       = store_request.json()
            response      = self.client.post( f'/cache/store?namespace={self.test_namespace}', json = payload                                  )
            assert response.status_code == 200

        stats_after  = get_stats()

        total_entries_before = stats_before['total_entries']
        assert stats_after == { 'namespace'    : 'test-http'                              ,
                                's3_bucket'    : 'mgraph-ai-cache'                        ,
                                's3_prefix'    : 'test-http'                              ,
                                'total_entries': total_entries_before + expected_new_files,         # todo, ca
                                'ttl_hours'    : 24                                       }

    def test__cache__stats__default_namespace(self):                                 # Test stats without namespace
        response = self.client.get('/cache/stats')

        assert response.status_code == 200
        result = response.json()
        assert result['namespace'].startswith('safe-id_')                           # todo: review this default assigment
        assert 'total_entries' in result

    def test__workflow_complete(self):                                               # Test complete workflow via HTTP
        namespace = "workflow-test"

        # 1. Store
        store_payload = { "data"         : self.test_data_base_64       ,
                         "tags"         : ["workflow", "test"] ,
                         "metadata"     : {"author": "tester"}  }
        store_payload__base_64 = str_to_base64(json_to_str(store_payload))
        store_request = Schema__Cache__Store__Request(data=store_payload__base_64)
        payload       = store_request.json()
        store_response = self.client.post( f'/cache/store?namespace={namespace}',
                                           json = payload                     )
        assert store_response.status_code == 200
        store_result = store_response.json()
        cache_id     = store_result['cache_id']

        # 2. Retrieve by ID
        retrieve_response = self.client.get(f'/cache/retrieve?cache_id={cache_id}&namespace={namespace}')
        assert retrieve_response.status_code == 200
        retrieve_result = retrieve_response.json().get('data')
        stored_at       = retrieve_result.get('stored_at')
        assert retrieve_result == {  'content_type': '',
                                     'data'        : store_payload__base_64,
                                     'hash'        : '08bb3e4ab6',
                                     'metadata'    : {},
                                     'namespace'   : 'workflow-test',
                                     'stored_at'   : stored_at,
                                     'tags'        : [],
                                     'ttl_hours'   : 24         }



        # 3. Retrieve by hash
        hash_short   = safe_str_hash('an value')
        hash_response = self.client.get(f'/cache/retrieve-by-hash?hash={hash_short}&namespace={namespace}')
        assert hash_response.status_code == 200
        hash_result = hash_response.json()
        assert hash_result == {'message': 'retrieval by hash not supported', 'status': 'error'}
        #assert hash_result['data'] == self.test_data_base_64

        # # 4. Check stats
        # stats_response = self.client.get(f'/cache/stats?namespace={namespace}')
        # assert stats_response.status_code == 200
        # stats_result = stats_response.json()
        # assert stats_result['total_entries'] >= 1
        #
        # # 5. List namespaces
        # namespaces_response = self.client.get('/cache/namespaces')
        # assert namespaces_response.status_code == 200
        # namespaces_result = namespaces_response.json()
        # assert namespace in namespaces_result['namespaces']
        #
        # # 6. Clear
        # clear_response = self.client.post(f'/cache/clear?namespace={namespace}')
        # assert clear_response.status_code == 200
        # clear_result = clear_response.json()
        # assert clear_result['success'] is True
        #
        # # 7. Verify cleared
        # final_response = self.client.get(f'/cache/retrieve?cache_id={cache_id}&namespace={namespace}')
        # assert final_response.status_code == 200
        # final_result = final_response.json()
        # assert final_result['status'] == "not_found"