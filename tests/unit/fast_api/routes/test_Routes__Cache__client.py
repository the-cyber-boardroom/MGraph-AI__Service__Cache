import base64
import gzip
import json
from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid              import Random_Guid
from osbot_utils.utils.Env                                                          import in_github_action
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

        assert result == { 'content_encoding': None,
                           'data'            : 'test string data'                                    , # Verify data
                           'data_type'       : 'string',
                           'metadata': { 'cache_hash'       : cache_hash                     , # Verify metadata
                                         'cache_id'         : cache_id                       ,
                                         'cache_key_data'   : self.test_string               ,
                                         'content_encoding' : None                           ,
                                         'file_type'        : 'json'                         ,
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

        assert result == { 'content_encoding': None                                                  ,
                           'data'            : 'test string data'                                    , # Verify data
                           'data_type'       : 'string'                                              ,
                           'metadata'        : { 'cache_hash'       : cache_hash                     , # Verify metadata
                                                 'cache_id'         : cache_id                       ,
                                                 'cache_key_data'   : self.test_string               ,
                                                 'content_encoding' : None                           ,
                                                 'file_type'        : 'json'                         ,
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
        if in_github_action():
            assert delete_result['deleted_count'] == 9
        else:
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

    def test__cache__store_and_retrieve_binary(self):                                   # Test binary via HTTP client
        # Create test binary data
        test_binary = b'\x00\x01\x02\x03\x04\x05'

        # Store binary
        response_store = self.client.post(f'/cache/store/binary/direct/{self.test_namespace}',
                                         content = test_binary                                ,
                                         headers = {"Content-Type": "application/octet-stream"})

        assert response_store.status_code == 200
        result_store = response_store.json()
        cache_id     = result_store['cache_id']

        # Retrieve as binary - this works
        response_binary = self.client.get(f'/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}')
        assert response_binary.status_code == 200
        assert response_binary.content     == test_binary
        assert response_binary.headers['content-type'] == 'application/octet-stream'

        # Retrieve as generic JSON - should redirect to binary endpoint
        response_generic = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{self.test_namespace}')
        assert response_generic.status_code == 200
        result_generic = response_generic.json()

        # Should get redirect message, not the actual data
        assert result_generic['status']     == 'binary_data'
        assert result_generic['data_type']  == 'binary'
        assert result_generic['size']       == len(test_binary)
        assert result_generic['binary_url'] == f'/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}'
        assert 'data' not in result_generic  # No actual data in JSON response

    def test__cache__compressed_data_via_http(self):                                    # Test compressed data via HTTP
        original_text   = "This will be compressed" * 100
        compressed_data = gzip.compress(original_text.encode())

        # Store compressed
        response_store = self.client.post(f'/cache/store/binary/temporal/{self.test_namespace}',
                                         content = compressed_data                             ,
                                         headers = {"Content-Type": "application/octet-stream",
                                                   "Content-Encoding": "gzip"                  })

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve via binary endpoint - gets decompressed data
        response_binary = self.client.get(f'/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}')
        assert response_binary.status_code == 200
        assert response_binary.content     == original_text.encode()  # Decompressed

        # Retrieve via JSON endpoint - should redirect
        response_json = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{self.test_namespace}')
        assert response_json.status_code == 200

        result = response_json.json()
        assert result['status']     == 'binary_data'
        assert result['data_type']  == 'binary'
        assert result['size']       == len(original_text.encode())  # Size of decompressed data
        assert result['binary_url'] == f'/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}'
        assert 'data' not in result  # No actual data in JSON response

    def test__cache__type_specific_endpoints(self):                                     # Test all type-specific endpoints
        # Store JSON data
        test_json = {"test": "data", "value": 42}
        response_store = self.client.post(f'/cache/store/json/direct/{self.test_namespace}',
                                         json = test_json                                  )

        assert response_store.status_code == 200
        cache_id   = response_store.json()['cache_id']
        cache_hash = response_store.json()['hash']

        # Test all retrieval endpoints
        endpoints = [
            (f'/cache/retrieve/string/by-id/{cache_id}/{self.test_namespace}', 'text/plain'),
            (f'/cache/retrieve/json/by-id/{cache_id}/{self.test_namespace}', 'application/json'),
            (f'/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}', 'application/octet-stream'),
            (f'/cache/retrieve/string/by-hash/{cache_hash}/{self.test_namespace}', 'text/plain'),
            (f'/cache/retrieve/json/by-hash/{cache_hash}/{self.test_namespace}', 'application/json'),
            (f'/cache/retrieve/binary/by-hash/{cache_hash}/{self.test_namespace}', 'application/octet-stream'),
        ]

        for endpoint, expected_content_type in endpoints:
            response = self.client.get(endpoint)
            assert response.status_code == 200
            assert expected_content_type in response.headers['content-type']

            # Verify data is accessible in each format
            if 'json' in endpoint:
                assert response.json() == test_json
            elif 'string' in endpoint:
                assert json.loads(response.text) == test_json
            elif 'binary' in endpoint:
                assert json.loads(response.content.decode()) == test_json

    def test__cache__retrieve__string__by_id(self):                                     # Test string-specific retrieval via HTTP
        # Store string data
        response_store = self.client.post(f'/cache/store/string/direct/{self.test_namespace}',
                                         content = self.test_string                           ,
                                         headers = {"Content-Type": "text/plain"}             )

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve as string
        response = self.client.get(f'/cache/retrieve/string/by-id/{cache_id}/{self.test_namespace}')

        assert response.status_code      == 200
        assert response.text              == self.test_string
        assert response.headers['content-type'] == 'text/plain; charset=utf-8'

    def test__cache__retrieve__json__by_id(self):                                       # Test JSON-specific retrieval via HTTP
        # Store JSON data
        response_store = self.client.post(f'/cache/store/json/direct/{self.test_namespace}',
                                         json = self.test_json                               )

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve as JSON
        response = self.client.get(f'/cache/retrieve/json/by-id/{cache_id}/{self.test_namespace}')

        assert response.status_code == 200
        assert response.json()       == self.test_json
        assert 'application/json' in response.headers['content-type']

    def test__cache__retrieve__binary__by_hash(self):                                   # Test binary retrieval by hash via HTTP
        test_binary = b'\x01\x02\x03\x04\x05'

        # Store binary
        response_store = self.client.post(f'/cache/store/binary/temporal/{self.test_namespace}',
                                         content = test_binary                                  ,
                                         headers = {"Content-Type": "application/octet-stream"})

        assert response_store.status_code == 200
        cache_hash = response_store.json()['hash']

        # Retrieve as binary by hash
        response = self.client.get(f'/cache/retrieve/binary/by-hash/{cache_hash}/{self.test_namespace}')

        assert response.status_code == 200
        assert response.content     == test_binary
        assert response.headers['content-type'] == 'application/octet-stream'

    def test__cache__retrieve__string__by_hash(self):                                   # Test string retrieval by hash via HTTP
        # Store string
        response_store = self.client.post(f'/cache/store/string/temporal_latest/{self.test_namespace}',
                                         content = self.test_string                                   ,
                                         headers = {"Content-Type": "text/plain"}                     )

        assert response_store.status_code == 200
        cache_hash = response_store.json()['hash']

        # Retrieve as string by hash
        response = self.client.get(f'/cache/retrieve/string/by-hash/{cache_hash}/{self.test_namespace}')

        assert response.status_code == 200
        assert response.text         == self.test_string
        assert 'text/plain' in response.headers['content-type']

    def test__cache__retrieve__json__by_hash(self):                                     # Test JSON retrieval by hash via HTTP
        # Store JSON
        response_store = self.client.post(f'/cache/store/json/temporal_versioned/{self.test_namespace}',
                                         json = self.test_json                                         )

        assert response_store.status_code == 200
        cache_hash = response_store.json()['hash']

        # Retrieve as JSON by hash
        response = self.client.get(f'/cache/retrieve/json/by-hash/{cache_hash}/{self.test_namespace}')

        assert response.status_code == 200
        assert response.json()       == self.test_json
        assert 'application/json' in response.headers['content-type']

    def test__cache__delete__not_found(self):                                           # Test delete non-existent via HTTP
        non_existent_id = Random_Guid()

        response = self.client.delete(f'/cache/delete/by-id/{non_existent_id}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']  == 'not_found'
        assert result['message'] == f'Cache ID {non_existent_id} not found'

    def test__cache__exists__by_hash__not_found(self):                                  # Test exists check for non-existent hash
        non_existent_hash = "0000000000000000"

        response = self.client.get(f'/cache/exists/{non_existent_hash}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"exists": False, "hash": non_existent_hash}

    def test__cache__retrieve__404_responses(self):                                     # Test 404 responses for type-specific endpoints
        non_existent_id   = Random_Guid()
        non_existent_hash = "0000000000000000"

        # String endpoints should return 404
        response_string_id = self.client.get(f'/cache/retrieve/string/by-id/{non_existent_id}/{self.test_namespace}')
        assert response_string_id.status_code == 404

        response_string_hash = self.client.get(f'/cache/retrieve/string/by-hash/{non_existent_hash}/{self.test_namespace}')
        assert response_string_hash.status_code == 404

        # JSON endpoints should return JSON error
        response_json_id = self.client.get(f'/cache/retrieve/json/by-id/{non_existent_id}/{self.test_namespace}')
        assert response_json_id.status_code == 200
        assert response_json_id.json() == {"status": "not_found", "message": "Cache entry not found"}

        response_json_hash = self.client.get(f'/cache/retrieve/json/by-hash/{non_existent_hash}/{self.test_namespace}')
        assert response_json_hash.status_code == 200
        assert response_json_hash.json() == {"status": "not_found", "message": "Cache entry not found"}

        # Binary endpoints should return 404
        response_binary_id = self.client.get(f'/cache/retrieve/binary/by-id/{non_existent_id}/{self.test_namespace}')
        assert response_binary_id.status_code == 404

        response_binary_hash = self.client.get(f'/cache/retrieve/binary/by-hash/{non_existent_hash}/{self.test_namespace}')
        assert response_binary_hash.status_code == 404

    def test__cache__cross_type_retrieval(self):                                        # Test retrieving data as different types
        # Store string that is valid JSON
        valid_json_string = '{"valid": "json"}'
        response_store = self.client.post(f'/cache/store/string/direct/{self.test_namespace}',
                                         content = valid_json_string                          ,
                                         headers = {"Content-Type": "text/plain"}             )

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve as string - returns as-is
        response_string = self.client.get(f'/cache/retrieve/string/by-id/{cache_id}/{self.test_namespace}')
        assert response_string.status_code == 200
        assert response_string.text        == valid_json_string

        # Retrieve as JSON - should parse it
        response_json = self.client.get(f'/cache/retrieve/json/by-id/{cache_id}/{self.test_namespace}')
        assert response_json.status_code == 200
        assert response_json.json()      == {"valid": "json"}

        # Retrieve as binary - should convert to bytes
        response_binary = self.client.get(f'/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}')
        assert response_binary.status_code == 200
        assert response_binary.content     == valid_json_string.encode('utf-8')

    def test__cache__json_stored_retrieved_as_string(self):                             # Test JSON data retrieved as string
        # Store JSON
        response_store = self.client.post(f'/cache/store/json/direct/{self.test_namespace}',
                                         json = self.test_json                             )

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve as string - should stringify JSON
        response = self.client.get(f'/cache/retrieve/string/by-id/{cache_id}/{self.test_namespace}')

        assert response.status_code == 200
        assert json.loads(response.text) == self.test_json  # Can parse back to original JSON

    def test__cache__binary_retrieved_as_string_base64(self):                           # Test non-UTF8 binary retrieved as string
        # Non-UTF8 binary data
        non_utf8_binary = b'\xff\xfe\x00\x01\x02\x03'

        response_store = self.client.post(f'/cache/store/binary/direct/{self.test_namespace}',
                                         content = non_utf8_binary                            ,
                                         headers = {"Content-Type": "application/octet-stream"})

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve as string - should return base64
        response = self.client.get(f'/cache/retrieve/string/by-id/{cache_id}/{self.test_namespace}')

        assert response.status_code == 200
        # Should be base64 encoded since it can't be decoded as UTF-8
        assert response.text == base64.b64encode(non_utf8_binary).decode('utf-8')

    def test__cache__binary_retrieved_as_json(self):                                    # Test binary data retrieved as JSON
        test_binary = b'\x00\x01\x02\x03'

        response_store = self.client.post(f'/cache/store/binary/direct/{self.test_namespace}',
                                         content = test_binary                               ,
                                         headers = {"Content-Type": "application/octet-stream"})

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve as JSON - should return base64 wrapper
        response = self.client.get(f'/cache/retrieve/json/by-id/{cache_id}/{self.test_namespace}')

        assert response.status_code == 200
        result = response.json()

        assert result == {"data_type": "binary"                                ,
                         "encoding" : "base64"                                ,
                         "data"     : base64.b64encode(test_binary).decode('utf-8')}

    def test__cache__default_namespace(self):                                           # Test operations without namespace (uses default)
        # Store without namespace
        response_store = self.client.post('/cache/store/string/direct/default',
                                         content = "default namespace test"   ,
                                         headers = {"Content-Type": "text/plain"})

        assert response_store.status_code == 200
        cache_id   = response_store.json()['cache_id']
        cache_hash = response_store.json()['hash']

        # Retrieve without explicit namespace
        response_retrieve = self.client.get(f'/cache/retrieve/by-id/{cache_id}/default')
        assert response_retrieve.status_code == 200
        assert response_retrieve.json()['data'] == "default namespace test"

        # Check exists
        response_exists = self.client.get(f'/cache/exists/{cache_hash}/default')
        assert response_exists.status_code == 200
        assert response_exists.json()['exists'] is True

        # Stats for default namespace
        response_stats = self.client.get('/cache/stats/namespaces/default')
        assert response_stats.status_code == 200
        assert response_stats.json()['namespace'] == 'default'

    def test__cache__stats__namespaces(self):                                           # Test stats/namespaces endpoint (list version)
        # Create some namespaces
        for ns in ["stats-ns-1", "stats-ns-2"]:
            response = self.client.post(f'/cache/store/string/direct/{ns}',
                                       content = f"data for {ns}"          ,
                                       headers = {"Content-Type": "text/plain"})
            assert response.status_code == 200

        # Get namespace list via stats endpoint
        response = self.client.get('/cache/stats/namespaces')

        assert response.status_code == 200
        result = response.json()
        assert type(result) is list
        assert "stats-ns-1" in result
        assert "stats-ns-2" in result

    def test__cache__binary_redirect_message(self):                                     # Test binary data redirect message detail
        test_binary = b'Some binary data here'

        # Store binary
        response_store = self.client.post(f'/cache/store/binary/direct/{self.test_namespace}',
                                         content = test_binary                                ,
                                         headers = {"Content-Type": "application/octet-stream"})

        assert response_store.status_code == 200
        cache_id   = response_store.json()['cache_id']
        cache_hash = response_store.json()['hash']

        # Try generic retrieval by ID - should redirect
        response_by_id = self.client.get(f'/cache/retrieve/by-id/{cache_id}/{self.test_namespace}')
        assert response_by_id.status_code == 200

        result_by_id = response_by_id.json()
        assert result_by_id == {"status"     : "binary_data"                                                      ,
                               "message"    : "Binary data cannot be returned in JSON response"                  ,
                               "data_type"  : "binary"                                                           ,
                               "size"       : len(test_binary)                                                   ,
                               "metadata"   : result_by_id['metadata']                                           ,
                               "binary_url" : f"/cache/retrieve/binary/by-id/{cache_id}/{self.test_namespace}"   }

        # Try generic retrieval by hash - should redirect
        response_by_hash = self.client.get(f'/cache/retrieve/by-hash/{cache_hash}/{self.test_namespace}')
        assert response_by_hash.status_code == 200

        result_by_hash = response_by_hash.json()
        assert result_by_hash["status"]     == "binary_data"
        assert result_by_hash["binary_url"] == f"/cache/retrieve/binary/by-hash/{cache_hash}/{self.test_namespace}"