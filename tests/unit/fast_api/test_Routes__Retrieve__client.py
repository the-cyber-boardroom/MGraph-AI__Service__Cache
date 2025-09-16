import base64
import gzip
import json
from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid               import Random_Guid
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Retrieve__client(TestCase):                                      # Test retrieve routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        with setup__service_fast_api_test_objs() as _:
            cls.client = _.fast_api__client                                         # Reuse TestClient
            cls.app    = _.fast_api__app                                            # Reuse FastAPI app
            cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_namespace    = "test-retrieve-http"
        cls.test_string       = "test string data"
        cls.test_json         = {"test": "data", "number": 123}
        cls.path_now          = Path__Handler__Temporal().path_now()                # get the current temporal path from the handler

    def _store_test_data(self, data, data_type="string", namespace=None):           # Tets helper to store test data and return cache_id and hash
        namespace = namespace or self.test_namespace

        if data_type == "string":
            response = self.client.post(url     = f'/{namespace}/temporal/store/string',
                                        content = data,
                                        headers = {"Content-Type": "text/plain"})
        elif data_type == "json":
            response = self.client.post(url     = f'/{namespace}/temporal/store/json',
                                        json    = data)
        elif data_type == "binary":
            response = self.client.post(url     = f'/{namespace}/temporal/store/binary',
                                        content = data,
                                        headers = {"Content-Type": "application/octet-stream"})

        assert response.status_code == 200
        result = response.json()
        return result.get('cache_id'), result.get('hash')

    def test__retrieve__cache_id(self):                                             # Test retrieve by ID via HTTP
        cache_id, cache_hash = self._store_test_data(self.test_string)             # Store first

        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}')   # Retrieve by ID

        assert response.status_code == 200
        result = response.json()

        assert 'data' in result
        assert result['data'] == self.test_string
        assert 'metadata' in result
        assert result['metadata']['cache_id'] == cache_id

    def test__retrieve__hash__cache_hash(self):                                     # Test retrieve by hash via HTTP
        # Store first
        cache_id, cache_hash = self._store_test_data(self.test_string)

        # Retrieve by hash
        response = self.client.get(f'/{self.test_namespace}/retrieve/hash/{cache_hash}')

        assert response.status_code == 200
        result = response.json()

        # Hash retrieval should work
        assert 'data' in result
        assert result['data'] == self.test_string

    def test__retrieve__not_found(self):                                            # Test retrieve non-existent entry
        non_existent_id = Random_Guid()
        response = self.client.get(f'/{self.test_namespace}/retrieve/{non_existent_id}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"status": "not_found", "message": 'Cache entry not found'}

    def test__retrieve__string__by_id(self):                                        # Test string-specific retrieval via HTTP
        # Store string data
        cache_id, cache_hash = self._store_test_data(self.test_string)

        # Retrieve as string
        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/string')

        assert response.status_code      == 200
        assert response.text              == self.test_string
        assert response.headers['content-type'] == 'text/plain; charset=utf-8'

    def test__retrieve__json__by_id(self):                                          # Test JSON-specific retrieval via HTTP
        # Store JSON data
        cache_id, cache_hash = self._store_test_data(self.test_json, "json")

        # Retrieve as JSON
        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/json')

        assert response.status_code == 200
        assert response.json()       == self.test_json
        assert 'application/json' in response.headers['content-type']

    def test__retrieve__binary__by_id(self):                                        # Test binary retrieval via HTTP
        test_binary = b'\x01\x02\x03\x04\x05'

        # Store binary
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Retrieve as binary
        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/binary')

        assert response.status_code == 200
        assert response.content     == test_binary
        assert response.headers['content-type'] == 'application/octet-stream'

    def test__retrieve__binary__by_hash(self):                                      # Test binary retrieval by hash via HTTP
        test_binary = b'\x01\x02\x03\x04\x05'

        # Store binary
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Retrieve as binary by hash
        response = self.client.get(f'/{self.test_namespace}/retrieve/hash/{cache_hash}/binary')

        assert response.status_code == 200
        assert response.content     == test_binary
        assert response.headers['content-type'] == 'application/octet-stream'

    def test__retrieve__string__by_hash(self):                                      # Test string retrieval by hash via HTTP
        # Store string
        cache_id, cache_hash = self._store_test_data(self.test_string)

        # Retrieve as string by hash
        response = self.client.get(f'/{self.test_namespace}/retrieve/hash/{cache_hash}/string')

        assert response.status_code == 200
        assert response.text         == self.test_string
        assert 'text/plain' in response.headers['content-type']

    def test__retrieve__json__by_hash(self):                                        # Test JSON retrieval by hash via HTTP
        # Store JSON
        cache_id, cache_hash = self._store_test_data(self.test_json, "json")

        # Retrieve as JSON by hash
        response = self.client.get(f'/{self.test_namespace}/retrieve/hash/{cache_hash}/json')

        assert response.status_code == 200
        assert response.json()       == self.test_json
        assert 'application/json' in response.headers['content-type']

    def test__retrieve__binary_redirect(self):                                      # Test binary data redirect
        # Create test binary data
        test_binary = b'\x00\x01\x02\x03\x04\x05'

        # Store binary
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Retrieve as generic JSON - should redirect to binary endpoint
        response_generic = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}')
        assert response_generic.status_code == 200
        result_generic = response_generic.json()

        # Should get redirect message, not the actual data
        assert result_generic['status']     == 'binary_data'
        assert result_generic['data_type']  == 'binary'
        assert result_generic['size']       == len(test_binary)

        assert result_generic['binary_url'] == f'/{self.test_namespace}/retrieve/{cache_id}/binary'
        assert 'data' not in result_generic  # No actual data in JSON response

        redirect_response = self.client.get(result_generic['binary_url'])
        assert redirect_response.content == test_binary

    # todo: review this compressed workflow (since a) we might want to do this on a separate endpoint, and b) we might want to handle the case where we also return the gzip (which might not be a bad idea for normal text and json data, since that will reduce the network traffic)
    def test__retrieve__compressed_data(self):                                      # Test compressed data via HTTP
        original_text   = "This will be compressed" * 100
        compressed_data = gzip.compress(original_text.encode())

        # Store compressed
        response_store = self.client.post(url     = f'/{self.test_namespace}/temporal/store/binary',
                                          content = compressed_data                         ,
                                          headers = {"Content-Type": "application/octet-stream",
                                                      "Content-Encoding": "gzip"                  })

        assert response_store.status_code == 200
        cache_id = response_store.json()['cache_id']

        # Retrieve via binary endpoint - gets decompressed data
        response_binary = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/binary')
        assert response_binary.status_code == 200
        assert response_binary.content     == original_text.encode()  # Decompressed

        # Retrieve via JSON endpoint - should redirect
        response_json     = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}')
        result            = response_json.json()
        response_redirect = self.client.get(result['binary_url'])
        assert response_json.status_code == 200


        assert result['status']     == 'binary_data'
        assert result['data_type']  == 'binary'
        assert result['size']       == len(original_text.encode())  # Size of decompressed data
        assert 'data' not in result  # No actual data in JSON response
        assert original_text == response_redirect.text


    def test__retrieve__type_specific_endpoints(self):                              # Test all type-specific endpoints
        # Store JSON data
        test_json = {"test": "data", "value": 42}
        cache_id, cache_hash = self._store_test_data(test_json, "json")

        # Test all retrieval endpoints
        endpoints = [(f'/{self.test_namespace}/retrieve/{cache_id}/string'       , 'text/plain'              ),
                     (f'/{self.test_namespace}/retrieve/{cache_id}/json'         , 'application/json'        ),
                     (f'/{self.test_namespace}/retrieve/{cache_id}/binary'       , 'application/octet-stream'),
                     (f'/{self.test_namespace}/retrieve/hash/{cache_hash}/string', 'text/plain'              ),
                     (f'/{self.test_namespace}/retrieve/hash/{cache_hash}/json'  , 'application/json'        ),
                     (f'/{self.test_namespace}/retrieve/hash/{cache_hash}/binary', 'application/octet-stream'),]

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

    def test__retrieve__404_responses(self):                                        # Test 404 responses for type-specific endpoints
        non_existent_id   = Random_Guid()
        non_existent_hash = "0000000000000000"

        # String endpoints should return 404
        response_string_id = self.client.get(f'/{self.test_namespace}/retrieve/{non_existent_id}/string')
        assert response_string_id.status_code == 404

        response_string_hash = self.client.get(f'/{self.test_namespace}/retrieve/hash/{non_existent_hash}/string')
        assert response_string_hash.status_code == 404

        # JSON endpoints should return JSON error
        response_json_id = self.client.get(f'/{self.test_namespace}/retrieve/{non_existent_id}/json')
        assert response_json_id.status_code == 200
        assert response_json_id.json() == {"status": "not_found", "message": "Cache entry not found"}

        response_json_hash = self.client.get(f'/{self.test_namespace}/retrieve/hash/{non_existent_hash}/json')
        assert response_json_hash.status_code == 200
        assert response_json_hash.json() == {"status": "not_found", "message": "Cache entry not found"}

        # Binary endpoints should return 404
        response_binary_id = self.client.get(f'/{self.test_namespace}/retrieve/{non_existent_id}/binary')
        assert response_binary_id.status_code == 404

        response_binary_hash = self.client.get(f'/{self.test_namespace}/retrieve/hash/{non_existent_hash}/binary')
        assert response_binary_hash.status_code == 404

    def test__retrieve__cross_type_retrieval(self):                                 # Test retrieving data as different types
        # Store string that is valid JSON
        valid_json_string = '{"valid": "json"}'
        cache_id, cache_hash = self._store_test_data(valid_json_string)

        # Retrieve as string - returns as-is
        response_string = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/string')
        assert response_string.status_code == 200
        assert response_string.text        == valid_json_string

        # Retrieve as JSON - should parse it
        response_json = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/json')
        assert response_json.status_code == 200
        assert response_json.json()      == {"valid": "json"}

        # Retrieve as binary - should convert to bytes
        response_binary = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/binary')
        assert response_binary.status_code == 200
        assert response_binary.content     == valid_json_string.encode('utf-8')

    def test__retrieve__json_as_string(self):                                       # Test JSON data retrieved as string
        # Store JSON
        cache_id, cache_hash = self._store_test_data(self.test_json, "json")

        # Retrieve as string - should stringify JSON
        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/string')

        assert response.status_code == 200
        assert json.loads(response.text) == self.test_json  # Can parse back to original JSON

    def test__retrieve__binary_as_string_base64(self):                              # Test non-UTF8 binary retrieved as string
        non_utf8_binary      = b'\xff\xfe\x00\x01\x02\x03'                          # Non-UTF8 binary data
        cache_id, cache_hash = self._store_test_data(non_utf8_binary, "binary")

        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/string')    # Retrieve as string - should return base64

        assert response.status_code == 200                                                  # Should be base64 encoded since it can't be decoded as UTF-8
        assert response.text        == base64.b64encode(non_utf8_binary).decode('utf-8')

    def test__retrieve__binary_as_json(self):                                       # Test binary data retrieved as JSON
        test_binary = b'\x00\x01\x02\x03'

        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Retrieve as JSON - should return base64 wrapper
        response = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}/json')

        assert response.status_code == 200
        result = response.json()

        assert result == {"data_type": "binary"                                ,
                          "encoding" : "base64"                                ,
                          "data"     : base64.b64encode(test_binary).decode('utf-8')}

    def test__retrieve__default_namespace(self):                                    # Test operations without namespace (uses default)
        # Store without namespace
        cache_id, cache_hash = self._store_test_data("default namespace test", namespace="default")

        # Retrieve without explicit namespace
        response_retrieve = self.client.get(f'/default/retrieve/{cache_id}')
        assert response_retrieve.status_code == 200
        assert response_retrieve.json()['data'] == "default namespace test"

        # Check hash retrieval
        response_hash = self.client.get(f'/default/retrieve/hash/{cache_hash}')
        assert response_hash.status_code == 200
        assert response_hash.json()['data'] == "default namespace test"

    def test__retrieve__binary_redirect_message(self):                              # Test binary data redirect message detail
        test_binary = b'Some binary data here'

        # Store binary
        cache_id, cache_hash = self._store_test_data(test_binary, "binary")

        # Try generic retrieval by ID - should redirect
        response_by_id = self.client.get(f'/{self.test_namespace}/retrieve/{cache_id}')
        assert response_by_id.status_code == 200

        result_by_id = response_by_id.json()
        assert result_by_id["status"]     == "binary_data"
        assert result_by_id["message"]    == "Binary data cannot be returned in JSON response"
        assert result_by_id["data_type"]  == "binary"
        assert result_by_id["size"]       == len(test_binary)
        assert result_by_id["binary_url"] == f"/{self.test_namespace}/retrieve/{cache_id}/binary"

        assert self.client.get(result_by_id["binary_url"] ).content == test_binary                  # confirm binary_url works

        # Try generic retrieval by hash - should redirect
        response_by_hash = self.client.get(f'/{self.test_namespace}/retrieve/hash/{cache_hash}')
        assert response_by_hash.status_code == 200

        result_by_hash = response_by_hash.json()
        assert result_by_hash["status"]     == "binary_data"
        assert result_by_hash["binary_url"] == f"/{self.test_namespace}/retrieve/hash/{cache_hash}/binary"
        assert self.client.get(result_by_hash["binary_url"] ).content == test_binary                        # confirm binary_url works for hashes

    def test__retrieve__workflow_complete(self):                                    # Test complete retrieval workflow
        namespace = "retrieve-workflow-test"
        test_data = {"workflow": "test", "step": 1}

        # 1. Store JSON
        cache_id_json, cache_hash_json = self._store_test_data(test_data, "json", namespace)

        # 2. Retrieve by ID
        response_id = self.client.get(f'/{namespace}/retrieve/{cache_id_json}')
        assert response_id.status_code == 200
        assert response_id.json()['data'] == test_data

        # 3. Retrieve by hash
        response_hash = self.client.get(f'/{namespace}/retrieve/hash/{cache_hash_json}')
        assert response_hash.status_code == 200
        assert response_hash.json()['data'] == test_data

        # 4. Retrieve as different types
        response_string = self.client.get(f'/{namespace}/retrieve/{cache_id_json}/string')
        assert response_string.status_code == 200
        assert json.loads(response_string.text) == test_data

        response_binary = self.client.get(f'/{namespace}/retrieve/{cache_id_json}/binary')
        assert response_binary.status_code == 200
        assert json.loads(response_binary.content.decode()) == test_data