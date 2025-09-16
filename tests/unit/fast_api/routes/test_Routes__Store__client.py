import gzip
import json
from unittest                                                                       import TestCase
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.AWS_Config                                                           import aws_config
from osbot_utils.utils.Misc                                                         import is_guid
from tests.unit.Service__Fast_API__Test_Objs                                        import setup__service_fast_api_test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Store__client(TestCase):                                         # Test store routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        with setup__service_fast_api_test_objs() as _:
            cls.client = _.fast_api__client                                         # Reuse TestClient
            cls.app    = _.fast_api__app                                            # Reuse FastAPI app
            cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        assert aws_config.account_id()  == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_namespace    = "test-store-http"
        cls.test_string       = "test string data"
        cls.test_json         = {"test": "data", "number": 123}
        cls.path_now          = Path__Handler__Temporal().path_now()                # get the current temporal path from the handler

    def test__store__string(self):                                                  # Test string storage via HTTP
        # Store string directly in body
        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string',
                                    content = self.test_string                               ,
                                    headers = {"Content-Type": "text/plain"}                 )

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

    def test__store__json(self):                                                    # Test JSON storage via HTTP
        # Store JSON directly in body
        response = self.client.post(url  = f'/{self.test_namespace}/temporal/store/json',
                                    json = self.test_json )

        assert response.status_code == 200
        result = response.json()
        cache_id = result.get('cache_id')
        cache_hash = result.get('hash')

        assert is_guid(cache_id) is True
        assert type(cache_hash) is str
        assert len(cache_hash) == 16

        return cache_id, cache_hash

    def test__store__binary(self):                                                  # Test binary storage via HTTP
        # Create test binary data
        test_binary = b'\x00\x01\x02\x03\x04\x05'

        # Store binary
        response = self.client.post(url  = f'/{self.test_namespace}/direct/store/binary',
                                   content = test_binary                           ,
                                   headers = {"Content-Type": "application/octet-stream"})

        assert response.status_code == 200
        result = response.json()
        cache_id     = result['cache_id']
        cache_hash   = result['hash']

        assert is_guid(cache_id) is True
        assert type(cache_hash) is str
        assert result['size'] == len(test_binary)

        return cache_id, cache_hash

    def test__store__multiple_strategies(self):                                     # Test different storage strategies
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            response = self.client.post(url    = f'/{self.test_namespace}/{strategy}/store/string',
                                        content= f"test data for {strategy}"                      ,
                                        headers= {"Content-Type": "text/plain"}                   )

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

    def test__store__compressed_data(self):                                         # Test compressed data via HTTP
        original_text   = "This will be compressed" * 100
        compressed_data = gzip.compress(original_text.encode())

        # Store compressed
        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/binary',
                                    content = compressed_data                      ,
                                    headers = {"Content-Type": "application/octet-stream",
                                             "Content-Encoding": "gzip"          })

        assert response.status_code == 200
        result = response.json()
        cache_id = result['cache_id']

        assert is_guid(cache_id) is True
        # Size should reflect the compressed size
        assert result['size'] < len(original_text)

    def test__store__string_special_characters(self):                               # Test string with special characters
        special_string = "Test with special: 你好世界 🚀 \n\t\r"

        response = self.client.post(f'/{self.test_namespace}/direct/store/string',
                                   content = special_string                        ,
                                   headers = {"Content-Type": "text/plain; charset=utf-8"})

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'hash' in result
        assert is_guid(result['cache_id']) is True

    def test__store__json_with_nulls(self):                                         # Test JSON with null values
        json_with_nulls = {
            "key1": None,
            "key2": "value",
            "key3": [1, None, 3],
            "key4": {"nested": None}
        }

        response = self.client.post(f'/{self.test_namespace}/temporal_latest/store/json',
                                   json = json_with_nulls                                )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'hash' in result
        assert is_guid(result['cache_id']) is True

    def test__store__large_json(self):                                              # Test large JSON storage
        large_json = {
            f"key_{i}": {
                "data": f"value_{i}",
                "nested": {"level": 2, "items": list(range(10))}
            } for i in range(100)
        }

        response = self.client.post(f'/{self.test_namespace}/temporal_versioned/store/json',
                                   json = large_json                                       )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'size' in result
        assert result['size'] > 1000  # Should be reasonably large

    def test__store__empty_data(self):                                              # Test storing empty data
        # Empty string
        response_string = self.client.post(f'/{ self.test_namespace}/direct/store/string',
                                                content = ""                             ,
                                                headers = {"Content-Type": "text/plain"} )

        assert response_string.status_code == 400                                               # empty content for strings is not supported
        assert response_string.json()      == {'detail': [{'input': None,
                                                           'loc': ['body'],
                                                           'msg': 'Field required',
                                                           'type': 'missing'}]}


        # Empty JSON object
        response_json = self.client.post(f'/{self.test_namespace}/direct/store/json',
                                        json = {}                                   )

        assert response_json.status_code == 200
        result_json = response_json.json()
        assert 'cache_id' in result_json

        # Empty binary
        response_binary = self.client.post(f'/{self.test_namespace}/direct/store/binary',
                                          content = b''                                  ,
                                          headers = {"Content-Type": "application/octet-stream"})

        assert response_binary.status_code == 400                                               # empty binary/bytes are not supported
        assert response_binary.json()      == {'detail': [{ 'input': None,
                                                            'loc': ['body'],
                                                            'msg': 'Field required',
                                                            'type': 'missing'}]}
        # result_binary = response_binary.json()
        # assert result_binary['size'] == 0

    def test__store__same_data_different_namespaces(self):                          # Test same data in different namespaces
        test_data = "namespace isolation test"
        ns1       = "test-store-ns1"
        ns2       = "test-store-ns2"

        # Store in namespace 1
        response1 = self.client.post(f'/{ns1}/direct/store/string',
                                    content = test_data                ,
                                    headers = {"Content-Type": "text/plain"})

        # Store in namespace 2
        response2 = self.client.post(f'/{ns2}/direct/store/string',
                                    content = test_data                ,
                                    headers = {"Content-Type": "text/plain"})

        assert response1.status_code == 200
        assert response2.status_code == 200

        result1 = response1.json()
        result2 = response2.json()

        # Different cache IDs (unique per store operation)
        assert result1['cache_id'] != result2['cache_id']

        # Same hash (same data)
        assert result1['hash'] == result2['hash']

    def test__store__workflow_complete(self):                                       # Test complete storage workflow
        namespace = "store-workflow-test"
        test_data = {"workflow": "test", "step": 1}

        # 1. Store JSON
        store_response = self.client.post(url  = f'/{namespace}/temporal/store/json',
                                          json = test_data                          )
        assert store_response.status_code == 200
        store_result = store_response.json()
        cache_id = store_result['cache_id']
        cache_hash = store_result['hash']

        assert is_guid(cache_id) is True
        assert len(cache_hash) == 16
        assert 'paths' in store_result
        assert 'size' in store_result

        # 2. Store string
        string_response = self.client.post(f'/{namespace}/direct/store/string',
                                          content = "test string"             ,
                                          headers = {"Content-Type": "text/plain"})
        assert string_response.status_code == 200
        string_result = string_response.json()

        assert is_guid(string_result['cache_id']) is True

        # 3. Store binary
        binary_response = self.client.post(f'/{namespace}/temporal_latest/store/binary',
                                          content = b'test binary data'                 ,
                                          headers = {"Content-Type": "application/octet-stream"})
        assert binary_response.status_code == 200
        binary_result = binary_response.json()

        assert is_guid(binary_result['cache_id']) is True

    def test__store__duplicate_data(self):                                          # Test storing duplicate data
        test_string = "duplicate test data"

        # Store first time
        response1 = self.client.post(f'/{self.test_namespace}/temporal/store/string',
                                    content = test_string                            ,
                                    headers = {"Content-Type": "text/plain"}        )

        # Store second time (same data)
        response2 = self.client.post(f'/{self.test_namespace}/temporal/store/string',
                                    content = test_string                            ,
                                    headers = {"Content-Type": "text/plain"}        )

        assert response1.status_code == 200
        assert response2.status_code == 200

        result1 = response1.json()
        result2 = response2.json()

        # Same hash (same data)
        assert result1['hash'] == result2['hash']

        # Different cache IDs (separate store operations)
        assert result1['cache_id'] != result2['cache_id']

    def test__store__default_namespace(self):                                       # Test operations without namespace (uses default)
        # Store without namespace (should use default)
        response = self.client.post('/default/direct/store/string',
                                   content = "default namespace test"     ,
                                   headers = {"Content-Type": "text/plain"})

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'hash' in result
        assert is_guid(result['cache_id']) is True

    def test__store__large_binary_file(self):                                       # Test handling of larger binary files
        # Create a 1MB binary file
        large_binary = bytes([i % 256 for i in range(1024 * 1024)])

        response = self.client.post(f'/{self.test_namespace}/direct/store/binary',
                                   content = large_binary                         ,
                                   headers = {"Content-Type": "application/octet-stream"})

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'size' in result
        assert result['size'] == len(large_binary)

    def test__store__json_as_string(self):                                          # Test storing JSON-like string
        json_string = '{"valid": "json", "as": "string"}'

        response = self.client.post(f'/{self.test_namespace}/direct/store/string',
                                   content = json_string                          ,
                                   headers = {"Content-Type": "text/plain"}      )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'hash' in result
        assert is_guid(result['cache_id']) is True

    def test__store__binary_that_looks_like_text(self):                             # Test binary that could be text
        text_like_binary = b"This looks like text but is stored as binary"

        response = self.client.post(f'/{self.test_namespace}/temporal_versioned/store/binary',
                                   content = text_like_binary                                ,
                                   headers = {"Content-Type": "application/octet-stream"}   )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'size' in result
        assert result['size'] == len(text_like_binary)