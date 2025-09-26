import gzip
from unittest                                                                       import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                             import skip__if_not__in_github_actions
from memory_fs.path_handlers.Path__Handler__Temporal                                import Path__Handler__Temporal
from osbot_utils.utils.Misc                                                         import is_guid
from tests.unit.Service__Cache__Test_Objs                                           import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__File__Store__client(TestCase):                                             # Test store routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        cls.test_objs      = setup__service__cache__test_objs()
        cls.cache_fixtures = cls.test_objs.cache_fixtures
        cls.client         = cls.test_objs.fast_api__client
        cls.app            = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        cls.test_namespace = "test-store-http"                                          # Test data
        cls.test_string    = "test string data"
        cls.test_json      = {"test": "data", "number": 123}
        cls.path_now       = Path__Handler__Temporal().path_now()                       # Current temporal path

    def test__store__string(self):                                                      # Test string storage via HTTP
        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string',
                                    content = self.test_string                               ,
                                    headers = {"Content-Type": "text/plain"}                 )

        assert response.status_code == 200
        result     = response.json()
        cache_id   = result.get('cache_id'  )
        cache_hash = result.get('cache_hash')

        assert is_guid(cache_id)    is True
        assert type(cache_hash)     is str
        assert len(cache_hash)      == 16                                               # Default hash length

        assert 'paths'    in result                                                     # Verify paths structure
        assert 'data'     in result['paths']
        assert 'by_hash'  in result['paths']
        assert 'by_id'    in result['paths']

        return cache_id, cache_hash

    def test__store__json(self):                                                        # Test JSON storage via HTTP
        response = self.client.post(url  = f'/{self.test_namespace}/temporal/store/json',
                                    json = self.test_json                               )

        assert response.status_code == 200
        result     = response.json()
        cache_id   = result.get('cache_id'  )
        cache_hash = result.get('cache_hash')

        assert is_guid(cache_id)    is True
        assert type(cache_hash)     is str
        assert len(cache_hash)      == 16

        return cache_id, cache_hash

    def test__store__binary(self):                                                      # Test binary storage via HTTP
        test_binary = b'\x00\x01\x02\x03\x04\x05'

        response = self.client.post(url     = f'/{self.test_namespace}/direct/store/binary'        ,
                                    content = test_binary                                          ,
                                    headers = {"Content-Type": "application/octet-stream"}        )

        assert response.status_code == 200
        result     = response.json()
        cache_id   = result['cache_id'  ]
        cache_hash = result['cache_hash']

        assert is_guid(cache_id)    is True
        assert type(cache_hash)     is str
        assert result['size']       == len(test_binary)

        return cache_id, cache_hash

    def test__store__multiple_strategies(self):                                         # Test different storage strategies
        skip__if_not__in_github_actions()
        strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

        for strategy in strategies:
            response = self.client.post(url     = f'/{self.test_namespace}/{strategy}/store/string',
                                        content = f"test data for {strategy}"                      ,
                                        headers = {"Content-Type": "text/plain"}                   )

            assert response.status_code == 200
            result = response.json()
            assert 'cache_id'   in result
            assert 'cache_hash' in result
            assert 'paths'      in result

            paths = result['paths']                                                     # Verify appropriate paths
            assert 'data'       in paths
            assert 'by_hash'    in paths
            assert 'by_id'      in paths

    def test__store__compressed_data(self):                                             # Test compressed data via HTTP
        original_text   = "This will be compressed" * 100
        compressed_data = gzip.compress(original_text.encode())

        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/binary'     ,
                                    content = compressed_data                                     ,
                                    headers = {"Content-Type"     : "application/octet-stream"    ,
                                              "Content-Encoding" : "gzip"                        })

        assert response.status_code == 200
        result   = response.json()
        cache_id = result['cache_id']

        assert is_guid(cache_id) is True
        assert result['size']    < len(original_text)                                   # Compressed size

    def test__store__string_special_characters(self):                                   # Test special characters
        special_string = "Test with special: 你好世界 🚀 \n\t\r"

        response = self.client.post(url     = f'/{self.test_namespace}/direct/store/string'        ,
                                    content = special_string                                       ,
                                    headers = {"Content-Type": "text/plain; charset=utf-8"}       )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id'                  in result
        assert 'cache_hash'                in result
        assert is_guid(result['cache_id']) is True

    def test__store__json_with_nulls(self):                                             # Test JSON with null values
        json_with_nulls = {"key1"  : None               ,
                           "key2"  : "value"            ,
                           "key3"  : [1, None, 3]       ,
                           "key4"  : {"nested": None}   }

        response = self.client.post(url  = f'/{self.test_namespace}/temporal_latest/store/json',
                                    json = json_with_nulls                                      )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id'                  in result
        assert 'cache_hash'                in result
        assert is_guid(result['cache_id']) is True

    def test__store__large_json(self):                                                  # Test large JSON storage
        large_json = {f"key_{i}": {"data"  : f"value_{i}"               ,
                                   "nested": {"level": 2                ,
                                             "items": list(range(10))   }}
                     for i in range(100)}

        response = self.client.post(url  = f'/{self.test_namespace}/temporal_versioned/store/json',
                                    json = large_json                                              )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'size'     in result
        assert result['size'] > 1000                                                    # Should be reasonably large

    def test__store__empty_data(self):                                                  # Test storing empty data
        response_string = self.client.post(url     = f'/{self.test_namespace}/direct/store/string' ,
                                           content = ""                                            ,
                                           headers = {"Content-Type": "text/plain"}               )

        assert response_string.status_code == 400                                       # Empty string not supported
        assert response_string.json()      == {'detail': [{'input' : None      ,
                                                           'loc'   : ['body']   ,
                                                           'msg'   : 'Field required',
                                                           'type'  : 'missing'  }]}

        response_json = self.client.post(url  = f'/{self.test_namespace}/direct/store/json',
                                         json = {}                                          )

        assert response_json.status_code == 200                                         # Empty JSON is valid
        result_json = response_json.json()
        assert 'cache_id' in result_json

        response_binary = self.client.post(url     = f'/{self.test_namespace}/direct/store/binary'        ,
                                           content = b''                                                   ,
                                           headers = {"Content-Type": "application/octet-stream"}         )

        assert response_binary.status_code == 400                                       # Empty binary not supported
        assert response_binary.json()      == {'detail': [{'input' : None      ,
                                                           'loc'   : ['body']   ,
                                                           'msg'   : 'Field required',
                                                           'type'  : 'missing'  }]}

    def test__store__same_data_different_namespaces(self):                              # Test namespace isolation
        skip__if_not__in_github_actions()
        test_data = "namespace isolation test"
        ns1       = "test-store-ns1"
        ns2       = "test-store-ns2"

        response1 = self.client.post(url     = f'/{ns1}/direct/store/string'       ,
                                     content = test_data                           ,
                                     headers = {"Content-Type": "text/plain"}     )

        response2 = self.client.post(url     = f'/{ns2}/direct/store/string'       ,
                                     content = test_data                           ,
                                     headers = {"Content-Type": "text/plain"}     )

        assert response1.status_code == 200
        assert response2.status_code == 200

        result1 = response1.json()
        result2 = response2.json()

        assert result1['cache_id'  ] != result2['cache_id']                               # Different IDs
        assert result1['cache_hash'] == result2['cache_hash']                                   # Same hash

    def test__store__workflow_complete(self):                                           # Test complete workflow
        skip__if_not__in_github_actions()
        namespace = "store-workflow-test"
        test_data = {"workflow": "test", "step": 1}

        store_response = self.client.post(url  = f'/{namespace}/temporal/store/json',
                                          json = test_data                           )

        assert store_response.status_code == 200
        store_result = store_response.json()
        cache_id     = store_result['cache_id']
        cache_hash   = store_result['cache_hash']

        assert is_guid(cache_id) is True
        assert len(cache_hash)   == 16
        assert 'paths'           in store_result
        assert 'size'            in store_result

        string_response = self.client.post(url     = f'/{namespace}/direct/store/string'       ,
                                           content = "test string"                            ,
                                           headers = {"Content-Type": "text/plain"}          )

        assert string_response.status_code == 200
        string_result = string_response.json()
        assert is_guid(string_result['cache_id']) is True

        binary_response = self.client.post(url     = f'/{namespace}/temporal_latest/store/binary'         ,
                                           content = b'test binary data'                                  ,
                                           headers = {"Content-Type": "application/octet-stream"}        )

        assert binary_response.status_code == 200
        binary_result = binary_response.json()
        assert is_guid(binary_result['cache_id']) is True

    def test__store__duplicate_data(self):                                              # Test storing duplicate data
        test_string = "duplicate test data"

        response1 = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string'    ,
                                     content = test_string                                        ,
                                     headers = {"Content-Type": "text/plain"}                    )

        response2 = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string'    ,
                                     content = test_string                                        ,
                                     headers = {"Content-Type": "text/plain"}                    )

        assert response1.status_code == 200
        assert response2.status_code == 200

        result1 = response1.json()
        result2 = response2.json()

        assert result1['cache_hash'] == result2['cache_hash']                                   # Same hash
        assert result1['cache_id'  ] != result2['cache_id']                               # Different IDs

    def test__store__default_namespace(self):                                           # Test default namespace
        response = self.client.post(url     = '/default/direct/store/string'       ,
                                    content = "default namespace test"            ,
                                    headers = {"Content-Type": "text/plain"}      )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id'                  in result
        assert 'cache_hash'                in result
        assert is_guid(result['cache_id']) is True

    def test__store__large_binary_file(self):                                           # Test large binary files
        large_binary = bytes([i % 256 for i in range(1024 * 1024)])                     # 1MB binary

        response = self.client.post(url     = f'/{self.test_namespace}/direct/store/binary'       ,
                                    content = large_binary                                        ,
                                    headers = {"Content-Type": "application/octet-stream"}       )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'size'     in result
        assert result['size'] == len(large_binary)

    def test__store__json_as_string(self):                                              # Test JSON-like string
        json_string = '{"valid": "json", "as": "string"}'

        response = self.client.post(url     = f'/{self.test_namespace}/direct/store/string'    ,
                                    content = json_string                                      ,
                                    headers = {"Content-Type": "text/plain"}                  )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id'                  in result
        assert 'cache_hash'                in result
        assert is_guid(result['cache_id']) is True

    def test__store__binary_that_looks_like_text(self):                                 # Test text-like binary
        text_like_binary = b"This looks like text but is stored as binary"

        response = self.client.post(url     = f'/{self.test_namespace}/temporal_versioned/store/binary'   ,
                                    content = text_like_binary                                            ,
                                    headers = {"Content-Type": "application/octet-stream"}               )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id' in result
        assert 'size'     in result
        assert result['size'] == len(text_like_binary)