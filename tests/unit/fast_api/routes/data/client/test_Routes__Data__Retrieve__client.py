from unittest                                                                               import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                                     import skip__if_not__in_github_actions
from tests.unit.Service__Cache__Test_Objs                                                   import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE

class test_Routes__Data__Retrieve__client(TestCase):                                            # Test data retrieve routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        cls.test_objs      = setup__service__cache__test_objs()
        cls.cache_fixtures = cls.test_objs.cache_fixtures
        cls.client         = cls.test_objs.fast_api__client
        cls.app            = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        cls.test_namespace  = "test-data-retrieve-client"                                       # Test namespace
        cls.test_data_key   = "configs/app"                                                     # Hierarchical path
        cls.parent_cache_id = cls._create_parent_cache_entry()                                  # Create parent entry once
        cls._setup_test_data()                                                                  # Store test data for retrieval

    @classmethod
    def _create_parent_cache_entry(cls):                                                        # Helper to create parent cache entry
        response = cls.client.post(url     = f'/{cls.test_namespace}/temporal/store/string',
                                   content = "parent for retrieval tests"                         ,
                                   headers = {"Content-Type": "text/plain"}                       )
        assert response.status_code == 200
        return response.json()['cache_id']

    @classmethod
    def _setup_test_data(cls):                                                                  # Setup test data files
        # Store string data with ID only
        response = cls.client.post(url     = f'/{cls.test_namespace}/cache/{cls.parent_cache_id}/data/store/string/string-001',
                                   content = "test string content"                                                                     ,
                                   headers = {"Content-Type": "text/plain"}                                                           )
        assert response.status_code == 200
        cls.string_id_1 = response.json()['file_id']

        # Store string data with ID and key
        response = cls.client.post(url     = f'/{cls.test_namespace}/cache/{cls.parent_cache_id}/data/store/string/{cls.test_data_key}/string-002',
                                   content = "nested string content"                                                                                         ,
                                   headers = {"Content-Type": "text/plain"}                                                                                 )
        assert response.status_code == 200
        cls.string_id_2 = response.json()['file_id']

        # Store JSON data with ID only
        response = cls.client.post(url  = f'/{cls.test_namespace}/cache/{cls.parent_cache_id}/data/store/json/json-001',
                                   json = {"status": "active", "count": 42}                                                    )
        assert response.status_code == 200
        cls.json_id_1 = response.json()['file_id']

        # Store JSON data with ID and key
        response = cls.client.post(url  = f'/{cls.test_namespace}/cache/{cls.parent_cache_id}/data/store/json/{cls.test_data_key}/json-002',
                                   json = {"nested": "data", "items": [1, 2, 3]}                                                                       )
        assert response.status_code == 200
        cls.json_id_2 = response.json()['file_id']

        # Store binary data with ID only
        response = cls.client.post(url     = f'/{cls.test_namespace}/cache/{cls.parent_cache_id}/data/store/binary/binary-001',
                                   content = b'\x89PNG\r\n\x1a\n' + b'\x00' * 10                                                     ,
                                   headers = {"Content-Type": "application/octet-stream"}                                            )
        assert response.status_code == 200
        cls.binary_id_1 = response.json()['file_id']

        # Store binary data with ID and key
        response = cls.client.post(url     = f'/{cls.test_namespace}/cache/{cls.parent_cache_id}/data/store/binary/{cls.test_data_key}/binary-002',
                                   content = b'binary content with \x00\x01\x02'                                                                           ,
                                   headers = {"Content-Type": "application/octet-stream"}                                                                  )
        assert response.status_code == 200
        cls.binary_id_2 = response.json()['file_id']

    def test__health_check(self):                                                               # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__data_retrieve__json_with_id(self):                                                # Test JSON retrieval with ID only
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{self.json_id_1}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"status": "active", "count": 42}

    def test__data_retrieve__json_with_id_and_key(self):                                        # Test JSON retrieval with ID and key
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{self.test_data_key}/{self.json_id_2}')

        assert response.status_code == 200
        result = response.json()
        assert result == {"nested": "data", "items": [1, 2, 3]}

    def test__data_retrieve__json_not_found(self):                                              # Test JSON 404 error
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/missing-json')

        assert response.status_code == 404
        error = response.json()
        assert error['detail']['error_type']   == 'NOT_FOUND'
        assert error['detail']['message']      == 'Data file not found'
        assert error['detail']['data_file_id'] == 'missing-json'

    def test__data_retrieve__string_with_id(self):                                              # Test string retrieval with ID only
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{self.string_id_1}')

        assert response.status_code == 200
        assert response.text        == "test string content"
        assert response.headers['content-type'] == 'text/plain; charset=utf-8'

    def test__data_retrieve__string_with_id_and_key(self):                                      # Test string retrieval with ID and key
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{self.test_data_key}/{self.string_id_2}')

        assert response.status_code == 200
        assert response.text        == "nested string content"

    def test__data_retrieve__string_not_found(self):                                            # Test string 404 response
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/missing-string')

        assert response.status_code == 404
        assert response.text        == ""

    def test__data_retrieve__binary_with_id(self):                                              # Test binary retrieval with ID only
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/{self.binary_id_1}')

        assert response.status_code == 200
        assert response.content     == b'\x89PNG\r\n\x1a\n' + b'\x00' * 10
        assert response.headers['content-type'] == 'application/octet-stream'

    def test__data_retrieve__binary_with_id_and_key(self):                                      # Test binary retrieval with ID and key
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/{self.test_data_key}/{self.binary_id_2}')

        assert response.status_code == 200
        assert response.content     == b'binary content with \x00\x01\x02'

    def test__data_retrieve__binary_not_found(self):                                            # Test binary 404 response
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/missing-binary')

        assert response.status_code == 404
        assert response.content     == b""

    def test__data_retrieve__wrong_type(self):                                                  # Test retrieving data as wrong types
        # Try to get JSON as string
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{self.json_id_1}')
        assert response.status_code == 404
        assert response.text        == ""

        # Try to get binary as JSON
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{self.binary_id_1}')
        assert response.status_code == 404
        assert response.json()['detail']['error_type'] == 'NOT_FOUND'

        # Try to get string as binary
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/{self.string_id_1}')
        assert response.status_code == 404
        assert response.content     == b""

    def test__data_retrieve__parent_not_found(self):                                            # Test with non-existent parent
        non_existent_id = "00000000-0000-0000-0000-000000000000"

        response = self.client.get(f'/{self.test_namespace}/cache/{non_existent_id}/data/json/any-id')

        assert response.status_code == 404
        error = response.json()
        assert error['detail']['error_type'] == 'NOT_FOUND'

    def test__data_retrieve__special_characters(self):                                          # Test handling special characters
        # Store data with special characters
        special_data = "Data with 中文 and émojis 🚀"
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/special-chars',
                                   content = special_data                                                                                  ,
                                   headers = {"Content-Type": "text/plain; charset=utf-8"}                                                )
        assert response.status_code == 200

        # Retrieve it back
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/special-chars')

        assert response.status_code == 200
        assert response.text        == special_data

    def test__data_retrieve__empty_json(self):                                                  # Test retrieving empty JSON
        # Store empty JSON
        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json/empty-json',
                                   json = {}                                                                                         )
        assert response.status_code == 200

        # Retrieve it
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/empty-json')

        assert response.status_code == 200
        assert response.json()       == {}

    def test__data_retrieve__large_json(self):                                                  # Test retrieving large JSON
        # Store large JSON
        large_json = {f"key_{i}": {"value": i, "data": list(range(10))} for i in range(50)}
        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json/large-json',
                                   json = large_json                                                                                 )
        assert response.status_code == 200

        # Retrieve it
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/large-json')

        assert response.status_code == 200
        assert response.json()       == large_json

    def test__data_retrieve__hierarchical_keys(self):                                           # Test deep hierarchical keys
        skip__if_not__in_github_actions()
        # Store data with deep hierarchy
        test_key  = "level1/level2/level3/level4"
        test_data = "deep hierarchical data"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/{test_key}/deep-file',
                                   content = test_data                                                                                            ,
                                   headers = {"Content-Type": "text/plain"}                                                                      )
        assert response.status_code == 200

        # Retrieve it
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{test_key}/deep-file')

        assert response.status_code == 200
        assert response.text        == test_data

    def test__data_retrieve__auth_required(self):                                               # Test authentication requirement
        auth_header = self.client.headers.pop(TEST_API_KEY__NAME)                               # Remove auth temporarily

        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{self.json_id_1}')

        assert response.status_code == 401

        self.client.headers[TEST_API_KEY__NAME] = auth_header                                   # Restore auth

    def test__data_retrieve__invalid_parent_id_format(self):                                    # Test invalid parent ID format
        invalid_id = "not-a-guid"

        response = self.client.get(f'/{self.test_namespace}/cache/{invalid_id}/data/json/test-id')

        assert response.status_code == 400                                                      # Validation error
        error = response.json()
        assert 'detail' in error
        assert any('cache_id' in str(detail) for detail in error['detail'])

    def test__data_retrieve__concurrent_requests(self):                                         # Test concurrent retrieval operations
        skip__if_not__in_github_actions()
        import concurrent.futures

        def retrieve_data(data_type, file_id):
            if data_type == 'json':
                response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{file_id}')
            elif data_type == 'string':
                response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{file_id}')
            else:  # binary
                response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/{file_id}')
            return response.status_code == 200


        # Mix of different data types
        requests = [ ('json'  , self.json_id_1  ),
                     ('string', self.string_id_1),
                     ('binary', self.binary_id_1),
                     ('json'  , self.json_id_1  ),          # we can't use json_id_2 here because it needs a data_key
                     ('string', self.string_id_1),
                     ('binary', self.binary_id_1)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(retrieve_data, dtype, fid) for dtype, fid in requests]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        assert all(results)                                                                     # All successful

    def test__data_retrieve__store_and_retrieve_cycle(self):                                    # Full integration test
        test_string = "integration test string"
        test_json   = {"integration": "test", "values": [1, 2, 3]}
        test_binary = b"integration\x00test\x01binary"
        test_key    = "integration/test"

        # Store them
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/{test_key}/integ-str',
                                   content = test_string                                                                                        ,
                                   headers = {"Content-Type": "text/plain"}                                                                    )
        assert response.status_code == 200

        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json/{test_key}/integ-json',
                                   json = test_json                                                                                            )
        assert response.status_code == 200

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/binary/{test_key}/integ-bin',
                                   content = test_binary                                                                                           ,
                                   headers = {"Content-Type": "application/octet-stream"}                                                         )
        assert response.status_code == 200

        # Retrieve them back
        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{test_key}/integ-str')
        assert response.status_code == 200
        assert response.text        == test_string

        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{test_key}/integ-json')
        assert response.status_code == 200
        assert response.json()       == test_json

        response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/{test_key}/integ-bin')
        assert response.status_code == 200
        assert response.content     == test_binary

    def test__data_retrieve__performance(self):                                                 # Test multiple rapid requests
        skip__if_not__in_github_actions()
        import time

        # Rapid retrieval of different data types
        start_time = time.time()

        for _ in range(5):
            response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/json/{self.json_id_1}')
            assert response.status_code == 200

            response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/string/{self.string_id_1}')
            assert response.status_code == 200

            response = self.client.get(f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/binary/{self.binary_id_1}')
            assert response.status_code == 200

        elapsed = time.time() - start_time
        assert elapsed < 2.0                                                                    # Should be fast (< 2 seconds for 15 requests)