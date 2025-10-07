from unittest                                                                               import TestCase
import pytest
from osbot_fast_api_serverless.utils.testing.skip_tests                                     import skip__if_not__in_github_actions
from osbot_utils.testing.__ import __
from osbot_utils.utils.Misc                                                                 import is_guid
from osbot_utils.utils.Objects import obj

from tests.unit.Service__Cache__Test_Objs                                                   import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Data__Store__client(TestCase):                                               # Test data store routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        cls.test_objs      = setup__service__cache__test_objs()
        cls.cache_fixtures = cls.test_objs.cache_fixtures
        cls.client         = cls.test_objs.fast_api__client
        cls.app            = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        cls.test_namespace = "test-data-store-client"                                           # Test namespace
        cls.parent_cache_id = cls._create_parent_cache_entry()                                  # Create parent entry once

    @classmethod
    def _create_parent_cache_entry(cls):                                                        # Helper to create parent cache entry
        response = cls.client.post(url     = f'/{cls.test_namespace}/temporal/store/string',
                                   content = "parent entry for data storage"               ,
                                   headers = {"Content-Type": "text/plain"}                )
        assert response.status_code == 200
        return response.json()['cache_id']

    def test__health_check(self):                                                               # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__data_store__string(self):                                                         # Test string storage without file_id
        test_string = "test string content for client"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string',
                                   content = test_string                                                                   ,
                                   headers = {"Content-Type": "text/plain"}                                               )

        assert response.status_code == 200
        result = response.json()

        assert 'cache_id'           in result
        assert 'data_files_created' in result
        assert 'data_key'           in result
        assert 'data_type'          in result
        assert 'extension'          in result
        assert 'file_id'            in result
        assert 'file_size'          in result
        assert 'namespace'          in result
        assert 'timestamp'          in result

        assert result['cache_id']  == self.parent_cache_id
        assert result['data_type'] == 'string'
        assert result['data_key']  == ''                                                        # No key provided
        assert result['extension'] == 'txt'
        assert result['file_size'] == len(test_string.encode('utf-8'))
        assert result['namespace'] == self.test_namespace
        assert is_guid(result['file_id']) is True                                               # Auto-generated ID
        assert len(result['data_files_created']) == 1

    def test__data_store__string_with_id(self):                                                 # Test string storage with specific file_id
        test_string = "test string with specific id"
        file_id     = "client-string-001"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/{file_id}',
                                   content = test_string                                                                               ,
                                   headers = {"Content-Type": "text/plain"}                                                           )

        assert response.status_code == 200
        result = response.json()

        assert result['cache_id']  == self.parent_cache_id
        assert result['data_type'] == 'string'
        assert result['file_id']   == file_id                                                   # Specific ID used
        assert result['file_size'] == len(test_string.encode('utf-8'))

    def test__data_store__string_with_id_and_key(self):                                         # Test string storage with ID and key
        test_string = "configuration data content"
        file_id     = "config-v1"
        data_key    = "configs/app"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/{data_key}/{file_id}',
                                   content = test_string                                                                                           ,
                                   headers = {"Content-Type": "text/plain"}                                                                       )

        assert response.status_code == 200
        result = response.json()

        assert result['cache_id']  == self.parent_cache_id
        assert result['data_type'] == 'string'
        assert result['file_id']   == file_id
        assert result['data_key']  == data_key                                                  # Hierarchical key preserved
        assert result['file_size'] == len(test_string.encode('utf-8'))

    def test__data_store__json(self):                                                           # Test JSON storage without file_id
        test_json = {"status": "active", "count": 42, "tags": ["test", "client"]}

        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json',
                                   json = test_json                                                                       )

        assert response.status_code == 200
        result = response.json()

        assert result['cache_id']  == self.parent_cache_id
        assert result['data_type'] == 'json'
        assert result['extension'] == 'json'
        assert is_guid(result['file_id']) is True                                               # Auto-generated

    def test__data_store__json_with_id(self):                                                   # Test JSON storage with specific file_id
        test_json = {"config": "value", "level": 1}
        file_id   = "settings-json"

        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json/{file_id}',
                                   json = test_json                                                                                )

        assert response.status_code == 200
        result = response.json()

        assert result['file_id']   == file_id
        assert result['data_type'] == 'json'

    def test__data_store__json_with_id_and_key(self):                                           # Test JSON with full path
        test_json = {"nested": "data", "items": [1, 2, 3]}
        file_id   = "app-config"
        data_key  = "settings/production"

        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json/{data_key}/{file_id}',
                                   json = test_json                                                                                            )

        assert response.status_code == 200
        result = response.json()

        assert result['file_id']  == file_id
        assert result['data_key'] == data_key
        assert result['data_type'] == 'json'

    def test__data_store__binary(self):                                                         # Test binary storage without file_id
        test_binary = b'\x89PNG\r\n\x1a\n' + b'\x00' * 10

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/binary',
                                   content = test_binary                                                                      ,
                                   headers = {"Content-Type": "application/octet-stream"}                                    )

        assert response.status_code == 200
        result = response.json()

        assert result['cache_id']  == self.parent_cache_id
        assert result['data_type'] == 'binary'
        assert result['extension'] == 'bin'
        assert result['file_size'] == len(test_binary)
        assert is_guid(result['file_id']) is True

    def test__data_store__binary_with_id(self):                                                 # Test binary storage with specific file_id
        test_binary = b'binary content with \x00\x01\x02'
        file_id     = "binary-file-001"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/binary/{file_id}',
                                   content = test_binary                                                                               ,
                                   headers = {"Content-Type": "application/octet-stream"}                                             )

        assert response.status_code == 200
        result = response.json()

        assert result['file_id']   == file_id
        assert result['data_type'] == 'binary'

    def test__data_store__binary_with_id_and_key(self):                                         # Test binary with full path
        test_binary = b'PNG image data simulation'
        file_id     = "thumbnail"
        data_key    = "images/thumbnails"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/binary/{data_key}/{file_id}',
                                   content = test_binary                                                                                           ,
                                   headers = {"Content-Type": "application/octet-stream"}                                                         )

        assert response.status_code == 200
        result = response.json()

        assert result['file_id']  == file_id
        assert result['data_key'] == data_key
        assert result['data_type'] == 'binary'

    def test__data_store__parent_not_found(self):                                               # Test error when parent doesn't exist
        non_existent_id = "00000000-0000-0000-0000-000000000000"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{non_existent_id}/data/store/string',
                                    content = "test"                                                                       ,
                                    headers = {"Content-Type": "text/plain"}                                              )

        error = response.json()
        assert error['detail']['error_type'] == 'NOT_FOUND'
        assert non_existent_id in error['detail']['message']
        assert error['detail'] == {'cache_id'  : '00000000-0000-0000-0000-000000000000',
                                   'error_type': 'NOT_FOUND',
                                   'message'   : "Cache entry '00000000-0000-0000-0000-000000000000' in namespace "
                                                 "'test-data-store-client' not found"}


    def test__data_store__multiple_files_same_parent(self):                                     # Test storing multiple data files
        skip__if_not__in_github_actions()
        parent_id = self._create_parent_cache_entry()                                           # Create fresh parent

        # Store multiple files under same parent
        files_created = []

        for i in range(3):
            response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_id}/data/store/string/file-{i}',
                                       content = f"content {i}"                                                                 ,
                                       headers = {"Content-Type": "text/plain"}                                                 )
            assert response.status_code == 200
            files_created.append(response.json()['file_id'])

        assert len(files_created)      == 3
        assert len(set(files_created)) == 3                                                     # All unique
        assert files_created           == ['file-0', 'file-1', 'file-2']

    def test__data_store__special_characters(self):                                             # Test handling of special characters
        special_string = "Data with 中文 and émojis 🚀 and newlines\n\ttabs"

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string',
                                   content = special_string                                                                   ,
                                   headers = {"Content-Type": "text/plain; charset=utf-8"}                                   )

        assert response.status_code == 200
        result = response.json()
        assert result['file_size'] == len(special_string.encode('utf-8'))                       # UTF-8 encoding size

    def test__data_store__empty_json(self):                                                     # Test empty JSON storage
        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json',
                                   json = {}                                                                              )

        assert response.status_code == 200
        result = response.json()
        assert result['data_type'] == 'json'
        assert result['file_size'] == 2                                                         # "{}" is 2 bytes

    def test__data_store__large_json(self):                                                     # Test large JSON storage
        large_json = {f"key_{i}": {"value": i, "data": list(range(10))} for i in range(50)}

        response = self.client.post(url  = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/json',
                                   json = large_json                                                                     )

        assert response.status_code == 200
        result = response.json()
        assert result['file_size'] > 1000                                                       # Large file

    def test__data_store__hierarchical_keys(self):                                              # Test deep hierarchical keys
        skip__if_not__in_github_actions()
        test_keys = [ "level1/level2/level3"         ,
                     "users/profiles/settings"      ,
                     "data/2024/09/25"              ]

        for data_key in test_keys:
            response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/{data_key}/test-file',
                                       content = f"data at {data_key}"                                                                                  ,
                                       headers = {"Content-Type": "text/plain"}                                                                         )

            assert response.status_code == 200
            assert response.json()['data_key'] == data_key

    def test__data_store__concurrent_requests(self):                                            # Test concurrent storage operations
        skip__if_not__in_github_actions()
        import concurrent.futures

        def store_data(index):
            response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string/concurrent-{index}',
                                       content = f"concurrent data {index}"                                                                          ,
                                       headers = {"Content-Type": "text/plain"}                                                                      )
            return response.status_code == 200

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(store_data, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        assert all(results)                                                                     # All successful

    def test__data_store__auth_required(self):                                                  # Test authentication requirement
        auth_header = self.client.headers.pop(TEST_API_KEY__NAME)                               # Remove auth temporarily

        response = self.client.post(url     = f'/{self.test_namespace}/cache/{self.parent_cache_id}/data/store/string',
                                   content = "test"                                                                            ,
                                   headers = {"Content-Type": "text/plain"}                                                   )

        assert response.status_code == 401

        self.client.headers[TEST_API_KEY__NAME] = auth_header                                   # Restore auth

    def test__regression__data_store__handling_of__invalid_parent_id_format(self):                                       # Test invalid parent ID format
        invalid_id = "not-a-guid"
        #error_message = "in Random_Guid: value provided was not a Guid: not-a-guid"
        # with pytest.raises(ValueError, match=error_message):
        #     response = self.client.post(url     = f'/{self.test_namespace}/cache/{invalid_id}/data/store/string',
        #                                 content = "test"                                                                  ,
        #                                headers = {"Content-Type": "text/plain"}                       # BUG
        #                                )
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{invalid_id}/data/store/string',
                                    content = "test"                                                                  ,
                                    headers = {"Content-Type": "text/plain"}                                         )

        assert response.status_code == 400                                                      # Validation error
        error = response.json()
        assert 'detail' in error
        assert any('cache_id' in str(detail) for detail in error['detail'])
        assert obj(error) == __(detail=[__(type  = 'value_error'                                              ,
                                           loc   = ['query', 'cache_id']                                      ,
                                           msg   = 'in Random_Guid: value provided was not a Guid: not-a-guid',
                                           input = 'not-a-guid'                                              )])