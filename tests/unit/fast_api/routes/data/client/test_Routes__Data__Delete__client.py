from unittest                                                                               import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                                     import skip__if_not__in_github_actions
from tests.unit.Service__Cache__Test_Objs                                                   import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Data__Delete__client(TestCase):                                              # Test data delete routes via FastAPI TestClient

    @classmethod
    def setUpClass(cls):
        cls.test_objs      = setup__service__cache__test_objs()
        cls.cache_fixtures = cls.test_objs.cache_fixtures
        cls.client         = cls.test_objs.fast_api__client
        cls.app            = cls.test_objs.fast_api__app

        cls.client.headers[TEST_API_KEY__NAME] = TEST_API_KEY__VALUE

        cls.test_namespace = "test-data-delete-client"                                          # Test namespace
        cls.test_data_key  = "configs/app"                                                      # Hierarchical path

    def _create_parent_with_data(self):                                                         # Helper to create parent and store test data
        # Create parent cache entry
        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string',
                                   content = "parent for deletion tests"                              ,
                                   headers = {"Content-Type": "text/plain"}                           )
        assert response.status_code == 200
        parent_cache_id = response.json()['cache_id']

        # Store test data files
        data_files = {}

        # String data with ID only
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_cache_id}/data/store/string/string-001',
                                   content = "test string to delete"                                                                ,
                                   headers = {"Content-Type": "text/plain"}                                                         )
        assert response.status_code == 200
        data_files['string_1'] = response.json()['file_id']

        # String data with ID and key
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_cache_id}/data/store/string/{self.test_data_key}/string-002',
                                   content = "nested string to delete"                                                                                       ,
                                   headers = {"Content-Type": "text/plain"}                                                                                 )
        assert response.status_code == 200
        data_files['string_2'] = response.json()['file_id']

        # JSON data
        response = self.client.post(url  = f'/{self.test_namespace}/cache/{parent_cache_id}/data/store/json/json-001',
                                   json = {"delete": "me", "value": 123}                                                     )
        assert response.status_code == 200
        data_files['json_1'] = response.json()['file_id']

        # JSON with key
        response = self.client.post(url  = f'/{self.test_namespace}/cache/{parent_cache_id}/data/store/json/{self.test_data_key}/json-002',
                                   json = {"nested": "json"}                                                                                       )
        assert response.status_code == 200
        data_files['json_2'] = response.json()['file_id']

        # Binary data
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_cache_id}/data/store/binary/binary-001',
                                   content = b'binary to delete'                                                                   ,
                                   headers = {"Content-Type": "application/octet-stream"}                                          )
        assert response.status_code == 200
        data_files['binary_1'] = response.json()['file_id']

        return parent_cache_id, data_files

    def test__health_check(self):                                                               # Verify API is accessible
        response = self.client.get('/info/health')
        assert response.status_code == 200
        assert response.json()       == {'status': 'ok'}

    def test__delete_data_file__string_with_id(self):                                           # Test deleting specific string file
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()
        file_id = data_files['string_1']

        # Verify file exists by retrieving it
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/string/{file_id}')
        assert response.status_code == 200

        # Delete the file
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{file_id}')

        assert response.status_code == 200
        result = response.json()
        assert result == { 'status'       : 'success'                           ,
                           'message'      : 'Data file deleted successfully'    ,
                           'cache_id'     : parent_id                           ,
                           'data_file_id' : file_id                             ,
                           'data_type'    : 'string'                            ,
                           'data_key'     : None                                ,
                           'namespace'    : self.test_namespace                 }

        # Verify file no longer exists
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/string/{file_id}')
        assert response.status_code == 404

    def test__delete_data_file__string_with_id_and_key(self):                                   # Test deleting string file with key
        parent_id, data_files = self._create_parent_with_data()
        file_id = data_files['string_2']

        # Delete the file with key path
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{self.test_data_key}/{file_id}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']       == 'success'
        assert result['data_file_id'] == file_id
        assert result['data_key']     == self.test_data_key
        assert result['data_type']    == 'string'

        # Verify deletion
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/string/{self.test_data_key}/{file_id}')
        assert response.status_code == 404

    def test__delete_data_file__json_with_id(self):                                             # Test deleting JSON file
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()
        file_id = data_files['json_1']

        # Delete the JSON file
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/json/{file_id}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']       == 'success'
        assert result['data_type']    == 'json'
        assert result['data_file_id'] == file_id

        # Verify deletion
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/json/{file_id}')
        assert response.status_code == 404

    def test__delete_data_file__binary_with_id(self):                                           # Test deleting binary file
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()
        file_id = data_files['binary_1']

        # Delete the binary file
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/binary/{file_id}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']       == 'success'
        assert result['data_type']    == 'binary'
        assert result['data_file_id'] == file_id

        # Verify deletion
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/binary/{file_id}')
        assert response.status_code == 404

    def test__delete_data_file__not_found(self):                                                # Test deleting non-existent file
        parent_id, _ = self._create_parent_with_data()

        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/non-existent-file')

        assert response.status_code == 404
        error = response.json()
        assert error['detail']['error_type']   == 'NOT_FOUND'
        assert error['detail']['message']      == 'Data file non-existent-file not found'
        assert error['detail']['data_file_id'] == 'non-existent-file'
        assert error['detail']['data_type']    == 'string'

    def test__delete_data_file__missing_data_file_id(self):                                     # Test validation error for missing file_id
        parent_id, _ = self._create_parent_with_data()

        # Try to delete without file_id (malformed URL)
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/')

        assert response.status_code == 404                                                      # FastAPI returns 404 for incomplete path

    def test__delete_all_data_files(self):                                                      # Test deleting all data files
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()

        # Verify files exist
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/string/{data_files["string_1"]}')
        assert response.status_code == 200

        # Delete all data files
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/all')

        assert response.status_code == 200
        result = response.json()
        assert result['status']        == 'success'
        assert result['deleted_count'] >= 5                                                     # At least our 5 test files
        assert result['cache_id']      == parent_id
        assert 'deleted_files' in result

        # Verify all files are deleted
        for file_type, file_id in [('string', data_files['string_1']),
                                   ('json', data_files['json_1']),
                                   ('binary', data_files['binary_1'])]:
            response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/{file_type}/{file_id}')
            assert response.status_code == 404

    def test__delete_all_data_files__with_key(self):                                            # Test deleting all files under specific key
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()

        # Add more files under the same key
        for i in range(3):
            response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_id}/data/store/string/{self.test_data_key}/extra-{i}',
                                        content = f"extra file {i}"                                                                                    ,
                                        headers = {"Content-Type": "text/plain"}                                                                       )
            assert response.status_code == 200

        # Delete all files under the key
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/all/{self.test_data_key}')

        assert response.status_code == 200
        result = response.json()
        assert result['status']        == 'success'
        assert result['deleted_count'] == 5                                                     # string-002, json-002, and 3 extras
        assert result['data_key']      == self.test_data_key

        # Verify files under key are deleted
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/string/{self.test_data_key}/{data_files["string_2"]}')
        assert response.status_code == 404

        # Verify files NOT under key still exist
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/string/{data_files["string_1"]}')
        assert response.status_code == 200                                                      # This one should still exist

    def test__delete_all_data_files__empty_parent(self):                                        # Test deleting when no files exist
        skip__if_not__in_github_actions()
        # Create parent with no data files
        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string',
                                   content = "empty parent"                                          ,
                                   headers = {"Content-Type": "text/plain"}                          )
        assert response.status_code == 200
        parent_id = response.json()['cache_id']

        # Try to delete all (should be none)
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/all')

        assert response.status_code == 200
        result = response.json()
        assert result == { 'status'        : 'success'                 ,
                          'message'       : 'No data files to delete' ,
                          'cache_id'     : parent_id                  ,
                          'deleted_count' : 0                         ,
                          'data_key'     : None                       ,
                          'namespace'    : self.test_namespace        }

    def test__delete_parent_not_found(self):                                                    # Test error when parent doesn't exist
        non_existent_id = "00000000-0000-0000-0000-000000000000"

        # Try to delete file from non-existent parent
        response = self.client.delete(f'/{self.test_namespace}/cache/{non_existent_id}/data/delete/string/test-file')

        assert response.status_code == 404
        error = response.json()
        assert error['detail']['error_type'] == 'NOT_FOUND'

    def test__delete_invalid_data_type(self):                                                   # Test invalid data type
        parent_id, _ = self._create_parent_with_data()

        # Try with invalid data type
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/invalid_type/test-file')

        assert response.status_code == 400                                                      # Validation error from FastAPI
        error = response.json()
        assert 'detail' in error

    def test__delete_auth_required(self):                                                       # Test authentication requirement
        parent_id, data_files = self._create_parent_with_data()
        auth_header = self.client.headers.pop(TEST_API_KEY__NAME)                               # Remove auth temporarily

        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{data_files["string_1"]}')

        assert response.status_code == 401

        self.client.headers[TEST_API_KEY__NAME] = auth_header                                   # Restore auth

    def test__delete_idempotency(self):                                                         # Test deleting same file twice
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()
        file_id = data_files['string_1']

        # First deletion - should succeed
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{file_id}')
        assert response.status_code == 200

        # Second deletion - should return 404
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{file_id}')
        assert response.status_code == 404
        assert response.json()['detail']['error_type'] == 'NOT_FOUND'

    def test__delete_mixed_types(self):                                                         # Test deleting different types sequentially
        skip__if_not__in_github_actions()
        parent_id, data_files = self._create_parent_with_data()

        # Delete string
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{data_files["string_1"]}')
        assert response.status_code == 200
        assert response.json()['data_type'] == 'string'

        # Delete JSON
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/json/{data_files["json_1"]}')
        assert response.status_code == 200
        assert response.json()['data_type'] == 'json'

        # Delete binary
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/binary/{data_files["binary_1"]}')
        assert response.status_code == 200
        assert response.json()['data_type'] == 'binary'

    def test__delete_hierarchical_keys(self):                                                   # Test deletion with deep hierarchical keys
        skip__if_not__in_github_actions()
        parent_id, _ = self._create_parent_with_data()
        deep_key = "level1/level2/level3/level4"

        # Store file with deep hierarchy
        response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_id}/data/store/string/{deep_key}/deep-file',
                                    content = "deep hierarchical data"                                                                    ,
                                    headers = {"Content-Type": "text/plain"}                                                              )
        assert response.status_code == 200

        # Delete it
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{deep_key}/deep-file')

        assert response.status_code == 200
        result = response.json()
        assert result['data_key']     == deep_key
        assert result['data_file_id'] == 'deep-file'
        assert result                 == { 'cache_id'    : parent_id,
                                           'data_file_id': 'deep-file',
                                           'data_key'    : 'level1/level2/level3/level4',
                                           'data_type'   : 'string',
                                           'message'     : 'Data file deleted successfully',
                                           'namespace'   : 'test-data-delete-client',
                                           'status'      : 'success'}

    def test__delete_concurrent_requests(self):                                                 # Test concurrent delete operations
        skip__if_not__in_github_actions()
        import concurrent.futures

        parent_id, _ = self._create_parent_with_data()

        # Create multiple files to delete
        file_ids = []
        for i in range(10):
            response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_id}/data/store/string/concurrent-{i}',
                                       content = f"concurrent file {i}"                                                               ,
                                       headers = {"Content-Type": "text/plain"}                                                       )
            assert response.status_code == 200
            file_ids.append(f'concurrent-{i}')

        def delete_file(file_id):
            response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/{file_id}')
            return response.status_code == 200

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(delete_file, fid) for fid in file_ids]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        assert all(results)                                                                     # All deletions successful

    def test__delete_complete_workflow(self):                                                   # Test complete create-retrieve-delete workflow
        # Create parent
        response = self.client.post(url     = f'/{self.test_namespace}/temporal/store/string',
                                   content = "workflow parent"                                       ,
                                   headers = {"Content-Type": "text/plain"}                          )
        assert response.status_code == 200
        parent_id = response.json()['cache_id']

        # Store data
        test_data = {"workflow": "test", "step": 1}
        response = self.client.post(url  = f'/{self.test_namespace}/cache/{parent_id}/data/store/json/workflow-data',
                                   json = test_data                                                                          )
        assert response.status_code == 200

        # Retrieve to verify it exists
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/json/workflow-data')
        assert response.status_code == 200
        assert response.json() == test_data

        # Delete it
        response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/json/workflow-data')
        assert response.status_code == 200
        assert response.json()['status'] == 'success'

        # Verify it's gone
        response = self.client.get(f'/{self.test_namespace}/cache/{parent_id}/data/json/workflow-data')
        assert response.status_code == 404

    def test__delete_performance(self):                                                         # Test deletion performance
        skip__if_not__in_github_actions()
        import time

        parent_id, _ = self._create_parent_with_data()

        # Create files to delete
        for i in range(10):
            response = self.client.post(url     = f'/{self.test_namespace}/cache/{parent_id}/data/store/string/perf-{i}',
                                       content = f"perf test {i}"                                                              ,
                                       headers = {"Content-Type": "text/plain"}                                                )
            assert response.status_code == 200

        # Time the deletions
        start_time = time.time()

        for i in range(10):
            response = self.client.delete(f'/{self.test_namespace}/cache/{parent_id}/data/delete/string/perf-{i}')
            assert response.status_code == 200

        elapsed = time.time() - start_time
        assert elapsed < 2.0                                                                    # Should be fast (< 2 seconds for 10 deletions)