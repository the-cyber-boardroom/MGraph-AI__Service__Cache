import base64
import gzip
import requests
import time
import concurrent.futures
from unittest                                                                   import TestCase
from typing                                                                     import Dict, Any
from memory_fs.path_handlers.Path__Handler__Temporal                            import Path__Handler__Temporal
from osbot_fast_api.utils.Fast_API_Server                                       import Fast_API_Server
from osbot_fast_api_serverless.utils.testing.skip_tests                         import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                     import __, __SKIP__
from osbot_utils.utils.Misc                                                     import is_guid
from osbot_utils.utils.Objects                                                  import obj
from osbot_utils.utils.Zip                                                      import zip_bytes_empty, zip_bytes__add_file, zip_bytes__file_list
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy   import Enum__Cache__Store__Strategy
from tests.unit.Service__Cache__Test_Objs                                       import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE

class test_Routes__Zip__http(TestCase):                                                      # HTTP tests for ZIP routes using live FastAPI server

    @classmethod
    def setUpClass(cls):                                                                     # ONE-TIME expensive setup
        cls.test_objs         = setup__service__cache__test_objs()
        cls.cache_fixtures    = cls.test_objs.cache_fixtures
        cls.service__fast_api = cls.test_objs.fast_api
        cls.service__app      = cls.test_objs.fast_api__app

        cls.fast_api_server = Fast_API_Server(app=cls.service__app)
        cls.fast_api_server.start()

        cls.base_url       = cls.fast_api_server.url().rstrip("/")
        cls.headers        = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        cls.test_namespace = f"http-zip-test-{int(time.time())}"                            # Unique namespace
        cls.path_now       = Path__Handler__Temporal().path_now()                           # Current temporal path

        # Create test ZIP files
        cls.test_zip = zip_bytes_empty()
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "test1.txt", b"content 1")
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "test2.txt", b"content 2")

        cls.created_resources = []                                                          # Track created resources

    @classmethod
    def tearDownClass(cls):                                                                 # Stop server
        cls.fast_api_server.stop()

    def setUp(self):
        self.stored_cache_id = None                                                         # Will be set when storing zip

    def tearDown(self):                                                                     # PER-TEST cleanup
        self.created_resources.clear()

    def _store_zip(self, zip_bytes : bytes          = None,
                        cache_key  : str            = None,
                        file_id    : str            = None,
                        namespace  : str            = None
                  ) -> Dict[str, Any]:                                                      # Helper to store ZIP
        namespace = namespace or self.test_namespace
        url       = f"{self.base_url}/{namespace}/zip/store"

        headers = {**self.headers, "Content-Type": "application/zip"}
        params  = {}
        if cache_key:
            params['cache_key'] = cache_key
        if file_id:
            params['file_id'] = file_id

        zip_bytes = zip_bytes or self.test_zip
        response = requests.post(url, data=zip_bytes, headers=headers, params=params)

        assert response.status_code == 200
        result = response.json()

        self.created_resources.append({ 'cache_id' : result.get('cache_id'),
                                        'namespace': namespace             ,
                                        'type'     : 'zip'                 })
        return result

    def test_health_check(self):                                                         # Test API is accessible
        response = requests.get(f"{self.base_url}/info/health", headers=self.headers)
        assert response.status_code == 200
        assert response.json() == {'status': 'ok'}

    def test_store_zip(self):                                                           # Test POST /namespace/zip/store
        result = self._store_zip()

        cache_id   = result.get("cache_id")
        cache_hash = result.get("cache_hash")

        assert is_guid(cache_id) is True
        assert len(cache_hash) == 16
        assert result["namespace"] == self.test_namespace
        assert result["file_count"] == 2
        assert result["size"] > 0
        assert "paths" in result
        assert "stored_at" in result

    def test_list_zip_files(self):                                                      # Test GET /namespace/zip/{cache_id}/list
        store_result = self._store_zip()
        cache_id = store_result["cache_id"]

        url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/list"
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        result = response.json()

        assert result["success"] == True
        assert result["operation"] == "list"
        assert result["cache_id"] == cache_id
        assert len(result["file_list"]) == 2
        assert "test1.txt" in result["file_list"]
        assert "test2.txt" in result["file_list"]

    def test_get_zip_file(self):                                                        # Test GET /namespace/zip/{cache_id}/file
        store_result = self._store_zip()
        cache_id     = store_result["cache_id"]
        cache_hash   = store_result["cache_hash"]
        assert type(store_result) is dict
        assert obj(store_result)  ==  __( cache_id     = cache_id            ,
                                          cache_hash   = cache_hash          ,
                                          namespace    = self.test_namespace ,
                                          paths        = __(data   = [ f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin',
                                                                       f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin.config',
                                                                       f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin.metadata'],
                                                           by_hash = [ f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json',
                                                                       #f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config',
                                                                       f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                                           by_id   = [ f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                                                                       f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                                                                       f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata']),
                                          size       = 232          ,
                                          file_count = 2            ,
                                          stored_at  = __SKIP__     )


        file_path = "test1.txt"
        url       = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/file/{file_path}"

        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        assert response.content     == b"content 1"
        assert response.headers["content-type"] == "application/octet-stream"

    def test_zip_file__creates_new_entry(self):                                     # Test POST creates new cache entry
        skip__if_not__in_github_actions()
        original_result = self._store_zip()
        original_id = original_result["cache_id"]

        url = f"{self.base_url}/{self.test_namespace}/zip/{original_id}/file/new.txt"
        response = requests.post(url,
                                data=b"new content",
                                headers={**self.headers, "Content-Type": "application/octet-stream"})

        assert response.status_code == 200
        result = response.json()

        assert result["success"] == True
        assert result["operation"] == "add"
        assert result["cache_id"] != original_id                                          # NEW cache ID!
        assert result["original_cache_id"] == original_id
        assert "new.txt" in result["files_affected"]

        new_id = result["cache_id"]

        # Verify new entry has the added file
        list_url = f"{self.base_url}/{self.test_namespace}/zip/{new_id}/list"
        list_response = requests.get(list_url, headers=self.headers)
        assert "new.txt" in list_response.json()["file_list"]
        assert len(list_response.json()["file_list"]) == 3

        # Verify original unchanged
        original_list_url = f"{self.base_url}/{self.test_namespace}/zip/{original_id}/list"
        original_list = requests.get(original_list_url, headers=self.headers)
        assert "new.txt" not in original_list.json()["file_list"]
        assert len(original_list.json()["file_list"]) == 2

    def test_remove_zip_file__creates_new_entry(self):                                  # Test DELETE creates new cache entry
        skip__if_not__in_github_actions()
        original_result = self._store_zip()
        original_id = original_result["cache_id"]

        url = f"{self.base_url}/{self.test_namespace}/zip/{original_id}/file/test1.txt"
        response = requests.delete(url, headers=self.headers)

        assert response.status_code == 200
        result = response.json()

        assert result["success"] == True
        assert result["operation"] == "remove"
        assert result["cache_id"] != original_id                                          # NEW cache ID!
        assert result["original_cache_id"] == original_id
        assert "test1.txt" in result["files_affected"]

        new_id = result["cache_id"]

        # Verify new entry has file removed
        list_url = f"{self.base_url}/{self.test_namespace}/zip/{new_id}/list"
        list_response = requests.get(list_url, headers=self.headers)
        assert "test1.txt" not in list_response.json()["file_list"]
        assert len(list_response.json()["file_list"]) == 1

    def test_batch_operations__creates_new_entry(self):                                 # Test batch creates single new entry
        skip__if_not__in_github_actions()
        original_result = self._store_zip()
        original_id = original_result["cache_id"]

        batch_request = { "cache_id"  : original_id,
                          "operations": [{ "action"   : "add"       ,
                                           "path"     : "new.txt"   ,
                                           "content"  : base64.b64encode(b"new content").decode('utf-8'),   # todo: double check that this is not causing some side effects (since we will need to make sure the roundtrip of b64encode is working ok)
                                           "condition": "always"              ,
                                           "new_path" : ""                    ,
                                           "pattern"  : ''                    },
                                         { "action"   : "remove"              ,
                                           "path"     : "test1.txt"           ,
                                           "condition": "if_exists"           ,
                                           "content"  : ''                    ,
                                           "pattern"  : ''                    ,
                                           "new_path" : ""                    }],
                          "atomic"     : False                                ,
                          "namespace"  : self.test_namespace                  ,
                          "strategy"   : Enum__Cache__Store__Strategy.TEMPORAL}

        url = f"{self.base_url}/{self.test_namespace}/zip/{original_id}/batch"
        response = requests.post(url, json=batch_request, headers=self.headers)

        assert response.status_code == 200
        result = response.json()

        assert result["success"           ] == True
        assert result["cache_id"          ] != original_id                                          # NEW cache ID!
        assert result["original_cache_id" ] == original_id
        assert result["operations_applied"] == 2
        assert result["operations_failed" ] == 0

    def test_batch_operations__atomic_failure(self):                                    # Test atomic failure doesn't create new entry
        original_result = self._store_zip()
        original_id = original_result["cache_id"]

        batch_request = { "cache_id" : original_id,
                         "operations": [ { "action"   : "add"                   ,
                                           "path"     : "good.txt"              ,
                                           "content"  : base64.b64encode(b"good").decode('utf-8'),
                                           "condition": "always"                ,
                                           "pattern"  : ''                      ,
                                           "new_path" : ""                      },
                                         { "action"   : "remove"                ,
                                           "path"     : "nonexistent.txt"       ,                                  # Will fail
                                           "condition": "always"                ,
                                           "content"  : ''                      ,
                                           "pattern"  : ''                      ,
                                           "new_path" : ""                      }],
                        "atomic"     : True,
                        "namespace"  : self.test_namespace,
                        "strategy"   : Enum__Cache__Store__Strategy.TEMPORAL}

        url      = f"{self.base_url}/{self.test_namespace}/zip/{original_id}/batch"
        response = requests.post(url, json=batch_request, headers=self.headers)
        result   = response.json()
        assert response.status_code == 200
        assert result["success"] == False
        assert result["cache_id"] == original_id                                          # No new entry on failure
        assert result["rollback_performed"] == True

    def test_download_zip(self):                                                        # Test GET /namespace/zip/{cache_id}/download
        skip__if_not__in_github_actions()
        store_result = self._store_zip()
        cache_id = store_result["cache_id"]

        url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/download"
        response = requests.get(url, headers=self.headers)

        assert response.status_code == 200
        assert response.content == self.test_zip
        assert "application/zip" in response.headers["content-type"]
        assert f"{cache_id}.zip" in response.headers.get("content-disposition", "")

    def test_immutability_chain(self):                                                  # Test chain of operations
        skip__if_not__in_github_actions()
        # Store initial
        v1_result = self._store_zip()
        v1_id = v1_result["cache_id"]

        # Operation 1: Add file
        add_url = f"{self.base_url}/{self.test_namespace}/zip/{v1_id}/file/v2.txt"
        add_response = requests.post(add_url,
                                    data=b"version 2",
                                    headers={**self.headers, "Content-Type": "application/octet-stream"})
        v2_id = add_response.json()["cache_id"]
        assert v2_id != v1_id

        # Operation 2: Remove file from v2
        remove_url = f"{self.base_url}/{self.test_namespace}/zip/{v2_id}/file/test1.txt"
        remove_response = requests.delete(remove_url, headers=self.headers)
        v3_id = remove_response.json()["cache_id"]
        assert v3_id != v2_id
        assert v3_id != v1_id

        # Verify all three versions exist independently
        for cache_id, expected_count in [(v1_id, 2), (v2_id, 3), (v3_id, 2)]:
            list_url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/list"
            list_response = requests.get(list_url, headers=self.headers)
            assert len(list_response.json()["file_list"]) == expected_count

    def test_concurrent_zip_operations(self):                                           # Test concurrent ZIP operations
        skip__if_not__in_github_actions()

        def store_and_modify_zip(index):
            # Create unique zip
            test_zip = zip_bytes_empty()
            test_zip = zip_bytes__add_file(test_zip, f"file_{index}.txt", f"content_{index}".encode())

            # Store it
            store_result = self._store_zip(zip_bytes=test_zip)
            cache_id = store_result["cache_id"]

            # Add another file
            add_url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/file/added_{index}.txt"
            add_response = requests.post(add_url,
                                        data=f"added_{index}".encode(),
                                        headers={**self.headers, "Content-Type": "application/octet-stream"})

            return add_response.status_code == 200, cache_id

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(store_and_modify_zip, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        for success, cache_id in results:
            assert success is True
            assert is_guid(cache_id) is True

    def test_large_zip_file(self):                                                      # Test large ZIP file
        skip__if_not__in_github_actions()

        large_zip = zip_bytes_empty()
        for i in range(100):
            large_zip = zip_bytes__add_file(large_zip, f"file_{i}.txt", f"content_{i}".encode() * 100)

        result = self._store_zip(zip_bytes=large_zip)

        assert is_guid(result["cache_id"]) is True
        assert result["file_count"] == 100
        assert result["size"] > 10000

    def test_zip_with_nested_paths(self):                                              # Test ZIP with nested directory structure
        nested_zip = zip_bytes_empty()
        nested_zip = zip_bytes__add_file(nested_zip, "dir1/file1.txt"     , b"content1")
        nested_zip = zip_bytes__add_file(nested_zip, "dir1/dir2/file2.txt", b"content2")
        nested_zip = zip_bytes__add_file(nested_zip, "dir3/file3.txt"     , b"content3")

        result = self._store_zip(zip_bytes=nested_zip)
        cache_id = result["cache_id"]

        # List files
        list_url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/list"
        list_response = requests.get(list_url, headers=self.headers)

        files = list_response.json()["file_list"]
        assert "dir1/file1.txt"      in files
        assert "dir1/dir2/file2.txt" in files
        assert "dir3/file3.txt"      in files

    def test_batch_with_patterns(self):                                                # Test batch operations with patterns
        skip__if_not__in_github_actions()
        # Create zip with multiple similar files
        pattern_zip = zip_bytes_empty()
        pattern_zip = zip_bytes__add_file(pattern_zip, "test1.tmp", b"temp1")
        pattern_zip = zip_bytes__add_file(pattern_zip, "test2.tmp", b"temp2")
        pattern_zip = zip_bytes__add_file(pattern_zip, "test.txt", b"keep")

        store_result = self._store_zip(zip_bytes=pattern_zip)
        cache_id = store_result["cache_id"]

        # Batch remove with pattern
        batch_request = {
            "cache_id": cache_id,
            "operations": [{"action"    : "remove",
                            "path"      : "",
                            "pattern"   : "*.tmp",
                            "condition" : "always",
                            "content"   : '' ,
                            "new_path"  : ''      }],
            "atomic"     : True                                  ,
            "namespace"  : self.test_namespace                   ,
            "strategy"   : Enum__Cache__Store__Strategy.TEMPORAL }

        url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/batch"
        response = requests.post(url, json=batch_request, headers=self.headers)

        assert response.status_code == 200

        result = response.json()
        new_id = result.get("cache_id", cache_id)

        # Verify pattern matching worked
        list_url = f"{self.base_url}/{self.test_namespace}/zip/{new_id}/list"
        list_response = requests.get(list_url, headers=self.headers)
        remaining_files = list_response.json()["file_list"]

        assert "test.txt" in remaining_files
        assert "test1.tmp" not in remaining_files
        assert "test2.tmp" not in remaining_files

    def test_error_handling(self):                                                     # Test error scenarios
        # Invalid ZIP data
        url = f"{self.base_url}/{self.test_namespace}/zip/store"
        response = requests.post(url,
                                data=b"not a valid zip",
                                headers={**self.headers, "Content-Type": "application/zip"})
        assert response.status_code == 400

        # Non-existent cache ID
        fake_id = "00000000-0000-0000-0000-000000000000"
        list_url = f"{self.base_url}/{self.test_namespace}/zip/{fake_id}/list"
        list_response = requests.get(list_url, headers=self.headers)
        assert list_response.status_code == 404

        # Missing file path for add operation
        store_result = self._store_zip()
        cache_id = store_result["cache_id"]
        add_url = f"{self.base_url}/{self.test_namespace}/zip/{cache_id}/file/"
        add_response = requests.post(add_url,
                                    data=b"content",
                                    headers={**self.headers, "Content-Type": "application/octet-stream"})
        assert add_response.status_code in [400, 404]  # Could be 404 due to route not matching

    def test_compressed_zip_handling(self):                                            # Test storing already compressed ZIP
        # Create and compress a ZIP
        test_zip = zip_bytes_empty()
        test_zip = zip_bytes__add_file(test_zip, "file.txt", b"content" * 1000)
        compressed_zip = gzip.compress(test_zip)

        # Try to store compressed (should fail as invalid ZIP)
        url = f"{self.base_url}/{self.test_namespace}/zip/store"
        response = requests.post(url,
                                data=compressed_zip,
                                headers={**self.headers, "Content-Type": "application/zip"})
        assert response.status_code == 400

    def test_namespace_isolation(self):                                                # Test namespace isolation for ZIPs
        ns1 = f"zip-ns1-{int(time.time())}"
        ns2 = f"zip-ns2-{int(time.time())}"

        # Store same ZIP in different namespaces
        result1 = self._store_zip(namespace=ns1)
        result2 = self._store_zip(namespace=ns2)

        assert result1["cache_id"] != result2["cache_id"]
        assert result1["namespace"] == ns1
        assert result2["namespace"] == ns2

        # Operations in one namespace shouldn't affect the other
        add_url1 = f"{self.base_url}/{ns1}/zip/{result1['cache_id']}/file/new.txt"
        requests.post(add_url1,
                     data=b"new content",
                     headers={**self.headers, "Content-Type": "application/octet-stream"})

        # Original in ns2 should be unchanged
        list_url2 = f"{self.base_url}/{ns2}/zip/{result2['cache_id']}/list"
        list_response2 = requests.get(list_url2, headers=self.headers)
        assert "new.txt" not in list_response2.json()["file_list"]