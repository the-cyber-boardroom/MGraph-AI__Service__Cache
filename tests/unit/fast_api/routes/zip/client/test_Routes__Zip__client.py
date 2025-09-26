from unittest                                                   import TestCase

from osbot_utils.utils.Dev import pprint
from osbot_utils.utils.Zip                                      import zip_bytes_empty, zip_bytes__add_file
from mgraph_ai_service_cache.fast_api.routes.zip.Routes__Zip    import Routes__Zip
from mgraph_ai_service_cache.service.cache.Cache__Service       import Cache__Service
from tests.unit.Service__Cache__Test_Objs                       import setup__service__cache__test_objs, TEST_API_KEY__NAME, TEST_API_KEY__VALUE


class test_Routes__Zip__client(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_objs       = setup__service__cache__test_objs()
        cls.fast_api        = cls.test_objs.fast_api
        cls.app             = cls.test_objs.fast_api__app
        cls.cache_service   = cls.fast_api.cache_service
        cls.routes          = Routes__Zip(cache_service=cls.cache_service)

        cls.client = cls.test_objs.fast_api__client

        cls.test_zip = zip_bytes_empty()                                                    # Create test zip files to use in this test
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "test1.txt", b"content 1")
        cls.test_zip = zip_bytes__add_file(cls.test_zip, "test2.txt", b"content 2")

        cls.test_namespace = "test-routes"

    def setUp(self):                                                                        # Per-test setup
        self.stored_cache_id = None                                                         # Will be set when storing zip

    def test__init__(self):                                                                 # Test routes initialization
        with Routes__Zip() as _:
            assert _.tag == "zip"
            assert _.prefix == "/{namespace}"
            assert type(_.cache_service) is Cache__Service

    def test__service_methods(self):                                                      # Test service method initialization
        with self.routes as _:
            # Test service getters
            assert type(_.zip_store_service()) is type(_.zip_store_service())             # Cached on self
            assert type(_.zip_ops_service()) is type(_.zip_ops_service())
            assert type(_.zip_batch_service()) is type(_.zip_batch_service())

            # Verify services share cache_service
            assert _.zip_store_service().cache_service is _.cache_service
            assert _.zip_ops_service().cache_service is _.cache_service
            assert _.zip_batch_service().cache_service is _.cache_service

    def test_store_zip(self):                                                             # Test POST /namespace/zip/store
        response = self.client.post(url     = f"/{self.test_namespace}/zip/store",
                                    content = self.test_zip,
                                    headers = {"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE})

        assert response.status_code == 200
        result = response.json()

        # Verify response structure
        assert "cache_id"   in result
        assert "cache_hash" in result
        assert "namespace"  in result
        assert "file_count" in result
        assert "size"       in result

        # Verify data
        assert result["namespace"]  == self.test_namespace
        assert result["file_count"] == 2                                                  # Two files in test zip
        assert result["size"]        > 0

        # Store ID for other tests
        self.stored_cache_id = result["cache_id"]

    def test_list_zip_files(self):                                                        # Test GET /namespace/zip/{cache_id}/list (read-only)
        store_response = self.client.post(url     = f"/{self.test_namespace}/zip/store",                # First store a zip
                                          content = self.test_zip,
                                          headers = {"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE})

        cache_id = store_response.json()["cache_id"]

        # List files
        request_url = f"/{self.test_namespace}/zip/{cache_id}/list"
        response = self.client.get(url= request_url, headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert response.status_code == 200
        result = response.json()

        assert result["success"] == True
        assert result["operation"] == "list"
        assert result["cache_id"] == cache_id                                             # List doesn't change ID
        assert len(result["file_list"]) == 2
        assert "test1.txt" in result["file_list"]
        assert "test2.txt" in result["file_list"]

    def test_get_zip_file(self):                                                          # Test GET /namespace/zip/{cache_id}/file (read-only)
        # Store a zip
        store_response = self.client.post(url     = f"/{self.test_namespace}/zip/store",
                                          content = self.test_zip,
                                          headers={"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        cache_id = store_response.json()["cache_id"]

        # Get specific file
        response = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/file/test1.txt" , headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})

        assert response.status_code == 200
        assert response.content == b"content 1"
        assert response.headers["content-type"] == "application/octet-stream"

    def test_add_zip_file__creates_new_entry(self):                                       # Test POST creates new cache entry
        # Store a zip
        store_response = self.client.post(url     = f"/{self.test_namespace}/zip/store",
                                          content = self.test_zip,
                                          headers = {"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        )
        original_id = store_response.json()["cache_id"]

        # Add new file
        response = self.client.post(url     = f"/{self.test_namespace}/zip/{original_id}/file/new.txt",
                                    content = b"new content"                                                ,
                                    headers = {"Content-Type"    : "application/octet-stream" ,
                                               TEST_API_KEY__NAME: TEST_API_KEY__VALUE} )
        assert response.status_code == 200
        result = response.json()
        assert result["success"] == True
        assert result["operation"] == "add"
        assert result["cache_id"] != original_id                                          # NEW cache ID!
        assert result["original_cache_id"] == original_id                                 # Original preserved
        assert "new.txt" in result["files_affected"]

        new_id = result["cache_id"]

        # Verify new entry has the added file
        list_response = self.client.get(f"/{self.test_namespace}/zip/{new_id}/list", headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert "new.txt" in list_response.json()["file_list"]
        assert len(list_response.json()["file_list"]) == 3                                # 2 original + 1 new

        # Verify original unchanged
        original_list = self.client.get(f"/{self.test_namespace}/zip/{original_id}/list", headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert "new.txt" not in original_list.json()["file_list"]
        assert len(original_list.json()["file_list"]) == 2                                # Original unchanged

    def test_remove_zip_file__creates_new_entry(self):                                    # Test DELETE creates new cache entry
        # Store a zip
        store_response = self.client.post(url     = f"/{self.test_namespace}/zip/store",
                                          content = self.test_zip,
                                          headers = { "Content-Type"    : "application/zip",
                                                      TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        original_id = store_response.json()["cache_id"]

        # Remove file
        response = self.client.delete(url     =  f"/{self.test_namespace}/zip/{original_id}/file/test1.txt",
                                      headers = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE})


        assert response.status_code == 200
        result = response.json()
        assert result["success"] == True
        assert result["operation"] == "remove"
        assert result["cache_id"] != original_id                                          # NEW cache ID!
        assert result["original_cache_id"] == original_id                                 # Original preserved
        assert "test1.txt" in result["files_affected"]

        new_id = result["cache_id"]

        # Verify new entry has file removed
        list_response = self.client.get(url     = f"/{self.test_namespace}/zip/{new_id}/list",
                                        headers = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert "test1.txt" not in list_response.json()["file_list"]
        assert len(list_response.json()["file_list"]) == 1                                # 2 - 1 = 1

        # Verify original unchanged
        original_list = self.client.get(url     = f"/{self.test_namespace}/zip/{original_id}/list",
                                        headers = {TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert "test1.txt" in original_list.json()["file_list"]                           # Still in original
        assert len(original_list.json()["file_list"]) == 2

    def test_batch_operations__creates_new_entry(self):                                   # Test batch creates single new entry
        # Store a zip
        store_response = self.client.post(url     = f"/{self.test_namespace}/zip/store",
                                          content = self.test_zip,
                                          headers = { "Content-Type"    : "application/zip",
                                                      TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        original_id = store_response.json()["cache_id"]

        # Create batch request
        batch_request = { "cache_id"  : original_id,
                          "operations": [ {  "action"    : "add",
                                             "path"     : "new.txt",
                                             "content"  : "bmV3IGNvbnRlbnQ=",                                        # base64 of "new content"
                                             "condition": "always" ,
                                             "new_path" : ""       ,
                                             "pattern"  : ""},
                                           { "action"   : "remove",
                                             "content"  : ''    ,
                                             "path"     : "test1.txt",
                                             "condition": "if_exists",
                                             "new_path" : ""        ,
                                             "pattern"  : ""}],
                          "atomic"    : False   ,
                          "strategy"  : "direct",
                          "namespace" : self.test_namespace}

        response = self.client.post(url     = f"/{self.test_namespace}/zip/{original_id}/batch",
                                    json    = batch_request ,
                                    headers = { "Content-Type"    : "application/json",
                                                TEST_API_KEY__NAME: TEST_API_KEY__VALUE})

        assert response.status_code == 200
        result = response.json()

        assert result["success"] == True
        assert result["cache_id"] != original_id                                          # NEW cache ID!
        assert result["original_cache_id"] == original_id                                 # Original preserved
        assert result["operations_applied"] == 2
        assert result["operations_failed"] == 0
        assert "new.txt" in result["files_added"]
        assert "test1.txt" in result["files_removed"]

        new_id = result["cache_id"]
        # Verify new entry has all changes
        new_list = self.client.get(f"/{self.test_namespace}/zip/{new_id}/list", headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        new_files = new_list.json()["file_list"]
        assert "new.txt" in new_files
        assert "test1.txt" not in new_files
        assert "test2.txt" in new_files

        # Verify original unchanged
        original_list = self.client.get(f"/{self.test_namespace}/zip/{original_id}/list", headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        original_files = original_list.json()["file_list"]
        assert "new.txt" not in original_files
        assert "test1.txt" in original_files
        assert "test2.txt" in original_files

    def test_batch_operations__atomic_failure(self):                                      # Test atomic failure doesn't create new entry
        # Store a zip
        store_response = self.client.post(
            f"/{self.test_namespace}/zip/store",
            content=self.test_zip,
            headers={"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        )
        original_id = store_response.json()["cache_id"]

        # Create batch with failing operation
        batch_request = {
            "cache_id": original_id,
            "operations": [
                {
                    "action": "add",
                    "path": "good.txt",
                    "content": "Z29vZA==",                                                 # base64 of "good"
                    "condition": "always",
                    "new_path" : ""       ,
                    "pattern"  : ""
                },
                {
                    "action": "remove",
                    "path": "nonexistent.txt",                                            # Will fail
                    "content"  : "" ,
                    "condition": "always",
                    "new_path" : ""       ,
                    "pattern"  : ""
                }
            ],
            "atomic"   : True,                                                               # Atomic mode
            "strategy" : "direct"           ,
            "namespace": self.test_namespace
        }

        response = self.client.post(
            f"/{self.test_namespace}/zip/{original_id}/batch",
            json=batch_request                               ,
            headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})

        result = response.json()
        assert response.status_code == 200
        assert result["success"] == False
        assert result["cache_id"] == original_id                                          # No new entry on failure
        assert result["original_cache_id"] == original_id
        assert result["rollback_performed"] == True
        assert result["operations_applied"] == 1
        assert result["operations_failed"] == 1

    def test_download_zip__any_version(self):                                             # Test can download any version
        # Store initial zip
        store_response = self.client.post(f"/{self.test_namespace}/zip/store",
                                          content=self.test_zip,
                                          headers = { "Content-Type"    : "application/zip",
                                                     TEST_API_KEY__NAME: TEST_API_KEY__VALUE})

        original_id = store_response.json()["cache_id"]

        # Add file (creates new version)
        add_response = self.client.post(
            f"/{self.test_namespace}/zip/{original_id}/file/added.txt",
            content=b"added",
            headers = { "Content-Type"    : "application/zip",
                         TEST_API_KEY__NAME: TEST_API_KEY__VALUE},
        )

        new_id = add_response.json()["cache_id"]

        # Download original version
        original_download = self.client.get(f"/{self.test_namespace}/zip/{original_id}/download",
                                            headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert original_download.status_code == 200
        assert original_download.content == self.test_zip                                 # Original content

        # Download new version
        new_download = self.client.get(f"/{self.test_namespace}/zip/{new_id}/download",
                                       headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        assert new_download.status_code == 200
        assert new_download.content != self.test_zip                                      # Modified content
        assert len(new_download.content) > len(self.test_zip)                             # Has added file

    def test_immutability_chain(self):                                                    # Test chain of operations
        # Store initial
        store_response = self.client.post(
            f"/{self.test_namespace}/zip/store",
            content=self.test_zip,
            headers={"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        )
        v1_id = store_response.json()["cache_id"]

        # Operation 1: Add file
        add_response = self.client.post(
            f"/{self.test_namespace}/zip/{v1_id}/file/v2.txt",
            content=b"version 2",
            headers={"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        )
        v2_id = add_response.json()["cache_id"]
        assert v2_id != v1_id

        # Operation 2: Remove file from v2
        remove_response = self.client.delete(f"/{self.test_namespace}/zip/{v2_id}/file/test1.txt",
                                             headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE})
        v3_id = remove_response.json()["cache_id"]
        assert v3_id != v2_id
        assert v3_id != v1_id

        # Verify all three versions exist independently
        v1_files = self.client.get(f"/{self.test_namespace}/zip/{v1_id}/list",headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE}).json()["file_list"]
        v2_files = self.client.get(f"/{self.test_namespace}/zip/{v2_id}/list",headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE}).json()["file_list"]
        v3_files = self.client.get(f"/{self.test_namespace}/zip/{v3_id}/list",headers={TEST_API_KEY__NAME: TEST_API_KEY__VALUE}).json()["file_list"]

        # V1: Original
        assert len(v1_files) == 2
        assert "test1.txt" in v1_files
        assert "test2.txt" in v1_files
        assert "v2.txt" not in v1_files

        # V2: Added v2.txt
        assert len(v2_files) == 3
        assert "test1.txt" in v2_files
        assert "test2.txt" in v2_files
        assert "v2.txt" in v2_files

        # V3: Removed test1.txt from V2
        assert len(v3_files) == 2
        assert "test1.txt" not in v3_files
        assert "test2.txt" in v3_files
        assert "v2.txt" in v3_files

    def test_headers_for_new_cache_ids(self):                                             # Test response headers include new IDs
        # Store a zip
        store_response = self.client.post(
            f"/{self.test_namespace}/zip/store",
            content=self.test_zip,
            headers={"Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE}
        )
        original_id = store_response.json()["cache_id"]

        # Add file should include new ID headers
        add_response = self.client.post(
            f"/{self.test_namespace}/zip/{original_id}/file/new.txt",
            content=b"new",
            headers={ "Content-Type": "application/zip", TEST_API_KEY__NAME: TEST_API_KEY__VALUE}

        )

        # Check response has new cache ID
        result = add_response.json()
        assert result["cache_id"] != original_id
        assert result["original_cache_id"] == original_id