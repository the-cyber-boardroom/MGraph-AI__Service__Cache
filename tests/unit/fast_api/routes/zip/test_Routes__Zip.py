# from unittest                                                                              import TestCase
# from fastapi.testclient                                                                   import TestClient
# from osbot_utils.testing.__                                                                import __, __SKIP__
# from osbot_utils.utils.Zip                                                                 import zip_bytes_empty, zip_bytes__add_file
# from mgraph_ai_service_cache.service.cache.Cache__Service                                  import Cache__Service
# from mgraph_ai_service_cache.api.routes.zip.Routes__Zip                                    import Routes__Zip
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Response         import Schema__Cache__Zip__Store__Response
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Response     import Schema__Cache__Zip__Operation__Response
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Request          import Schema__Cache__Zip__Batch__Request
# from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Response         import Schema__Cache__Zip__Batch__Response
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                      import Random_Guid
# import pytest
#
#
# class test_Routes__Zip(TestCase):
#
#     @classmethod
#     def setUpClass(cls):                                                                   # One-time expensive setup for FastAPI
#         from fastapi import FastAPI
#
#         # Create FastAPI app and routes
#         cls.app = FastAPI()
#         cls.cache_service = Cache__Service()
#         cls.routes = Routes__Zip(cache_service=cls.cache_service)
#         cls.routes.setup_routes()
#
#         # Add routes to app
#         cls.app.include_router(cls.routes.router, prefix=cls.routes.prefix, tags=[cls.routes.tag])
#
#         # Create test client
#         cls.client = TestClient(cls.app)
#
#         # Create test zip files once
#         cls.test_zip = zip_bytes_empty()
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "test1.txt", b"content 1")
#         cls.test_zip = zip_bytes__add_file(cls.test_zip, "test2.txt", b"content 2")
#
#         cls.test_namespace = "test-routes"
#
#     def setUp(self):                                                                       # Per-test setup
#         self.stored_cache_id = None                                                       # Will be set when storing zip
#
#     def test__init__(self):                                                               # Test routes initialization
#         with Routes__Zip() as _:
#             assert _.tag == "zip"
#             assert _.prefix == "/{namespace}"
#             assert type(_.cache_service) is Cache__Service
#
#     def test__service_methods(self):                                                      # Test service method initialization
#         with self.routes as _:
#             # Test service getters
#             assert type(_.zip_store_service()) is type(_.zip_store_service())             # Cached on self
#             assert type(_.zip_ops_service()) is type(_.zip_ops_service())
#             assert type(_.zip_batch_service()) is type(_.zip_batch_service())
#
#             # Verify services share cache_service
#             assert _.zip_store_service().cache_service is _.cache_service
#             assert _.zip_ops_service().cache_service is _.cache_service
#             assert _.zip_batch_service().cache_service is _.cache_service
#
#     def test_store_zip(self):                                                             # Test POST /namespace/zip/store
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#
#         assert response.status_code == 200
#         result = response.json()
#
#         # Verify response structure
#         assert "cache_id" in result
#         assert "cache_hash" in result
#         assert "namespace" in result
#         assert "file_count" in result
#         assert "size" in result
#         assert "compression_info" in result
#
#         # Verify data
#         assert result["namespace"] == self.test_namespace
#         assert result["file_count"] == 2                                                  # Two files in test zip
#         assert result["size"] > 0
#
#         # Store ID for other tests
#         self.stored_cache_id = result["cache_id"]
#
#     def test_store_zip__with_params(self):                                                # Test store with optional params
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/store?cache_key=backups/test.zip&file_id=custom-id",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#
#         assert response.status_code == 200
#         result = response.json()
#         assert result["file_count"] == 2
#
#     def test_store_zip__invalid_zip(self):                                                # Test store with invalid data
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=b"not a valid zip",
#             headers={"Content-Type": "application/zip"}
#         )
#
#         assert response.status_code == 400
#         assert "Invalid zip file" in response.json()["detail"]
#
#     def test_list_zip_files(self):                                                        # Test GET /namespace/zip/{cache_id}/list
#         # First store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # List files
#         response = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/list")
#
#         assert response.status_code == 200
#         result = response.json()
#
#         assert result["success"] == True
#         assert result["operation"] == "list"
#         assert len(result["file_list"]) == 2
#         assert "test1.txt" in result["file_list"]
#         assert "test2.txt" in result["file_list"]
#
#     def test_list_zip_files__not_found(self):                                             # Test list on non-existent zip
#         fake_id = Random_Guid()
#         response = self.client.get(f"/{self.test_namespace}/zip/{fake_id}/list")
#
#         assert response.status_code == 404
#         assert "Zip file not found" in response.json()["detail"]
#
#     def test_get_zip_file(self):                                                          # Test GET /namespace/zip/{cache_id}/file
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Get specific file
#         response = self.client.get(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=test1.txt"
#         )
#
#         assert response.status_code == 200
#         assert response.content == b"content 1"
#         assert response.headers["content-type"] == "application/octet-stream"
#
#     def test_get_zip_file__not_found(self):                                               # Test get non-existent file
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Get non-existent file
#         response = self.client.get(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=missing.txt"
#         )
#
#         assert response.status_code == 404
#         assert "not found in zip" in response.json()["detail"]
#
#     def test_add_zip_file(self):                                                          # Test POST /namespace/zip/{cache_id}/file
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Add new file
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=new.txt",
#             content=b"new content"
#         )
#
#         assert response.status_code == 200
#         result = response.json()
#         assert result["success"] == True
#         assert result["operation"] == "add"
#         assert "new.txt" in result["files_affected"]
#
#         # Verify file was added
#         list_response = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/list")
#         assert "new.txt" in list_response.json()["file_list"]
#
#     def test_add_zip_file__no_path(self):                                                 # Test add without file_path
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Try to add without path
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/{cache_id}/file",
#             content=b"content"
#         )
#
#         assert response.status_code == 400
#         assert "file_path required" in response.json()["detail"]
#
#     def test_remove_zip_file(self):                                                       # Test DELETE /namespace/zip/{cache_id}/file
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Remove file
#         response = self.client.delete(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=test1.txt"
#         )
#
#         assert response.status_code == 200
#         result = response.json()
#         assert result["success"] == True
#         assert result["operation"] == "remove"
#         assert "test1.txt" in result["files_affected"]
#
#         # Verify file was removed
#         list_response = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/list")
#         assert "test1.txt" not in list_response.json()["file_list"]
#
#     def test_batch_operations(self):                                                      # Test POST /namespace/zip/{cache_id}/batch
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Create batch request
#         batch_request = {
#             "cache_id": cache_id,
#             "operations": [
#                 {
#                     "action": "add",
#                     "path": "new.txt",
#                     "content": "bmV3IGNvbnRlbnQ=",                                        # base64 of "new content"
#                     "condition": "always"
#                 },
#                 {
#                     "action": "remove",
#                     "path": "test1.txt",
#                     "condition": "if_exists"
#                 }
#             ],
#             "atomic": False,
#             "namespace": self.test_namespace,
#             "create_backup": True
#         }
#
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/{cache_id}/batch",
#             json=batch_request
#         )
#
#         assert response.status_code == 200
#         result = response.json()
#
#         assert result["success"] == True
#         assert result["operations_applied"] == 2
#         assert result["operations_failed"] == 0
#         assert "new.txt" in result["files_added"]
#         assert "test1.txt" in result["files_removed"]
#         assert result["backup_cache_id"] is not None                                      # Backup created
#
#     def test_batch_operations__atomic_failure(self):                                      # Test atomic batch rollback
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Create batch with failing operation
#         batch_request = {
#             "cache_id": cache_id,
#             "operations": [
#                 {
#                     "action": "add",
#                     "path": "good.txt",
#                     "content": "Z29vZA==",                                                 # base64 of "good"
#                     "condition": "always"
#                 },
#                 {
#                     "action": "remove",
#                     "path": "nonexistent.txt",                                            # Will fail
#                     "condition": "always"
#                 }
#             ],
#             "atomic": True,                                                               # Atomic mode
#             "namespace": self.test_namespace,
#             "create_backup": False
#         }
#
#         response = self.client.post(
#             f"/{self.test_namespace}/zip/{cache_id}/batch",
#             json=batch_request
#         )
#
#         assert response.status_code == 200
#         result = response.json()
#
#         assert result["success"] == False
#         assert result["rollback_performed"] == True
#         assert result["operations_applied"] == 1
#         assert result["operations_failed"] == 1
#
#     def test_download_zip(self):                                                          # Test GET /namespace/zip/{cache_id}/download
#         # Store a zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # Download entire zip
#         response = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/download")
#
#         assert response.status_code == 200
#         assert response.headers["content-type"] == "application/zip"
#         assert f"filename={cache_id}.zip" in response.headers["content-disposition"]
#         assert response.content == self.test_zip                                          # Exact content preserved
#
#     def test_download_zip__not_found(self):                                               # Test download non-existent zip
#         fake_id = Random_Guid()
#         response = self.client.get(f"/{self.test_namespace}/zip/{fake_id}/download")
#
#         assert response.status_code == 404
#         assert "Zip file not found" in response.json()["detail"]
#
#     def test_complete_workflow(self):                                                     # Test complete zip workflow
#         # Store initial zip
#         store_response = self.client.post(
#             f"/{self.test_namespace}/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#         cache_id = store_response.json()["cache_id"]
#
#         # List files
#         list_response = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/list")
#         assert len(list_response.json()["file_list"]) == 2
#
#         # Add a file
#         add_response = self.client.post(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=added.txt",
#             content=b"added content"
#         )
#         assert add_response.status_code == 200
#
#         # Get the added file
#         get_response = self.client.get(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=added.txt"
#         )
#         assert get_response.content == b"added content"
#
#         # Remove original file
#         remove_response = self.client.delete(
#             f"/{self.test_namespace}/zip/{cache_id}/file?file_path=test1.txt"
#         )
#         assert remove_response.status_code == 200
#
#         # Final list
#         final_list = self.client.get(f"/{self.test_namespace}/zip/{cache_id}/list")
#         files = final_list.json()["file_list"]
#         assert len(files) == 2                                                            # 2 original - 1 + 1 = 2
#         assert "added.txt" in files
#         assert "test1.txt" not in files
#         assert "test2.txt" in files
#
#     def test_error_handling__invalid_namespace(self):                                     # Test with special chars in namespace
#         # Note: Depending on Safe_Str__Id implementation, this might sanitize instead of error
#         response = self.client.post(
#             f"/test@namespace!/zip/store",
#             content=self.test_zip,
#             headers={"Content-Type": "application/zip"}
#         )
#
#         # Either sanitized or error, but should handle gracefully
#         assert response.status_code in [200, 400, 422]