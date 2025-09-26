import pytest
from fastapi                                                                               import HTTPException, Request, Response
from unittest                                                                              import TestCase
from osbot_utils.utils.Misc                                                                import is_guid
from memory_fs.path_handlers.Path__Handler__Temporal                                       import Path__Handler__Temporal
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                              import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                                 import Safe_Str__Fast_API__Route__Tag
from osbot_utils.testing.__                                                                import __, __SKIP__
from osbot_utils.utils.Objects                                                             import base_classes
from osbot_utils.type_safe.Type_Safe                                                       import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                      import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id            import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path          import Safe_Str__File__Path
from osbot_fast_api.api.routes.Fast_API__Routes                                            import Fast_API__Routes
from osbot_utils.utils.Zip                                                                 import zip_bytes_empty, zip_bytes__add_file, zip_bytes__file_list, zip_bytes__files
from mgraph_ai_service_cache.fast_api.routes.zip.Routes__Zip                               import Routes__Zip, TAG__ROUTES_ZIP, PREFIX__ROUTES_ZIP, ROUTES_PATHS__ZIP
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Retrieve__Success                import Schema__Cache__Retrieve__Success
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy              import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                                  import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Store__Request          import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Store__Response         import Schema__Cache__Zip__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Response     import Schema__Cache__Zip__Operation__Response
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Batch__Request          import Schema__Cache__Zip__Batch__Request, Schema__Zip__Batch__Operation
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Batch__Response         import Schema__Cache__Zip__Batch__Response
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve               import Cache__Service__Retrieve
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                     import Cache__Service__Store


class test_Routes__Zip(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.cache_service    = Cache__Service()
        cls.routes           = Routes__Zip             (cache_service=cls.cache_service)
        cls.retrieve_service = Cache__Service__Retrieve(cache_service=cls.cache_service)

        # Create test data
        cls.test_zip         = zip_bytes_empty()
        cls.test_zip         = zip_bytes__add_file(cls.test_zip, "file1.txt", b"content 1")
        cls.test_zip         = zip_bytes__add_file(cls.test_zip, "file2.txt", b"content 2")
        cls.test_namespace   = Safe_Str__Id("test-routes")
        cls.path_now         = Path__Handler__Temporal().path_now()                                     # Current temporal path

        # Store a test zip for operations
        store_request        = Schema__Cache__Zip__Store__Request(zip_bytes = cls.test_zip      ,
                                                                  namespace = cls.test_namespace)
        store_result         = cls.routes.zip_store_service().store_zip(store_request)
        cls.test_cache_id    = store_result.cache_id

    def setUp(self):
        self.request = Request(scope={"type": "http", "headers": []})                                   # Create mock request for methods that need it

    def test__init__(self):
        with Routes__Zip() as _:
            assert type(_)              is Routes__Zip
            assert base_classes(_)      == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                == TAG__ROUTES_ZIP
            assert _.prefix             == PREFIX__ROUTES_ZIP
            assert type(_.cache_service) is Cache__Service

    def test__class_constants(self):
        assert TAG__ROUTES_ZIP        == Safe_Str__Fast_API__Route__Tag('zip')
        assert PREFIX__ROUTES_ZIP     == Safe_Str__Fast_API__Route__Prefix('/{namespace}')
        assert len(ROUTES_PATHS__ZIP) == 9
        assert ROUTES_PATHS__ZIP[0]   == '/{namespace}/{strategy}/zip/create/{cache_key:path}/{file_id}'
        assert ROUTES_PATHS__ZIP[1]   == '/{namespace}/{strategy}/zip/store/{cache_key:path}/{file_id}'

    def test__service_methods(self):
        with self.routes as _:
            # Test service getters are cached
            store_service1 = _.zip_store_service()
            store_service2 = _.zip_store_service()
            assert store_service1 is store_service2  # Same instance (cached)

            ops_service1 = _.zip_ops_service()
            ops_service2 = _.zip_ops_service()
            assert ops_service1 is ops_service2

            batch_service1 = _.zip_batch_service()
            batch_service2 = _.zip_batch_service()
            assert batch_service1 is batch_service2

            # Verify services use the same cache_service
            assert store_service1.cache_service is _.cache_service
            assert ops_service1.cache_service   is _.cache_service
            assert batch_service1.cache_service is _.cache_service

    def test_zip_create(self):
        with self.routes as _:
            cache_key  = 'an/cache/key'
            file_id    = 'new-archive'
            result     = _.zip_create(namespace = self.test_namespace                    ,
                                      cache_key = cache_key                              ,
                                      file_id   = file_id                                ,
                                      strategy  = Enum__Cache__Store__Strategy.KEY_BASED )
            cache_id   = result.cache_id
            cache_hash = result.cache_hash
            assert cache_hash           == 'e3b0c44298fc1c14'
            assert type(result)          is Schema__Cache__Zip__Store__Response
            assert type(result.cache_id) is Random_Guid
            assert result.namespace      == self.test_namespace
            assert result.file_count     == 0
            assert result.size            > 0
            assert result.obj()          ==  __( cache_id      = cache_id          ,
                                                 cache_hash    = cache_hash        ,
                                                 error_type    = None              ,
                                                 error_message = None              ,
                                                 success       = True              ,
                                                 namespace     = 'test-routes'     ,
                                                 paths         = __(data   = [ f'{self.test_namespace}/data/key-based/{cache_key}/{file_id}.bin',
                                                                               f'{self.test_namespace}/data/key-based/{cache_key}/{file_id}.bin.config',
                                                                               f'{self.test_namespace}/data/key-based/{cache_key}/{file_id}.bin.metadata'],
                                                                   by_hash = [ f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json',
                                                                               f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config',
                                                                               f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                                                   by_id   = [ f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                                                                               f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                                                                               f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata']),
                                                 size          = 22           ,
                                                 file_count    = 0            ,
                                                 stored_at     = __SKIP__     )

            zip_files = _.zip_files_list(cache_id=cache_id, namespace=self.test_namespace)
            assert zip_files.obj() == __(cache_id           = cache_id,
                                         original_cache_id = None   ,
                                         message           = 'Found 0 files in zip',
                                         success           = True   ,
                                         operation         = 'list' ,
                                         file_list         = []     ,
                                         file_content      = b''    ,
                                         file_size         = 0      ,
                                         files_affected    = []     ,
                                         error_details     =''      )

    def test_zip_store(self):
        with self.routes as _:
            body       = self.test_zip
            result     = _.zip_store(body      = body               ,
                                     namespace = self.test_namespace,
                                     cache_key = None               ,
                                     file_id   = None               ,
                                     strategy  = Enum__Cache__Store__Strategy.TEMPORAL )
            cache_id   = result.cache_id
            cache_hash = result.cache_hash
            assert cache_hash           == 'd7e01b9f2e36c1bd'
            assert type(result)          is Schema__Cache__Zip__Store__Response
            assert type(result.cache_id) is Random_Guid
            assert result.namespace      == self.test_namespace
            assert result.file_count     == 2
            assert result.size           > 0
            assert result.obj()          ==  __( cache_id      = cache_id          ,
                                                 cache_hash    = cache_hash        ,
                                                 error_type    = None              ,
                                                 error_message = None              ,
                                                 success       = True              ,
                                                 namespace     = 'test-routes'     ,
                                                 paths         = __(data   = [ f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin',
                                                                               f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin.config',
                                                                               f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin.metadata'],
                                                                   by_hash = [ f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json',
                                                                               #f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config',
                                                                               f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                                                   by_id   = [ f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                                                                               f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                                                                               f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata']),
                                                 size          = 232          ,
                                                 file_count    = 2            ,
                                                 stored_at     = __SKIP__     )

            entry_by_id = self.retrieve_service.retrieve_by_id(cache_id=cache_id, namespace=self.test_namespace)

            assert type(cache_id)    is Random_Guid
            assert is_guid(cache_id) is True
            assert type(entry_by_id) is Schema__Cache__Retrieve__Success
            with entry_by_id as _:
                assert _.obj() == __(data     = self.test_zip,
                                     metadata = __(cache_id         = cache_id      ,
                                                   cache_hash       = cache_hash    ,
                                                   cache_key        =''             ,
                                                   file_id          = cache_id      ,
                                                   namespace        ='test-routes'  ,
                                                   strategy         ='temporal'     ,
                                                   stored_at        = __SKIP__      ,
                                                   file_type        ='binary'       ,
                                                   content_encoding = None          ,
                                                   content_size     = 0             ),
                                     data_type='binary')
                assert zip_bytes__file_list(_.data) == ['file1.txt', 'file2.txt']
                assert zip_bytes__files    (_.data) == {'file1.txt': b'content 1',
                                                        'file2.txt': b'content 2'}


    def test_zip_store__with_params(self):
        with self.routes as _:
            body       = self.test_zip
            cache_key  = "backups/test"
            strategy   = Enum__Cache__Store__Strategy.KEY_BASED
            file_id    = "custom-id"
            result     = _.zip_store(body      = body               ,
                                     namespace = self.test_namespace,
                                     strategy  = strategy           ,
                                     cache_key = cache_key          ,
                                     file_id   = file_id            )
            cache_id   = result.cache_id
            cache_hash = result.cache_hash

            assert cache_hash        == 'd7e01b9f2e36c1bd'
            assert type(result)      is Schema__Cache__Zip__Store__Response
            assert result.namespace  == self.test_namespace
            assert result.file_count == 2
            assert result.obj()      ==  __( cache_id      = cache_id          ,
                                             cache_hash    = cache_hash        ,
                                             error_type    = None              ,
                                             error_message = None              ,
                                             success       = True              ,
                                             namespace     = 'test-routes'     ,
                                             paths         = __(data   = [ f'{self.test_namespace}/data/key-based/{cache_key}/{file_id}.bin',
                                                                           f'{self.test_namespace}/data/key-based/{cache_key}/{file_id}.bin.config',
                                                                           f'{self.test_namespace}/data/key-based/{cache_key}/{file_id}.bin.metadata'],
                                                               by_hash = [ f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json',
                                                                           #f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config',
                                                                           f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                                               by_id   = [ f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                                                                           f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                                                                           f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata']),
                                             size          = 232          ,
                                             file_count    = 2            ,
                                             stored_at     = __SKIP__     )

    def test_zip_store__invalid_zip(self):
        with self.routes as _:
            with pytest.raises(HTTPException) as exc:
                _.zip_store(
                    body      = b"not a valid zip",
                    namespace = self.test_namespace,
                    cache_key = None,
                    file_id   = None
                )

            assert exc.value.status_code == 400
            assert "Invalid zip file" in exc.value.detail

    def test_zip_files_list(self):
        with self.routes as _:
            result = _.zip_files_list(cache_id  = self.test_cache_id ,
                                      namespace = self.test_namespace)

            assert type(result)          is Schema__Cache__Zip__Operation__Response
            assert result.success        == True
            assert result.operation      == "list"
            assert result.cache_id       == self.test_cache_id
            assert len(result.file_list) == 2
            assert "file1.txt" in result.file_list
            assert "file2.txt" in result.file_list

    def test_zip_files_list__not_found(self):
        with self.routes as _:
            with pytest.raises(HTTPException) as exc:
                _.zip_files_list(
                    cache_id  = Random_Guid(),
                    namespace = self.test_namespace
                )

            assert exc.value.status_code == 404
            assert "Zip file not found" in exc.value.detail

    def test_zip_file_retrieve(self):
        with self.routes as _:
            result = _.zip_file_retrieve(
                cache_id  = self.test_cache_id,
                file_path = Safe_Str__File__Path("file1.txt"),
                namespace = self.test_namespace
            )

            assert type(result)      is Response
            assert result.body       == b"content 1"
            assert result.media_type == "application/octet-stream"

    def test_zip_file_retrieve__not_found(self):
        with self.routes as _:
            with pytest.raises(HTTPException) as exc:
                _.zip_file_retrieve(
                    cache_id  = self.test_cache_id,
                    file_path = Safe_Str__File__Path("missing.txt"),
                    namespace = self.test_namespace
                )

            assert exc.value.status_code == 404
            assert "not found in zip" in exc.value.detail

    def test_zip_file_add_from_bytes(self):
        with self.routes as _:
            result = _.zip_file_add_from_bytes(
                cache_id  = self.test_cache_id,
                body      = b"new content",
                file_path = Safe_Str__File__Path("new.txt"),
                namespace = self.test_namespace
            )

            assert type(result)             is Schema__Cache__Zip__Operation__Response
            assert result.success           == True
            assert result.operation         == "add"
            assert result.cache_id          != self.test_cache_id  # NEW ID!
            assert result.original_cache_id == self.test_cache_id
            assert result.files_affected    == [Safe_Str__File__Path("new.txt")]

    def test_zip_file_add_from_string(self):                                              # Test the string version
        with self.routes as _:
            result = _.zip_file_add_from_string(
                cache_id  = self.test_cache_id,
                body      = "string content",
                file_path = Safe_Str__File__Path("string.txt"),
                namespace = self.test_namespace
            )

            assert type(result)             is Schema__Cache__Zip__Operation__Response
            assert result.success           == True
            assert result.operation         == "add"
            assert result.cache_id          != self.test_cache_id  # NEW ID!
            assert result.original_cache_id == self.test_cache_id
            assert result.files_affected    == [Safe_Str__File__Path("string.txt")]

    def test_zip_file_add_from_bytes__no_path(self):
        with self.routes as _:
            with pytest.raises(HTTPException) as exc:
                _.zip_file_add_from_bytes(
                    cache_id  = self.test_cache_id,
                    body      = b"content",
                    file_path = None,
                    namespace = self.test_namespace
                )

            assert exc.value.status_code == 400
            assert "file_path required" in exc.value.detail

    def test_zip_file_delete(self):
        with self.routes as _:
            result = _.zip_file_delete(
                cache_id  = self.test_cache_id,
                file_path = Safe_Str__File__Path("file1.txt"),
                namespace = self.test_namespace
            )

            assert type(result)             is Schema__Cache__Zip__Operation__Response
            assert result.success           == True
            assert result.operation         == "remove"
            assert result.cache_id          != self.test_cache_id  # NEW ID!
            assert result.original_cache_id == self.test_cache_id
            assert result.files_affected    == [Safe_Str__File__Path("file1.txt")]

    def test_batch_operations(self):
        with self.routes as _:
            operations = [Schema__Zip__Batch__Operation(action  = "add",
                                                        path    = Safe_Str__File__Path("new.txt"),
                                                        content = b"new content"),
                Schema__Zip__Batch__Operation(action = "remove",
                                              path   = Safe_Str__File__Path("file1.txt"))]

            request = Schema__Cache__Zip__Batch__Request(
                cache_id   = self.test_cache_id,
                operations = operations,
                namespace  = self.test_namespace,
                atomic     = False
            )

            result = _.batch_operations(
                request   = request,
                cache_id  = self.test_cache_id,
                namespace = self.test_namespace
            )

            assert type(result)              is Schema__Cache__Zip__Batch__Response
            assert result.success            == True
            assert result.cache_id           != self.test_cache_id  # NEW ID!
            assert result.original_cache_id  == self.test_cache_id
            assert result.operations_applied == 2
            assert "new.txt" in result.files_added
            assert "file1.txt" in result.files_removed

    def test_batch_operations__error_handling(self):
        with self.routes as _:
            # Create request with failing operations
            operations = [
                Schema__Zip__Batch__Operation(action = "remove",
                                              path   = Safe_Str__File__Path("nonexistent.txt")
                )
            ]

            request = Schema__Cache__Zip__Batch__Request(cache_id   = Random_Guid(),  # Invalid ID
                                                        operations = operations,
                                                        namespace  = self.test_namespace)

            result = _.batch_operations(request   = request            ,
                                        cache_id  = request.cache_id   ,
                                        namespace = self.test_namespace)
            assert type(result) is Schema__Cache__Zip__Batch__Response
            assert result.obj() == __( cache_id           = request.cache_id,
                                       original_cache_id  = request.cache_id,
                                       rollback_performed = False,
                                       success            = False,
                                       operations_applied = 0,
                                       operations_failed  = 0,
                                       operation_results  = [],
                                       files_added        = [],
                                       files_removed      = [],
                                       files_modified     = [],
                                       new_file_count     = 0,
                                       new_size           = 0,
                                       completed_at       = __SKIP__,
                                       error_message      = 'Zip file not found in cache')

    def test_zip_retrieve(self):
        with self.routes as _:
            result = _.zip_retrieve(cache_id  = self.test_cache_id,
                                    namespace = self.test_namespace)

            assert type(result)      is Response
            assert result.body       == self.test_zip
            assert result.media_type == "application/zip"
            assert f"{self.test_cache_id}.zip" in result.headers["Content-Disposition"]

    def test_zip_retrieve__not_found(self):
        with self.routes as _:
            with pytest.raises(HTTPException) as exc:
                _.zip_retrieve(
                    cache_id  = Random_Guid(),
                    namespace = self.test_namespace
                )

            assert exc.value.status_code == 404
            assert "Zip file not found" in exc.value.detail

    def test_zip_retrieve__not_binary(self):
        # Store non-binary data
        store_service = Cache__Service__Store(cache_service=self.cache_service)

        # Store string data
        result = store_service.store_string(
            data      = "not a zip",
            namespace = self.test_namespace
        )

        with self.routes as _:
            with pytest.raises(HTTPException) as exc:
                _.zip_retrieve(
                    cache_id  = result.cache_id,
                    namespace = self.test_namespace
                )

            assert exc.value.status_code == 400
            assert "not a binary file" in exc.value.detail

    def test_zip_file_add_from_string__round_trip(self):                                  # Test complete round-trip: add string → retrieve → verify
        with self.routes as _:
            # Add a string file to the test zip
            test_string = "Hello, this is a test string with special chars: 你好 🎉 & < > \" '"
            file_path   = Safe_Str__File__Path("test_string.txt")

            result = _.zip_file_add_from_string(cache_id  = self.test_cache_id  ,
                                                body      = test_string         ,
                                                file_path = file_path           ,
                                                namespace = self.test_namespace )

            assert result.success           == True
            assert result.operation         == "add"
            assert result.cache_id          != self.test_cache_id                         # New cache ID created
            assert result.original_cache_id == self.test_cache_id
            assert file_path in result.files_affected

            new_cache_id = result.cache_id

            # Retrieve the file we just added
            retrieve_result = _.zip_file_retrieve(cache_id  = new_cache_id       ,
                                                  file_path = file_path          ,
                                                  namespace = self.test_namespace)

            assert type(retrieve_result)    is Response
            assert retrieve_result.body     == test_string.encode('utf-8')               # String was encoded as UTF-8

            # Verify the file is in the list
            list_result = _.zip_files_list(cache_id  = new_cache_id,
                                           namespace = self.test_namespace)

            assert file_path in list_result.file_list
            assert len(list_result.file_list) == 3                                        # Original 2 + new 1

    def test_zip_file_add_from_string__multiple_files(self):                              # Test adding multiple string files
        with self.routes as _:
            files_to_add = { "config.json"    : '{"setting": "value", "number": 42}'      ,
                             "readme.md"      : "# Test Project\n\nThis is a test."       ,
                             "script.py"      : "def hello():\n    print('Hello, World!')",
                             "data.csv"       : "name,value\ntest1,100\ntest2,200"        }

            current_cache_id = self.test_cache_id

            # Add each file sequentially
            for file_name, content in files_to_add.items():
                result = _.zip_file_add_from_string(
                    cache_id  = current_cache_id,
                    body      = content,
                    file_path = Safe_Str__File__Path(file_name),
                    namespace = self.test_namespace
                )

                assert result.success    == True
                current_cache_id         = result.cache_id                                # Update to new cache ID

            # Verify all files are present
            list_result = _.zip_files_list(
                cache_id  = current_cache_id,
                namespace = self.test_namespace
            )

            assert len(list_result.file_list) == 6                                        # Original 2 + added 4
            for file_name in files_to_add.keys():
                assert Safe_Str__File__Path(file_name) in list_result.file_list

            # Verify each file's content
            for file_name, expected_content in files_to_add.items():
                retrieve_result = _.zip_file_retrieve(
                    cache_id  = current_cache_id,
                    file_path = Safe_Str__File__Path(file_name),
                    namespace = self.test_namespace
                )

                assert retrieve_result.body == expected_content.encode('utf-8')

    def test_zip_file_add_from_string__empty_string(self):                                # Test adding empty string file
        with self.routes as _:

            result = _.zip_file_add_from_string(cache_id  = self.test_cache_id              ,
                                                body      = ""                              ,                                                           # Empty string
                                                file_path = Safe_Str__File__Path("empty.txt"),
                                                namespace = self.test_namespace             )

            assert result.success == True

            # Retrieve and verify empty file
            retrieve_result = _.zip_file_retrieve(
                cache_id  = result.cache_id,
                file_path = Safe_Str__File__Path("empty.txt"),
                namespace = self.test_namespace
            )

            assert retrieve_result.body == b""                                            # Empty bytes

    def test_zip_file_add_from_string__overwrite_existing(self):                          # Test overwriting existing file with string content
        with self.routes as _:
            # First verify original content
            original_retrieve = _.zip_file_retrieve(
                cache_id  = self.test_cache_id,
                file_path = Safe_Str__File__Path("file1.txt"),
                namespace = self.test_namespace
            )
            assert original_retrieve.body == b"content 1"

            # Add file with same name but different content
            new_content = "This is the new content for file1.txt"
            result = _.zip_file_add_from_string(
                cache_id  = self.test_cache_id,
                body      = new_content,
                file_path = Safe_Str__File__Path("file1.txt"),                           # Same filename
                namespace = self.test_namespace
            )

            assert result.success == True
            new_cache_id = result.cache_id

            # Retrieve and verify new content
            new_retrieve = _.zip_file_retrieve(
                cache_id  = new_cache_id,
                file_path = Safe_Str__File__Path("file1.txt"),
                namespace = self.test_namespace
            )

            assert new_retrieve.body == new_content.encode('utf-8')                       # New content

            # Verify original is unchanged
            original_still_same = _.zip_file_retrieve(
                cache_id  = self.test_cache_id,                                           # Original cache ID
                file_path = Safe_Str__File__Path("file1.txt"),
                namespace = self.test_namespace
            )
            assert original_still_same.body == b"content 1"                               # Original unchanged

    def test_zip_file_add_from_string__nested_paths(self):                                # Test adding string files in nested directories
        with self.routes as _:
            nested_files = { "docs/readme.md"         : "# Documentation"                     ,
                             "src/main.py"            : "if __name__ == '__main__':\n    pass",
                             "src/utils/helpers.py"   : "def helper():\n    return 42"        ,
                             "tests/test_main.py"     : "import unittest\n# Tests here"       }

            current_cache_id = self.test_cache_id

            # Add nested files
            for file_path, content in nested_files.items():
                result = _.zip_file_add_from_string(
                    cache_id  = current_cache_id,
                    body      = content,
                    file_path = Safe_Str__File__Path(file_path),
                    namespace = self.test_namespace
                )
                assert result.success == True
                current_cache_id = result.cache_id

            # Verify all nested files are retrievable
            for file_path, expected_content in nested_files.items():
                retrieve_result = _.zip_file_retrieve(
                    cache_id  = current_cache_id,
                    file_path = Safe_Str__File__Path(file_path),
                    namespace = self.test_namespace
                )
                assert retrieve_result.body == expected_content.encode('utf-8')

            # Verify file list shows nested structure
            list_result = _.zip_files_list(
                cache_id  = current_cache_id,
                namespace = self.test_namespace
            )

            for file_path in nested_files.keys():
                assert Safe_Str__File__Path(file_path) in list_result.file_list

    def test_zip_file_add_from_string__large_content(self):                               # Test adding large string content
        with self.routes as _:
            # Create a large string (1MB)
            large_content = "x" * (1024 * 1024)                                           # 1MB of 'x' characters
            file_path     = Safe_Str__File__Path("large_file.txt")

            result = _.zip_file_add_from_string(cache_id  = self.test_cache_id  ,
                                                body      = large_content       ,
                                                file_path = file_path           ,
                                                namespace = self.test_namespace )

            assert result.success == True

            # Retrieve and verify size
            retrieve_result = _.zip_file_retrieve(
                cache_id  = result.cache_id,
                file_path = file_path,
                namespace = self.test_namespace
            )

            assert len(retrieve_result.body) == 1024 * 1024                               # Correct size
            assert retrieve_result.body      == large_content.encode('utf-8')             # Correct content

    def test_zip_file_add_from_string__vs_add_from_bytes(self):                           # Compare string vs bytes methods
        with self.routes as _:
            test_content = "Test content for comparison"

            # Add using string method
            string_result = _.zip_file_add_from_string(
                cache_id  = self.test_cache_id,
                body      = test_content,
                file_path = Safe_Str__File__Path("from_string.txt"),
                namespace = self.test_namespace
            )

            # Add using bytes method
            bytes_result = _.zip_file_add_from_bytes(
                cache_id  = string_result.cache_id,                                       # Chain from string result
                body      = test_content.encode('utf-8'),
                file_path = Safe_Str__File__Path("from_bytes.txt"),
                namespace = self.test_namespace
            )

            final_cache_id = bytes_result.cache_id

            # Retrieve both files
            string_retrieve = _.zip_file_retrieve(
                cache_id  = final_cache_id,
                file_path = Safe_Str__File__Path("from_string.txt"),
                namespace = self.test_namespace
            )

            bytes_retrieve = _.zip_file_retrieve(
                cache_id  = final_cache_id,
                file_path = Safe_Str__File__Path("from_bytes.txt"),
                namespace = self.test_namespace
            )

            # Both should have identical content
            assert string_retrieve.body == bytes_retrieve.body
            assert string_retrieve.body == test_content.encode('utf-8')