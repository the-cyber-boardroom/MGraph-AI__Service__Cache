import pytest
from unittest                                                                         import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                               import skip__if_not__in_github_actions
from memory_fs.path_handlers.Path__Handler__Temporal                                  import Path__Handler__Temporal
from osbot_utils.testing.__                                                           import __, __SKIP__
from osbot_utils.utils.Objects                                                        import base_classes
from osbot_utils.utils.Zip                                                            import zip_bytes_empty, zip_bytes__add_file
from mgraph_ai_service_cache.service.cache.Cache__Service                             import Cache__Service
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store             import Cache__Service__Zip__Store
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Request     import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Response    import Schema__Cache__Zip__Store__Response
from osbot_utils.type_safe.Type_Safe                                                  import Type_Safe



class test_Cache__Service__Zip__Store(TestCase):

    @classmethod
    def setUpClass(cls):                                                                # One-time expensive setup
        cls.cache_service = Cache__Service()                                           # Create cache service once
        cls.service = Cache__Service__Zip__Store(cache_service=cls.cache_service)      # Create zip store service

        # Create test zip files once
        cls.empty_zip = zip_bytes_empty()
        cls.test_zip = zip_bytes__add_file(cls.empty_zip, "test.txt", b"test content")
        cls.multi_file_zip = cls.test_zip
        cls.multi_file_zip = zip_bytes__add_file(cls.multi_file_zip, "file2.txt"    , b"content 2"     )
        cls.multi_file_zip = zip_bytes__add_file(cls.multi_file_zip, "dir/file3.txt", b"nested content")
        cls.path_now       = Path__Handler__Temporal().path_now()

    def setUp(self):                                                                   # Per-test lightweight setup
        self.test_namespace = "test-zip"                                              # Fresh namespace for isolation

    def test__init__(self):                                                           # Test service initialization
        with Cache__Service__Zip__Store() as _:
            assert type(_)         is Cache__Service__Zip__Store
            assert base_classes(_) == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service                            # Verify cache service initialized

    def test_store_zip(self):                                                         # Test storing a zip file
        with self.service as _:
            request = Schema__Cache__Zip__Store__Request(zip_bytes = self.test_zip      ,
                                                         namespace = self.test_namespace)

            result     = _.store_zip(request)
            cache_hash = _.calculate_zip_content_hash(request.zip_bytes)
            cache_id = result.cache_id

            # Verify response type and structure
            assert type(result)         is Schema__Cache__Zip__Store__Response
            assert result.cache_id      is not None                                        # ID generated
            assert result.cache_hash    == cache_hash                                      # Hash calculated
            assert result.namespace     == self.test_namespace
            assert result.file_count    == 1                                             # One file in test zip
            assert result.size          > 0
            assert result.obj() == __( cache_id      = cache_id          ,
                                       cache_hash    = cache_hash        ,
                                       namespace     = 'test-zip'        ,
                                       error_type    = None              ,
                                       error_message = None              ,
                                       success       = True              ,
                                       paths         = __(data   = [ f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin',
                                                                     f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin.config',
                                                                     f'{self.test_namespace}/data/temporal/{self.path_now}/{cache_id}.bin.metadata'],
                                                         by_hash = [ f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json',
                                                                     f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.config',
                                                                     f'{self.test_namespace}/refs/by-hash/{cache_hash[0:2]}/{cache_hash[2:4]}/{cache_hash}.json.metadata'],
                                                         by_id   = [ f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json',
                                                                     f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.config',
                                                                     f'{self.test_namespace}/refs/by-id/{cache_id[0:2]}/{cache_id[2:4]}/{cache_id}.json.metadata']),
                                       size          = 128          ,
                                       file_count    = 1            ,
                                       stored_at     = __SKIP__     )

    def test_store_zip__with_cache_key(self):                                         # Test storing with semantic key
        with self.service as _:
            request = Schema__Cache__Zip__Store__Request(zip_bytes = self.multi_file_zip       ,
                                                         cache_key = "backups/2024/january.zip",
                                                         namespace = self.test_namespace       )

            result = _.store_zip(request)

            assert result.cache_id is not None
            assert result.file_count == 3                                             # Three files in multi-file zip
            # Cache key is used for hash calculation in underlying service

    def test_store_zip__invalid_zip(self):                                            # Test error handling for invalid zip
        with self.service as _:
            request = Schema__Cache__Zip__Store__Request(zip_bytes = b"not a valid zip file",
                                                         namespace = self.test_namespace    )

            result = _.store_zip(request)
            assert result.obj() == __(cache_id       = None                                       ,
                                      cache_hash     = None                                       ,
                                      error_type     = 'INVALID_ZIP_FORMAT'                       ,
                                      error_message  = 'Invalid zip file: File is not a zip file' ,
                                      success        = False                                      ,
                                      namespace      = 'test-zip'                                 ,
                                      paths          = __()                                       ,
                                      size           = 0                                          ,
                                      file_count     = 0                                          ,
                                      stored_at      = __SKIP__                                   )

    def test_store_zip__empty_bytes(self):                                            # Test error for empty input
        with self.service as _:
            request = Schema__Cache__Zip__Store__Request(zip_bytes = b""                ,
                                                         namespace = self.test_namespace)


            result = _.store_zip(request)
            assert result.obj() == __(error_type    = 'INVALID_INPUT'                          ,
                                      error_message = 'Zip bytes cannot be empty'              ,
                                      success       = False                                    ,
                                      cache_id      = None                                     ,
                                      cache_hash    = None                                     ,
                                      namespace     = 'test-zip'                               ,
                                      paths         = __()                                     ,
                                      size          = 0                                        ,
                                      file_count    = 0                                        ,
                                      stored_at     = __SKIP__                            )

    def test_store_zip__empty_zip_file(self):                                         # Test storing empty zip (valid)
        with self.service as _:
            request = Schema__Cache__Zip__Store__Request(zip_bytes = self.empty_zip     ,
                                                         namespace = self.test_namespace)

            result = _.store_zip(request)

            assert result.file_count == 0                                             # Empty but valid zip
            assert result.size > 0                                                    # Empty zip still has structure


    def test_store_and_retrieve_roundtrip(self):                                      # Test full storage and retrieval
        with self.service as _:
            # Store the zip
            request = Schema__Cache__Zip__Store__Request(zip_bytes = self.multi_file_zip,
                                                         namespace = self.test_namespace)

            store_result = _.store_zip(request)

            retrieved = _.cache_service.retrieve_by_id(cache_id  = store_result.cache_id,           # Retrieve it back
                                                       namespace = self.test_namespace  )

            assert retrieved is not None
            assert retrieved['data_type'] == 'binary'                                               # Stored as binary
            assert retrieved['data'     ] == self.multi_file_zip                                    # Content preserved

    def test_store_zip__different_strategies(self):                                   # Test different storage strategies
        skip__if_not__in_github_actions()
        with self.service as _:
            strategies = ["direct", "temporal", "temporal_latest", "temporal_versioned"]

            for strategy in strategies:
                request = Schema__Cache__Zip__Store__Request(zip_bytes = self.test_zip       ,
                                                             namespace = f"test-{strategy}"  ,
                                                             strategy  = strategy            )

                result = _.store_zip(request)
                assert result.cache_id is not None                                    # All strategies should work
                assert result.paths is not None                                       # Paths created