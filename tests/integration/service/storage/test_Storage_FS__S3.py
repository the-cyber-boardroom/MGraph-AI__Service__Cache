from unittest                                                                       import TestCase
from osbot_aws.testing.Temp__Random__AWS_Credentials                                import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                               import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.utils.Misc                                                         import random_string_short
from osbot_utils.utils.Objects                                                      import base_classes, __
from osbot_aws.AWS_Config                                                           import aws_config
from memory_fs.storage_fs.Storage_FS                                                import Storage_FS
from mgraph_ai_service_cache.service.storage.Storage_FS__S3                         import Storage_FS__S3
from tests.integration.Service__Cache__Test_Objs__Integration import setup__service__cache__test_objs__integration
from tests.unit.Service__Cache__Test_Objs                                           import setup__service__cache__test_objs


class test_Storage_FS__S3(TestCase):                                                 # Test S3 storage backend implementation

    @classmethod
    def setUpClass(cls):                                                             # ONE-TIME expensive setup
        cls.test_objs   = setup__service__cache__test_objs__integration()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-storage-"))
        cls.test_prefix = "test-prefix"

        assert aws_config.account_id () == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        # Test data
        cls.test_path    = Safe_Str__File__Path("test/file.txt")
        cls.test_content = b"test content"
        cls.test_json    = {"key": "value", "number": 42, "nested": {"inner": "data"}}

        with Storage_FS__S3() as _:
            cls.storage      = _
            _.s3_bucket      = cls.test_bucket
            _.s3_prefix      = cls.test_prefix
            _.setup()

    @classmethod
    def tearDownClass(cls):                                                          # ONE-TIME cleanup
        with cls.storage.s3 as _:
            if _.bucket_exists(cls.test_bucket):
                _.bucket_delete_all_files(cls.test_bucket)
                _.bucket_delete(cls.test_bucket)

    def tearDown(self):                                                              # PER-TEST cleanup
        self.storage.clear()

    def test__setupUpClass(self):                                                    # Test setup class configuration
        assert self.test_bucket.startswith('test-storage-') is True
        with self.test_objs.local_stack as _:
            assert _.is_localstack_enabled() is True

    def test__init__(self):                                                          # Test auto-initialization
        with Storage_FS__S3() as _:
            assert type(_)         is Storage_FS__S3
            assert base_classes(_) == [Storage_FS, Type_Safe, object]
            assert _.s3_bucket     == ""                                             # Default empty bucket
            assert _.s3_prefix     == ""                                             # Default empty prefix
            assert _.s3            is None                                           # Not initialized yet

    def test__init__with_prefix(self):                                               # Test initialization with prefix
        storage = Storage_FS__S3(s3_bucket=self.test_bucket, s3_prefix=self.test_prefix)
        storage.setup()

        assert storage.s3_prefix == self.test_prefix

        # Test that files are saved with prefix
        storage.file__save(self.test_path, self.test_content)

        # Verify file exists with prefix in S3
        expected_key = f"{self.test_prefix}/{self.test_path}"
        assert storage.s3.file_exists(self.test_bucket, expected_key) is True

        # Clean up
        storage.clear()

    def test_setup(self):                                                            # Test S3 client setup
        with Storage_FS__S3() as _:
            _.s3_bucket = str_to_valid_s3_bucket_name(random_string_short("test-setup-"))
            _.setup()

            assert _.s3 is not None
            assert _.s3.bucket_exists(_.s3_bucket) is True

            _.s3.bucket_delete(_.s3_bucket)                                          # Cleanup

    def test__get_s3_key(self):                                                      # Test path to S3 key conversion
        with self.storage as _:
            path = Safe_Str__File__Path("folder/file.json")
            key  = _._get_s3_key(path)
            assert key == f"{self.test_prefix}/folder/file.json"

            # Test without prefix
            _.s3_prefix = ""
            key = _._get_s3_key(path)
            assert key == "folder/file.json"
            _.s3_prefix = self.test_prefix                                            # Restore

    def test__get_path_from_key(self):                                               # Test S3 key to path conversion
        with self.storage as _:
            s3_key = f"{self.test_prefix}/folder/file.json"
            path   = _._get_path_from_key(s3_key)
            assert type(path) is Safe_Str__File__Path
            assert str(path)  == "folder/file.json"

    def test_file__save(self):                                                       # Test saving file to S3
        path = Safe_Str__File__Path("test-file.txt")
        data = b"test content"

        with self.storage as _:
            result = _.file__save(path, data)
            assert result is True
            assert _.file__exists(path) is True


            # Verify in S3 directly
            s3_key =_._get_s3_key(path)
            assert _.s3.file_exists(_.s3_bucket, s3_key) is True


    def test_file__save_nested_path(self):                                           # Test saving with nested directories
        nested_path = Safe_Str__File__Path("folder1/folder2/folder3/file.txt")

        with self.storage as _:
            assert _.file__save(nested_path, self.test_content) is True
            assert _.file__exists(nested_path) is True
            assert _.file__bytes(nested_path) == self.test_content

    def test_file__save_update(self):                                                # Test updating existing file
        with self.storage as _:
            _.file__save(self.test_path, b"original")
            assert _.file__bytes(self.test_path) == b"original"

            _.file__save(self.test_path, b"updated")
            assert _.file__bytes(self.test_path) == b"updated"
            assert _.file__delete(self.test_path) is True

    def test_file__bytes(self):                                                      # Test reading bytes from S3
        path = Safe_Str__File__Path("test-bytes.bin")
        data = b"binary content \x00\x01\x02"

        with self.storage as _:
            assert _.file__bytes(path) is None                                       # File doesn't exist yet

            _.file__save(path, data)
            loaded = _.file__bytes(path)
            assert loaded == data
            assert type(loaded) is bytes

    def test_file__str(self):                                                        # Test reading string from S3
        path = Safe_Str__File__Path("test-str.txt")
        text = "string content æ–‡å­—"                                                # With unicode

        with self.storage as _:
            assert _.file__str(path) is None                                         # File doesn't exist yet

            _.file__save(path, text.encode('utf-8'))
            loaded = _.file__str(path)
            assert loaded == text
            assert type(loaded) is str

    def test_file__json(self):                                                       # Test JSON operations
        path = Safe_Str__File__Path("test-data.json")
        data = {"key": "value", "number": 42, "nested": {"inner": "data"}}

        with self.storage as _:
            assert _.file__json(path) is None                                        # File doesn't exist yet

            import json
            _.file__save(path, json.dumps(data).encode())
            loaded = _.file__json(path)
            assert loaded == data
            assert type(loaded) is dict

    def test_file__delete(self):                                                     # Test file deletion
        path = Safe_Str__File__Path("test-delete.txt")

        with self.storage as _:
            assert _.file__delete(path) is False                                     # Can't delete non-existent file

            _.file__save(path, b"delete me")
            assert _.file__exists(path) is True

            result = _.file__delete(path)
            assert result is True
            assert _.file__exists(path) is False

            # Delete non-existent returns False
            assert _.file__delete(path) is False

    def test_file__exists(self):                                                     # Test file existence check
        path = Safe_Str__File__Path("test-exists.txt")

        with self.storage as _:
            assert _.file__exists(path) is False

            _.file__save(path, b"now I exist")
            assert _.file__exists(path) is True

    def test_files__paths(self):                                                     # Test listing all file paths
        paths = [ Safe_Str__File__Path("file1.txt"),
                  Safe_Str__File__Path("folder/file2.txt"),
                  Safe_Str__File__Path("folder/subfolder/file3.txt")]

        with self.storage as _:
            for path in paths:
                _.file__save(path, b"content")

            all_paths = _.files__paths()
            assert len(all_paths) == 3
            assert all(type(p) is Safe_Str__File__Path for p in all_paths)
            assert sorted([str(p) for p in all_paths]) == [str(p) for p in sorted(paths)]

    def test_files__paths_with_prefix(self):                                         # Test listing with prefix
        storage = Storage_FS__S3(s3_bucket=self.test_bucket, s3_prefix="my-prefix")
        storage.setup()

        # Create files
        paths = [Safe_Str__File__Path("file1.txt"),
                 Safe_Str__File__Path("file2.txt")]

        for path in paths:
            storage.file__save(path, b"content")

        # List should return paths without prefix
        result = storage.files__paths()
        assert result == sorted(paths)

        # Verify files are actually stored with prefix
        for path in paths:
            s3_key = f"my-prefix/{path}"
            assert storage.s3.file_exists(self.test_bucket, s3_key) is True

        storage.clear()

    def test_clear(self):                                                            # Test clearing all files
        with self.storage as _:
            # Create multiple files
            _.file__save(Safe_Str__File__Path("clear1.txt"), b"data1")
            _.file__save(Safe_Str__File__Path("clear2.txt"), b"data2")
            _.file__save(Safe_Str__File__Path("folder/clear3.txt"), b"data3")

            assert len(_.files__paths()) == 3

            result = _.clear()
            assert result is True
            assert len(_.files__paths()) == 0

            # Bucket should still exist
            assert _.s3.bucket_exists(_.s3_bucket) is True

    def test_clear_with_prefix(self):                                                # Test clearing only affects prefix
        # Create storage with prefix
        storage1 = Storage_FS__S3(s3_bucket=self.test_bucket, s3_prefix="prefix1").setup()
        storage2 = Storage_FS__S3(s3_bucket=self.test_bucket, s3_prefix="prefix2").setup()

        # Add files to both prefixes
        storage1.file__save(Safe_Str__File__Path("file1.txt"), b"content1")
        storage2.file__save(Safe_Str__File__Path("file2.txt"), b"content2")

        # Clear storage1
        assert storage1.clear() is True

        # Storage1 should be empty, storage2 should still have files
        assert len(storage1.files__paths()) == 0
        assert len(storage2.files__paths()) == 1

        # Clean up
        storage2.clear()

    def test__with_nested_folders(self):                                             # Test nested folder operations
        with self.storage as _:
            paths = [ Safe_Str__File__Path("a/b/c/deep.txt"),
                      Safe_Str__File__Path("a/b/mid.txt"),
                      Safe_Str__File__Path("a/shallow.txt")]

            for path in paths:
                _.file__save(path, f"content at {path}".encode())

            all_paths = _.files__paths()
            assert len(all_paths) == 3

            # Verify all can be read back
            for path in paths:
                content = _.file__str(path)
                assert content == f"content at {path}"

    def test__error_handling(self):                                                  # Test error conditions
        with self.storage as _:
            # Read non-existent file
            assert _.file__bytes(Safe_Str__File__Path("no-such-file.txt")) is None
            assert _.file__str(Safe_Str__File__Path("no-such-file.txt")) is None
            assert _.file__json(Safe_Str__File__Path("no-such-file.json")) is None

    def test_file__metadata(self):                                                   # Test S3 metadata operations
        with self.storage as _:
            _.file__save(self.test_path, self.test_content)

            # Get metadata
            metadata = _.file__metadata(self.test_path)
            assert metadata is not None

            # Update metadata
            new_metadata = {"custom-key": "custom-value", "another-key": "another-value"}
            assert _.file__metadata_update(self.test_path, new_metadata) is True

            # Verify metadata was updated
            updated_metadata = _.file__metadata(self.test_path)
            assert updated_metadata["custom-key"] == "custom-value"
            assert updated_metadata["another-key"] == "another-value"
            assert _.file__delete(self.test_path) is True

    def test_file__copy(self):                                                       # Test file copying
        source_path = Safe_Str__File__Path("source.txt")
        dest_path = Safe_Str__File__Path("destination.txt")

        with self.storage as _:
            # Copy non-existent file should fail
            assert _.file__copy(source_path, dest_path) is False

            # Create source file
            _.file__save(source_path, self.test_content)

            # Copy file
            assert _.file__copy(source_path, dest_path) is True

            # Both files should exist with same content
            assert _.file__exists(source_path)      is True
            assert _.file__exists(dest_path)        is True
            assert _.file__bytes(source_path)       == _.file__bytes(dest_path)
            assert _.file__delete(source_path)      is True
            assert _.file__delete(dest_path)        is True

    def test_file__move(self):                                                       # Test file moving
        source_path = Safe_Str__File__Path("source-a.txt")
        dest_path   = Safe_Str__File__Path("destination-b.txt")

        with self.storage as _:
            # Move non-existent file should fail
            assert _.file__move(source_path, dest_path) is False

            # Create source file
            _.file__save(source_path, self.test_content)

            # Move file
            assert _.file__move(source_path, dest_path) is True

            # Source should not exist, destination should
            assert _.file__exists(source_path) is False
            assert _.file__exists(dest_path  ) is True
            assert _.file__bytes(dest_path   ) == self.test_content
            assert _.file__delete(dest_path  ) is True

    def test_file__size(self):                                                       # Test getting file size
        with self.storage as _:
            assert _.file__size(self.test_path) is None                              # Non-existent file

            _.file__save(self.test_path, self.test_content)
            size = _.file__size(self.test_path)

            assert size == len(self.test_content)
            assert type(size) is int
            assert _.file__delete(self.test_path) is True

    def test_file__last_modified(self):                                              # Test getting last modified time
        with self.storage as _:
            assert _.file__last_modified(self.test_path) is None                     # Non-existent file

            _.file__save(self.test_path, self.test_content)
            last_modified = _.file__last_modified(self.test_path)

            assert last_modified is not None
            assert type(last_modified) is str
            assert 'T' in last_modified                                              # ISO format check

    def test_folder__files(self):                                                    # Test listing files in folder
        with self.storage as _:
            # Create files in different folders
            _.file__save(Safe_Str__File__Path("root.txt"), b"root")
            _.file__save(Safe_Str__File__Path("folder1/file1.txt"), b"content1")
            _.file__save(Safe_Str__File__Path("folder1/file2.txt"), b"content2")
            _.file__save(Safe_Str__File__Path("folder2/file3.txt"), b"content3")

            # List files in folder1
            folder1_files = _.folder__files("folder1")
            assert len(folder1_files) == 2

            # With full path
            folder1_files_full = _.folder__files("folder1", return_full_path=True)
            assert len(folder1_files_full) == 2
            assert all("folder1" in str(path) for path in folder1_files_full)

    def test_pre_signed_url(self):                                                   # Test pre-signed URL generation
        with self.storage as _:
            _.file__save(self.test_path, self.test_content)

            # Get pre-signed URL for reading
            get_url = _.pre_signed_url(self.test_path, operation='get_object')
            assert get_url is not None
            assert self.test_bucket in get_url
            assert "AWSAccessKeyId" in get_url

            # Get pre-signed URL for writing
            put_url = _.pre_signed_url(self.test_path, operation='put_object')
            assert put_url is not None

    def test_concurrent_operations(self):                                            # Test multiple operations
        with self.storage as _:
            paths = [Safe_Str__File__Path(f"file{i}.txt") for i in range(10)]

            # Save all files
            for i, path in enumerate(paths):
                assert _.file__save(path, f"content{i}".encode()) is True

            # Verify all exist
            for path in paths:
                assert _.file__exists(path) is True

            # Delete even-numbered files
            for i in range(0, 10, 2):
                assert _.file__delete(paths[i]) is True

            # Verify correct files remain
            remaining = _.files__paths()
            assert len(remaining) == 5

            for i in range(1, 10, 2):
                assert paths[i] in remaining

    def test_large_content(self):                                                    # Test with large content
        large_content = b"x" * (1024 * 1024)                                        # 1MB
        path = Safe_Str__File__Path("large_file.bin")

        with self.storage as _:
            assert _.file__save(path, large_content) is True
            assert _.file__bytes(path) == large_content
            assert _.file__size(path) == len(large_content)

    def test_special_characters_in_path(self):                                       # Test paths with special characters
        special_paths = [Safe_Str__File__Path("file with spaces.txt"),
                         Safe_Str__File__Path("file-with-dashes.txt"),
                         Safe_Str__File__Path("file_with_underscores.txt")]

        with self.storage as _:
            for path in special_paths:
                assert _.file__save(path, b"content") is True
                assert _.file__exists(path) is True

            files__paths = _.files__paths()
            for path in special_paths:
                assert path in files__paths

    def test_empty_file(self):                                                       # Test empty file handling
        empty_path = Safe_Str__File__Path("empty.txt")

        with self.storage as _:
            assert _.file__save(empty_path, b"") is True
            assert _.file__exists(empty_path) is True
            assert _.file__bytes(empty_path) == b""
            assert _.file__str(empty_path) == ""
            assert _.file__size(empty_path) == 0

    def test_bucket_versioning(self):                                                # Test versioning check
        with self.storage as _:
            versioning_enabled = _.bucket_versioning_enabled()
            assert type(versioning_enabled) is bool

    def test_integration_with_s3_utilities(self):                                    # Test that it works with S3 utilities
        with self.storage as _:
            _.file__save(self.test_path, self.test_content)

            # Use S3 utilities directly
            s3_key = _._get_s3_key(self.test_path)
            assert _.s3.file_exists(_.s3_bucket, s3_key) is True
            assert _.s3.file_bytes (_.s3_bucket, s3_key) == self.test_content

            # Files deleted through S3 should reflect in storage
            _.s3.file_delete(_.s3_bucket, s3_key)
            assert _.file__exists(s3_key) is False