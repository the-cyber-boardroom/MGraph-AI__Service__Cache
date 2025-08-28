from unittest                                                                    import TestCase
from osbot_aws.testing.Temp__Random__AWS_Credentials                             import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                            import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                             import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path   import Safe_Str__File__Path
from osbot_utils.utils.Misc                                                      import random_string_short
from osbot_utils.utils.Objects                                                   import base_classes, __
from osbot_aws.AWS_Config                                                        import aws_config
from memory_fs.storage_fs.Storage_FS                                             import Storage_FS
from mgraph_ai_service_cache.service.cache.Storage_FS__S3                        import Storage_FS__S3
from tests.unit.Service__Fast_API__Test_Objs                                     import setup__service_fast_api_test_objs


class test_Storage_FS__S3(TestCase):                                                 # Test S3 storage backend implementation

    @classmethod
    def setUpClass(cls):                                                             # ONE-TIME expensive setup
        cls.test_objs   = setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-storage-"))
        cls.test_prefix = "test-prefix"

        assert aws_config.account_id () == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

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

    def test__init__(self):                                                          # Test auto-initialization
        with Storage_FS__S3() as _:
            assert type(_)         is Storage_FS__S3
            assert base_classes(_) == [Storage_FS, Type_Safe, object]
            assert _.s3_bucket     == ""                                             # Default empty bucket
            assert _.s3_prefix     == ""                                             # Default empty prefix
            assert _.s3            is None                                           # Not initialized yet

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

    def test_file__bytes(self):                                                      # Test reading bytes from S3
        path = Safe_Str__File__Path("test-bytes.bin")
        data = b"binary content \x00\x01\x02"

        with self.storage as _:
            _.file__save(path, data)
            loaded = _.file__bytes(path)
            assert loaded == data
            assert type(loaded) is bytes

    def test_file__str(self):                                                        # Test reading string from S3
        path = Safe_Str__File__Path("test-str.txt")
        text = "string content"

        with self.storage as _:
            _.file__save(path, text.encode())
            loaded = _.file__str(path)
            assert loaded == text
            assert type(loaded) is str

    def test_file__json(self):                                                       # Test JSON operations
        path = Safe_Str__File__Path("test-data.json")
        data = {"key": "value", "number": 42, "nested": {"inner": "data"}}

        with self.storage as _:
            import json
            _.file__save(path, json.dumps(data).encode())
            loaded = _.file__json(path)
            assert loaded == data
            assert type(loaded) is dict

    def test_file__delete(self):                                                     # Test file deletion
        path = Safe_Str__File__Path("test-delete.txt")

        with self.storage as _:
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