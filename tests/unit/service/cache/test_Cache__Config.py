import pytest
from unittest                                                                   import TestCase
from memory_fs.storage_fs.providers.Storage_FS__Local_Disk                      import Storage_FS__Local_Disk
from memory_fs.storage_fs.providers.Storage_FS__Memory                          import Storage_FS__Memory
from memory_fs.storage_fs.providers.Storage_FS__Sqlite                          import Storage_FS__Sqlite
from memory_fs.storage_fs.providers.Storage_FS__Zip                             import Storage_FS__Zip
from osbot_utils.testing.__                                                     import __
from osbot_utils.type_safe.Type_Safe                                            import Type_Safe
from osbot_utils.utils.Env                                                      import set_env, del_env
from osbot_utils.utils.Objects                                                  import base_classes
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Storage_Mode      import Enum__Cache__Storage_Mode
from mgraph_ai_service_cache.service.cache.Cache__Config                        import Cache__Config

class test_Cache__Config(TestCase):

    @classmethod
    def setUpClass(cls):                                                    # ONE-TIME setup
        cls.original_env = {}                                               # Clear any existing storage mode env vars
        env_vars = ['CACHE__SERVICE__STORAGE_MODE',
                    'AWS_ACCESS_KEY_ID',
                    'AWS_SECRET_ACCESS_KEY',
                    'CACHE__SERVICE__BUCKET_NAME',
                    'CACHE__SERVICE__LOCAL_DISK_PATH',
                    'CACHE__SERVICE__SQLITE_PATH',
                    'CACHE__SERVICE__ZIP_PATH']

        for var in env_vars:
            import os
            cls.original_env[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

    @classmethod
    def tearDownClass(cls):                                                 # Restore original environment
        import os
        for var, value in cls.original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

    def test__init__(self):                                                 # Test auto-initialization
        with Cache__Config() as _:
            assert type(_)                   is Cache__Config
            assert base_classes(_)           == [Type_Safe, object]
            assert type(_.storage_mode)     is Enum__Cache__Storage_Mode
            assert _.storage_mode            == Enum__Cache__Storage_Mode.MEMORY  # Default without AWS creds
            assert _.default_ttl_hours       == 24                               # Default TTL

            # Memory mode shouldn't have S3 config
            assert _.default_bucket          is None
            assert _.local_disk_path         is None
            assert _.sqlite_path             is None
            assert _.zip_path                is None

    def test_determine_storage_mode__explicit(self):                        # Test explicit mode configuration
        set_env('CACHE__SERVICE__STORAGE_MODE', 's3')

        with Cache__Config() as _:
            assert _.storage_mode == Enum__Cache__Storage_Mode.S3

        modes = ['memory', 'local_disk', 'sqlite', 'zip']                   # Test each explicit mode
        for mode_str in modes:
            set_env('CACHE__SERVICE__STORAGE_MODE', mode_str)
            config = Cache__Config()
            assert config.storage_mode.value == mode_str

        del_env('CACHE__SERVICE__STORAGE_MODE')

    def test_determine_storage_mode__aws_auto_detection(self):              # Test AWS credential detection
        set_env('AWS_ACCESS_KEY_ID'     , 'test-key')                       # With AWS credentials, should select S3
        set_env('AWS_SECRET_ACCESS_KEY' , 'test-secret')

        with Cache__Config() as _:
            assert _.has_aws_credentials()  is True
            assert _.storage_mode           == Enum__Cache__Storage_Mode.S3

        # Clean up
        del_env('AWS_ACCESS_KEY_ID')
        del_env('AWS_SECRET_ACCESS_KEY')

        # Without credentials, should use memory
        with Cache__Config() as _:
            assert _.has_aws_credentials()  is False
            assert _.storage_mode           == Enum__Cache__Storage_Mode.MEMORY

    def test_configure_for_storage_mode__s3(self):                         # Test S3 mode configuration
        set_env('CACHE__SERVICE__STORAGE_MODE', 's3')
        set_env('CACHE__SERVICE__BUCKET_NAME', 'test-bucket')

        with Cache__Config() as _:
            assert _.storage_mode      == Enum__Cache__Storage_Mode.S3
            assert _.default_bucket    == 'test-bucket'
            assert _.default_ttl_hours == 24

        del_env('CACHE__SERVICE__STORAGE_MODE')
        del_env('CACHE__SERVICE__BUCKET_NAME')

    def test_configure_for_storage_mode__local_disk(self):                 # Test local disk configuration
        set_env('CACHE__SERVICE__STORAGE_MODE', 'local_disk')
        set_env('CACHE__SERVICE__LOCAL_DISK_PATH', '/custom/path')

        with Cache__Config() as _:
            assert _.storage_mode       == Enum__Cache__Storage_Mode.LOCAL_DISK
            assert _.local_disk_path    == '/custom/path'
            assert _.default_bucket     is None                            # No S3 config in local mode

        del_env('CACHE__SERVICE__STORAGE_MODE')
        del_env('CACHE__SERVICE__LOCAL_DISK_PATH')

        # Test default path
        set_env('CACHE__SERVICE__STORAGE_MODE', 'local_disk')
        with Cache__Config() as _:
            assert _.local_disk_path == '/tmp/cache'                      # Default path

        del_env('CACHE__SERVICE__STORAGE_MODE')

    def test_configure_for_storage_mode__sqlite(self):                     # Test SQLite configuration
        set_env('CACHE__SERVICE__STORAGE_MODE', 'sqlite')
        set_env('CACHE__SERVICE__SQLITE_PATH', '/data/cache.db')

        with Cache__Config() as _:
            assert _.storage_mode   == Enum__Cache__Storage_Mode.SQLITE
            assert _.sqlite_path    == '/data/cache.db'

        del_env('CACHE__SERVICE__STORAGE_MODE')
        del_env('CACHE__SERVICE__SQLITE_PATH')

        # Test in-memory default
        set_env('CACHE__SERVICE__STORAGE_MODE', 'sqlite')
        with Cache__Config() as _:
            assert _.sqlite_path == ':memory:'                             # Default in-memory

        del_env('CACHE__SERVICE__STORAGE_MODE')

    def test_configure_for_storage_mode__zip(self):                        # Test ZIP configuration
        set_env('CACHE__SERVICE__STORAGE_MODE', 'zip')
        set_env('CACHE__SERVICE__ZIP_PATH', '/archives/cache.zip')

        with Cache__Config() as _:
            assert _.storage_mode == Enum__Cache__Storage_Mode.ZIP
            assert _.zip_path     == '/archives/cache.zip'

        del_env('CACHE__SERVICE__STORAGE_MODE')
        del_env('CACHE__SERVICE__ZIP_PATH')

        # Test default path
        set_env('CACHE__SERVICE__STORAGE_MODE', 'zip')
        with Cache__Config() as _:
            assert _.zip_path == '/tmp/cache.zip'                          # Default path

        del_env('CACHE__SERVICE__STORAGE_MODE')

    def test_create_storage_backend__memory(self):                         # Test memory backend creation
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY) as _:
            storage = _.create_storage_backend()

            assert storage is not None
            assert type(storage) is Storage_FS__Memory


    def test_create_storage_backend__s3(self):                             # Test S3 backend creation
        pytest.skip("move to integration test")                            # todo: do the move
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.S3,
                          default_bucket='test-bucket') as _:
            # This will fail without actual S3 setup, but we can test the attempt
            try:
                storage = _.create_storage_backend()
                from mgraph_ai_service_cache.service.storage.Storage_FS__S3 import Storage_FS__S3
                assert type(storage) is Storage_FS__S3
            except:
                pass  # Expected in test environment without S3

    def test_create_storage_backend__s3_missing_bucket(self):              # Test S3 without bucket fails
        kwargs = dict(storage_mode   = Enum__Cache__Storage_Mode.S3,
                      default_bucket = ''                        )        # Force no bucket
        with Cache__Config(**kwargs) as _:
            assert _.default_bucket == ''
            with pytest.raises(ValueError, match="S3 bucket name required"):
                _.create_storage_backend()

    def test_create_storage_backend__local_disk(self):                     # Test local disk backend
        import tempfile
        temp_dir = tempfile.mkdtemp()

        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.LOCAL_DISK,
                          local_disk_path=temp_dir) as _:
            storage = _.create_storage_backend()

            assert type(storage) is Storage_FS__Local_Disk
            assert storage.root_path == temp_dir

    def test_create_storage_backend__sqlite(self):                         # Test SQLite backend
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.SQLITE,
                          sqlite_path=':memory:') as _:
            storage = _.create_storage_backend()

            assert type(storage) is Storage_FS__Sqlite
            assert storage.in_memory is True

    def test_create_storage_backend__zip(self):                            # Test ZIP backend
        import tempfile
        temp_file = tempfile.mktemp(suffix='.zip')

        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.ZIP,
                          zip_path=temp_file) as _:
            storage = _.create_storage_backend()

            assert type(storage) is Storage_FS__Zip

    def test_create_storage_backend__unknown(self):                        # Test unknown mode fails
        with Cache__Config() as _:
            error_message = "'invalid' is not a valid Enum__Cache__Storage_Mode"
            with pytest.raises(ValueError, match=error_message):
                _.storage_mode = "invalid"                                      # Invalid mode (picked up by type safe)

    def test_get_storage_info(self):                                            # Test storage info retrieval
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY) as _: # Memory mode
            info = _.get_storage_info()
            assert info == {'storage_mode': 'memory', 'ttl_hours': 24}

        # S3 mode
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.S3,
                          default_bucket='test-bucket') as _:
            info = _.get_storage_info()
            assert info == {'storage_mode': 's3',
                           'ttl_hours': 24,
                           's3_bucket': 'test-bucket'}

        # Local disk mode
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.LOCAL_DISK,
                          local_disk_path='/custom/path') as _:
            info = _.get_storage_info()
            assert info == {'storage_mode': 'local_disk',
                           'ttl_hours': 24,
                           'local_disk_path': '/custom/path'}

    def test__obj_method(self):                                            # Test .obj() comprehensive comparison
        with Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY) as _:
            assert _.obj() == __(storage_mode      = Enum__Cache__Storage_Mode.MEMORY,
                                default_bucket    = None                              ,
                                default_ttl_hours = 24                                ,
                                local_disk_path   = None                              ,
                                sqlite_path       = None                              ,
                                zip_path          = None                              )

    def test_explicit_initialization(self):                                # Test explicit parameter setting
        config = Cache__Config(storage_mode      = Enum__Cache__Storage_Mode.S3,
                              default_bucket    = 'explicit-bucket'            ,
                              default_ttl_hours = 48                           ,
                              local_disk_path   = '/explicit/path'             )

        assert config.storage_mode      == Enum__Cache__Storage_Mode.S3
        assert config.default_bucket    == 'explicit-bucket'
        assert config.default_ttl_hours == 48
        assert config.local_disk_path   == '/explicit/path'

    def test_invalid_mode_string(self):                                    # Test invalid mode string handling
        set_env('CACHE__SERVICE__STORAGE_MODE', 'invalid_mode')

        with Cache__Config() as _:
            # Should fall back to auto-detection
            assert _.storage_mode == Enum__Cache__Storage_Mode.MEMORY     # No AWS creds, so memory

        del_env('CACHE__SERVICE__STORAGE_MODE')

    def test_has_aws_credentials__various_forms(self):                     # Test different AWS credential forms
        with Cache__Config() as _:
            # Test with access key
            set_env('AWS_ACCESS_KEY_ID', 'key')
            set_env('AWS_SECRET_ACCESS_KEY', 'secret')
            assert _.has_aws_credentials() is True
            del_env('AWS_ACCESS_KEY_ID')
            del_env('AWS_SECRET_ACCESS_KEY')

            # Test with profile
            set_env('AWS_PROFILE', 'test-profile')
            assert _.has_aws_credentials() is True
            del_env('AWS_PROFILE')

            # Test with Lambda environment
            set_env('AWS_REGION', 'us-east-1')
            set_env('AWS_LAMBDA_FUNCTION_NAME', 'test-function')
            assert _.has_aws_credentials() is True
            del_env('AWS_REGION')
            del_env('AWS_LAMBDA_FUNCTION_NAME')

            # No credentials
            assert _.has_aws_credentials() is False