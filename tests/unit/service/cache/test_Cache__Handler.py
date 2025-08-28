from unittest                                                                   import TestCase
from datetime                                                                   import datetime
from osbot_aws.testing.Temp__Random__AWS_Credentials                            import OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID, OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION
from osbot_aws.utils.AWS_Sanitization                                           import str_to_valid_s3_bucket_name
from osbot_utils.type_safe.Type_Safe                                            import Type_Safe
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path  import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id              import Safe_Id
from osbot_utils.utils.Misc                                                     import random_string_short
from osbot_utils.utils.Objects                                                  import base_classes, __
from osbot_aws.AWS_Config                                                       import aws_config
from memory_fs.file_fs.File_FS                                                  import File_FS
from memory_fs.file_types.Memory_FS__File__Type__Json                           import Memory_FS__File__Type__Json
from memory_fs.file_types.Memory_FS__File__Type__Text                           import Memory_FS__File__Type__Text
from memory_fs.helpers.Memory_FS__Latest                                        import Memory_FS__Latest
from memory_fs.helpers.Memory_FS__Temporal                                      import Memory_FS__Temporal
from memory_fs.helpers.Memory_FS__Latest_Temporal                               import Memory_FS__Latest_Temporal
from mgraph_ai_service_cache.service.cache.Cache__Handler                       import Cache__Handler
from mgraph_ai_service_cache.service.storage.Storage_FS__S3                     import Storage_FS__S3
from tests.unit.Service__Fast_API__Test_Objs                                    import setup__service_fast_api_test_objs


class test_Cache__Handler(TestCase):                                                 # Test generic cache handler implementation

    @classmethod
    def setUpClass(cls):                                                             # ONE-TIME expensive setup
        cls.test_objs   = setup__service_fast_api_test_objs()
        cls.test_bucket = str_to_valid_s3_bucket_name(random_string_short("test-handler-"))
        cls.test_prefix = "test-cache"

        assert aws_config.account_id () == OSBOT_AWS__LOCAL_STACK__AWS_ACCOUNT_ID
        assert aws_config.region_name() == OSBOT_AWS__LOCAL_STACK__AWS_DEFAULT_REGION

        with Cache__Handler() as _:
            cls.handler       = _
            _.s3__bucket      = cls.test_bucket
            _.s3__prefix      = cls.test_prefix
            _.cache_ttl_hours = 12
            _.setup()

            # Test data
            cls.test_file_id = Safe_Id("test-document")
            cls.test_data    = {"content": "test data", "version": 1}
            cls.path_now     = _.fs__temporal.handler__temporal.path_now()           # get the current temporal path from the handler

    @classmethod
    def tearDownClass(cls):                                                          # ONE-TIME cleanup
        with cls.handler.s3__storage.s3 as _:
            if _.bucket_exists(cls.test_bucket):
                _.bucket_delete_all_files(cls.test_bucket)
                _.bucket_delete(cls.test_bucket)

    # def tearDown(self):                                                              # PER-TEST cleanup
    #     self.handler.clear_all()

    def test__init__(self):                                                          # Test auto-initialization
        with Cache__Handler() as _:
            assert type(_)             is Cache__Handler
            assert base_classes(_)     == [Type_Safe, object]
            assert _.s3__bucket        == ""
            assert _.s3__prefix        == ""
            assert _.s3__storage       is None
            assert _.fs__latest        is None
            assert _.fs__temporal      is None
            assert _.fs__latest_temporal is None
            assert _.cache_ttl_hours   == 24                                         # Default TTL

    def test_setup(self):                                                            # Test cache handler setup
        with self.handler as _:
            assert type(_.s3__storage)         is Storage_FS__S3
            assert type(_.fs__latest)          is Memory_FS__Latest
            assert type(_.fs__temporal)        is Memory_FS__Temporal
            assert type(_.fs__latest_temporal) is Memory_FS__Latest_Temporal

            assert _.s3__storage.s3_bucket     == self.test_bucket
            assert _.s3__storage.s3_prefix     == self.test_prefix
            assert _.cache_ttl_hours           == 12                                 # Custom TTL

    def test_file_for_latest(self):                                                  # Test latest pattern file creation
        with self.handler as _:
            file_fs = _.file_for_latest(self.test_file_id)

            assert type(file_fs)                            is File_FS
            assert type(file_fs.file__config.file_type)     is Memory_FS__File__Type__Json
            assert file_fs.file__config.file_id             == self.test_file_id
            assert file_fs.storage_fs                       == _.s3__storage
            assert 'latest' in file_fs.file__config.file_paths[0]

    def test_file_for_latest__with_custom_type(self):                                # Test with custom file type
        with self.handler as _:
            file_fs = _.file_for_latest(self.test_file_id, Memory_FS__File__Type__Text)

            assert type(file_fs.file__config.file_type) is Memory_FS__File__Type__Text
            assert file_fs.file__config.file_type.file_extension == 'txt'

    def test_file_for_temporal(self):                                                # Test temporal pattern file creation
        with self.handler as _:
            file_fs       = _.file_for_temporal(self.test_file_id)
            assert type(file_fs)                    is File_FS
            assert file_fs.file__config.file_id     == self.test_file_id
            assert file_fs.file__config.file_paths  == [self.path_now]               # Should have temporal path (year/month/day)

    def test_file_for_latest_temporal(self):                                         # Test combined pattern file creation
        with self.handler as _:
            file_fs = _.file_for_latest_temporal(self.test_file_id)

            assert type(file_fs)                        is File_FS
            assert len(file_fs.file__config.file_paths) == 2                         # Both latest and temporal
            assert 'latest'                             in file_fs.file__config.file_paths[0]
            assert str(datetime.now().year)             in file_fs.file__config.file_paths[1]

    def test__latest_pattern_workflow(self):                                         # Test complete latest pattern workflow
        with self.handler as _:
            file_fs = _.file_for_latest(self.test_file_id)

            assert file_fs.create()                                                  # Create and save data
            assert file_fs.update(self.test_data)
            assert file_fs.exists() is True

            assert file_fs.content() == self.test_data                               # Verify retrieval

            assert file_fs.delete()                                                  # Clean up
            assert file_fs.exists() is False

    def test__temporal_pattern_workflow(self):                                       # Test complete temporal pattern workflow
        with self.handler as _:
            file_fs = _.file_for_temporal(self.test_file_id)

            # Create and save data
            assert file_fs.create()
            assert file_fs.update(self.test_data)
            assert file_fs.exists() is True

            # Verify content
            assert file_fs.content() == self.test_data

            # Clean up
            assert file_fs.delete()
            assert file_fs.exists() is False

    def test__latest_temporal_pattern_workflow(self):                                # Test combined pattern workflow
        with self.handler as _:
            file_fs = _.file_for_latest_temporal(self.test_file_id)

            # Create and save data
            created_paths = file_fs.create()
            assert len(created_paths) > 0

            updated_paths = file_fs.update(self.test_data)
            assert len(updated_paths) > 0
            assert file_fs.exists() is True

            # Verify content
            assert file_fs.content() == self.test_data

            # Both latest and temporal should exist
            assert created_paths == [ Safe_Str__File__Path(f'{self.path_now}/test-document.json'            ),
                                      Safe_Str__File__Path(f'{self.path_now}/test-document.json.config'     ),
                                      Safe_Str__File__Path(f'{self.path_now}/test-document.json.metadata'   ),
                                      Safe_Str__File__Path('latest/test-document.json'                      ),
                                      Safe_Str__File__Path('latest/test-document.json.config'               ),
                                      Safe_Str__File__Path('latest/test-document.json.metadata'             )]

            # Clean up
            deleted_paths = file_fs.delete()
            assert len(deleted_paths) > 0
            assert file_fs.exists() is False