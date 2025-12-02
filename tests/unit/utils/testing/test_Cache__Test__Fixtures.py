from unittest                                                                       import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                             import skip__if_not__in_github_actions
from osbot_utils.type_safe.Type_Safe                                                import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                  import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from osbot_utils.utils.Objects                                                      import base_classes
from mgraph_ai_service_cache.service.cache.Cache__Service                           import Cache__Service
from mgraph_ai_service_cache.utils.testing.Cache__Test__Fixtures                    import Cache__Test__Fixtures
from tests.unit.Service__Cache__Test_Objs                                           import setup__service__cache__test_objs


class test_Cache__Test__Fixtures(TestCase):

    @classmethod
    def setUpClass(cls):
        #skip__if_not__in_github_actions()
        cls.test_obj      = setup__service__cache__test_objs()                                              # LocalStack setup
        cls.cache_service = Cache__Service()
        cls.test_fixtures = Cache__Test__Fixtures(namespace     = Safe_Str__Id("test-fixtures-test"),
                                                  cache_service = cls.cache_service).setup()


    # @classmethod
    # def tearDownClass(cls):                                                           # ONE-TIME cleanup
    #     cls.test_fixtures.cleanup_all()                                                 # Clean up test fixtures


        # # Clean up test bucket
        # cache_service = Cache__Service(default_bucket=cls.test_bucket)
        # handler       = cache_service.get_or_create_handler(Safe_Str__Id("test-fixtures-test"))
        # with handler.s3__storage.s3 as s3:
        #     if s3.bucket_exists(cls.test_bucket):
        #         s3.bucket_delete_all_files(cls.test_bucket)
        #         s3.bucket_delete          (cls.test_bucket)

    def test_00__init__(self):                                                           # Test Type_Safe inheritance and initialization
        with Cache__Test__Fixtures() as _:
            assert type(_)               is Cache__Test__Fixtures
            assert base_classes(_)       == [Type_Safe, object]

            # Verify default initialization
            assert _.cache_service       is None                                      # Not initialized until setup()
            assert _.manifest_cache_id   is None                                      # Will be set during setup
            assert _.namespace           == 'fixtures-namespace'                                      # Will default to "test-fixtures"
            assert _.fixtures            == {}
            assert _.setup_completed     is False
            assert _.manifest            is None
            assert _.delete_on_exit      is False

            # Verify default fixtures are defined
            assert 'string_simple'   in _.default_fixtures
            assert 'json_simple'     in _.default_fixtures
            assert 'binary_small'    in _.default_fixtures

    def test_01_verify_fixtures(self):                                                   # Test fixture verification
        skip__if_not__in_github_actions()
        with self.test_fixtures as _:                                                    # this test needs to run first
            # All fixtures should exist
            assert _.verify_fixtures() is True

            # Delete one fixture manually
            fixture_info = _.fixtures['string_simple']
            cache_id = Cache_Id(fixture_info['cache_id'])
            _.cache_service.delete_by_id(cache_id  = cache_id,
                                         namespace = _.namespace)

            # Verification should now fail
            assert _.verify_fixtures() is False

            # Recreate fixtures
            _.fixtures = {}                                                           # Clear fixture map
            _.create_fixtures()
            assert _.verify_fixtures() is True



    def test_setup(self):                                                             # Test fixture setup and initialization
        skip__if_not__in_github_actions()
        with Cache__Test__Fixtures(cache_service = Cache__Service()          ,
                                   namespace     = Safe_Str__Id("test-setup")) as _:
            _.setup()

            # Verify setup completed
            assert _.setup_completed     is True
            assert type(_.cache_service) is Cache__Service

            # Verify manifest ID was set
            assert type(_.manifest_cache_id) is Cache_Id
            assert str(_.manifest_cache_id)  == "00000000-0000-0000-0000-000000000001"  # Predictable ID

            # Verify namespace was used
            assert _.namespace == Safe_Str__Id("test-setup")

            # Clean up
            _.delete_on_exit = True
            _.cleanup_all()

    def test_create_fixtures(self):                                                   # Test fixture creation
        with self.test_fixtures as _:

            # Verify fixtures were created
            assert len(_.fixtures) == len(_.default_fixtures)

            # Check specific fixture
            string_fixture = _.fixtures.get('string_simple')
            assert string_fixture        is not None
            assert 'cache_id'            in string_fixture
            assert 'cache_hash'          in string_fixture
            assert 'type'                in string_fixture
            assert string_fixture['type'] == 'str'

            # Verify different types
            json_fixture = _.fixtures.get('json_simple')
            assert json_fixture['type'] == 'dict'

            binary_fixture = _.fixtures.get('binary_small')
            assert binary_fixture['type'] == 'bytes'

    def test_get_fixture_data(self):                                                  # Test retrieving fixture data
        with self.test_fixtures as _:

            # Retrieve string fixture
            string_data = _.get_fixture_data('string_simple')
            assert string_data == "test retrieve string data"

            # Retrieve JSON fixture
            json_data = _.get_fixture_data('json_simple')
            assert json_data == {"key": "value", "number": 42}

            # Retrieve binary fixture
            binary_data = _.get_fixture_data('binary_small')
            assert binary_data == b'\x01\x02\x03\x04\x05'

            # Non-existent fixture
            missing_data = _.get_fixture_data('non_existent')
            assert missing_data is None

    def test_get_fixture_id(self):                                                    # Test getting fixture IDs
        with self.test_fixtures as _:

            # Get ID for existing fixture
            fixture_id = _.get_fixture_id('string_simple')
            assert type(fixture_id) is Cache_Id

            # Use ID to retrieve directly
            result = _.cache_service.retrieve_by_id(cache_id  = fixture_id,
                                                   namespace = _.namespace)
            assert result.get("data") == "test retrieve string data"

            # Non-existent fixture
            missing_id = _.get_fixture_id('non_existent')
            assert missing_id is None

    def test_get_fixture_hash(self):                                                  # Test getting fixture hashes
        with self.test_fixtures as _:

            # Get hash for existing fixture
            fixture_hash = _.get_fixture_hash('string_simple')
            assert fixture_hash is not None
            assert len(fixture_hash) == 16                                            # Default hash length

            # Verify hash matches calculated hash
            expected_hash = _.cache_service.hash_from_string("test retrieve string data")
            assert fixture_hash == str(expected_hash)

    def test_cleanup_all(self):                                                  # Test fixture cleanup
        skip__if_not__in_github_actions()
        namespace = Safe_Str__Id("test-cleanup")

        with Cache__Test__Fixtures(cache_service  = Cache__Service(),
                                   delete_on_exit = True            ,
                                   namespace      = namespace       ) as _:
            _.setup()

            # Verify fixtures exist
            assert len(_.fixtures) > 0
            fixture_id = _.get_fixture_id('string_simple')

            # Clean up
            _.cleanup_all()

            # Verify fixtures are gone
            assert _.fixtures == {}
            assert _.manifest == {}

            # Verify data was deleted
            result = _.cache_service.retrieve_by_id(cache_id  = fixture_id,
                                                    namespace = namespace)
            assert result is None

    def test_complex_json_fixture(self):                                              # Test complex JSON preservation
        with self.test_fixtures as _:
            _.setup()

            complex_json = _.get_fixture_data('json_complex')

            # Verify nested structure preserved
            assert complex_json['nested']['data']        == [1, 2, 3]
            assert complex_json['nested']['more']['level'] == 3
            assert complex_json['flag']                  is True
            assert complex_json['count']                 == 100
            assert complex_json['tags']                  == ["test", "fixture", "cache"]

    def test_binary_sizes(self):                                                      # Test different binary sizes
        with self.test_fixtures as _:
            _.setup()

            small_binary  = _.get_fixture_data('binary_small')
            medium_binary = _.get_fixture_data('binary_medium')
            large_binary  = _.get_fixture_data('binary_large')

            assert len(small_binary)  == 5
            assert len(medium_binary) == 100
            assert len(large_binary)  == 1024                                         # 256 * 4

            # Verify content integrity
            assert small_binary == b'\x01\x02\x03\x04\x05'
            assert medium_binary[0] == 0
            assert medium_binary[99] == 99