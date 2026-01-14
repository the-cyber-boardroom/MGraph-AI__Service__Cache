from unittest                                                                            import TestCase
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__Exists__Response   import Schema__Cache__Exists__Response
from osbot_utils.testing.__                                                              import __
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                       import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.utils.Objects                                                           import base_classes
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Exists                   import Routes__File__Exists, TAG__ROUTES_EXISTS, PREFIX__ROUTES_EXISTS, BASE_PATH__ROUTES_EXISTS, ROUTES_PATHS__EXISTS
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from tests.unit.Service__Cache__Test_Objs                                                import setup__service__cache__test_objs


class test_Routes__File__Exists(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service__cache__test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.routes             = Routes__File__Exists(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace     = Safe_Str__Id("test-exists")                         # Use different namespace for test-specific data
        cls.test_hash          = Safe_Str__Cache_Hash("0000000000000000")             # Known non-existent hash
        cls.test_cache_id      = Cache_Id()                                          # Random non-existent cache_id

    def test__init__(self):                                                           # Test initialization
        with Routes__File__Exists() as _:
            assert type(_)               is Routes__File__Exists
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_EXISTS
            assert _.prefix              == PREFIX__ROUTES_EXISTS
            assert type(_.cache_service) is Cache__Service

            # Use .obj() for comprehensive comparison
            assert _.obj() ==__(tag                ='exists',
                                prefix             = '/{namespace}',
                                router             = 'APIRouter',
                                route_registration = __(analyzer        = __(),
                                                        converter       = __(),
                                                        wrapper_creator = __(converter=__()),
                                                        route_parser    = __()),
                                cache_service = __(cache_config=__(storage_mode      ='memory',
                                                                   default_bucket    = None   ,
                                                                   default_ttl_hours = 24     ,
                                                                   local_disk_path   = None   ,
                                                                   sqlite_path       = None   ,
                                                                   zip_path          = None   ),
                                                   cache_handlers    = __()                                     ,
                                                   hash_config       = __(algorithm='sha256', length=16)        ,
                                                   hash_generator    = __(config=__(algorithm='sha256', length=16))),
                                app          = None ,
                                filter_tag   = True )

    def test__class_constants(self):                                                  # Test module-level constants
        assert TAG__ROUTES_EXISTS       == 'exists'
        assert PREFIX__ROUTES_EXISTS    == '/{namespace}'
        assert BASE_PATH__ROUTES_EXISTS == '/{namespace}/exists/'
        assert ROUTES_PATHS__EXISTS     == ['/{namespace}/exists/hash/{cache_hash}',
                                            '/{namespace}/exists/id/{cache_id}'       ]

    def test_exists__id__cache_id(self):                                                  # Test cache_id existence check using fixtures
        with self.routes as _:
            # Use a fixture that we know exists
            fixture_id = self.cache_fixtures.get_fixture_id("string_simple")

            # Check it exists in fixtures namespace
            result = _.exists__id__cache_id(cache_id  = fixture_id              ,
                                            namespace = self.fixtures_namespace)

            assert type(result)    is Schema__Cache__Exists__Response
            assert result.exists   is True
            assert result.cache_id == fixture_id
            assert result.namespace == self.fixtures_namespace

            # Check non-existent cache_id
            non_existent = _.exists__id__cache_id(cache_id  = self.test_cache_id,
                                                  namespace = self.test_namespace)

            assert type(non_existent)    is Schema__Cache__Exists__Response
            assert non_existent.exists   is False
            assert non_existent.cache_id == self.test_cache_id
            assert non_existent.namespace == self.test_namespace

    def test_exists__hash__cache_hash(self):                                          # Test hash existence check using fixtures
        with self.routes as _:
            # Use a fixture that we know exists
            fixture_hash = self.cache_fixtures.get_fixture_hash("string_simple")

            # Check it exists in fixtures namespace
            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                namespace  = self.fixtures_namespace)

            assert type(result)      is Schema__Cache__Exists__Response
            assert result.exists     is True
            assert result.cache_hash == fixture_hash
            assert result.namespace  == self.fixtures_namespace

            # Check non-existent hash
            non_existent = _.exists__hash__cache_hash(cache_hash = self.test_hash      ,
                                                      namespace  = self.test_namespace)

            assert type(non_existent)      is Schema__Cache__Exists__Response
            assert non_existent.exists     is False
            assert non_existent.cache_hash == self.test_hash
            assert non_existent.namespace  == self.test_namespace


    def test_exists__id__cache_id__multiple_namespaces(self):                             # Test namespace isolation for cache_id using fixtures
        with self.routes as _:
            # Get a fixture id that exists in the fixtures namespace
            fixture_id = self.cache_fixtures.get_fixture_id("json_simple")

            # Check exists in fixtures namespace
            result_fixtures = _.exists__id__cache_id(cache_id  = fixture_id              ,
                                                    namespace = self.fixtures_namespace)
            assert result_fixtures.exists is True

            # Check doesn't exist in test namespace
            result_test = _.exists__id__cache_id(cache_id  = fixture_id        ,
                                             namespace = self.test_namespace)
            assert result_test.exists is False

    def test_exists__hash__cache_hash__multiple_namespaces(self):                     # Test namespace isolation for hash using fixtures
        with self.routes as _:
            # Get a fixture hash that exists in the fixtures namespace
            fixture_hash = self.cache_fixtures.get_fixture_hash("json_simple")

            # Check exists in fixtures namespace
            result_fixtures = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                         namespace  = self.fixtures_namespace)
            assert result_fixtures.exists is True

            # Check doesn't exist in test namespace
            result_test = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                     namespace  = self.test_namespace)
            assert result_test.exists is False

    def test_type_enforcement(self):                                                  # Test type safety of parameters
        with self.routes as _:
            # Valid Safe_Str__Cache_Hash
            valid_hash = Safe_Str__Cache_Hash("abc0123456789def")
            result = _.exists__hash__cache_hash(cache_hash = valid_hash        ,
                                                namespace  = self.test_namespace)
            assert type(result)      is Schema__Cache__Exists__Response
            assert result.cache_hash == valid_hash

            # Valid Cache_Id
            valid_cache_id = Cache_Id()
            result = _.exists__id__cache_id(cache_id  = valid_cache_id    ,
                                            namespace = self.test_namespace)
            assert type(result)    is Schema__Cache__Exists__Response
            assert result.cache_id == valid_cache_id

            # Valid Safe_Str__Id namespace
            valid_namespace = Safe_Str__Id("valid-namespace")
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash    ,
                                                namespace  = valid_namespace   )
            assert result.namespace == valid_namespace

            # Type_Safe should auto-convert compatible types
            # String to Safe_Str__Cache_Hash (auto-conversion)
            result = _.exists__hash__cache_hash(cache_hash = "aaa0123456789ccc",
                                                namespace  = self.test_namespace)
            assert result.cache_hash == "aaa0123456789ccc"                             # Auto-converted

            # String to Safe_Str__Id (auto-conversion)
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash    ,
                                                namespace  = "string-namespace")
            assert result.namespace == "string-namespace"                              # Auto-converted

    def test__integration_with_cache_service__using_fixtures(self):                   # Test integration with fixtures data
        with self.routes as _:
            # Test with all fixture types
            fixtures_to_test = ["string_simple", "json_complex", "binary_small"]

            for fixture_name in fixtures_to_test:
                fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)
                fixture_id   = self.cache_fixtures.get_fixture_id(fixture_name)

                # Check exists by hash in fixtures namespace
                hash_result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                         namespace  = self.fixtures_namespace)
                assert hash_result.exists is True

                # Check exists by cache_id in fixtures namespace
                id_result = _.exists__id__cache_id(cache_id  = fixture_id              ,
                                                namespace = self.fixtures_namespace)
                assert id_result.exists is True

                # Verify we can retrieve the data too
                retrieve_result = self.cache_service.retrieve_by_id(fixture_id, self.fixtures_namespace)
                assert retrieve_result is not None

    def test__edge_cases_with_fixtures(self):                                         # Test edge cases using fixtures
        with self.routes as _:
            # Test with empty JSON fixture
            empty_json_hash = self.cache_fixtures.get_fixture_hash("json_empty")
            empty_json_id   = self.cache_fixtures.get_fixture_id("json_empty")

            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(empty_json_hash),
                                                namespace  = self.fixtures_namespace)
            assert result.exists is True

            result = _.exists__id__cache_id(cache_id  = empty_json_id          ,
                                            namespace = self.fixtures_namespace)
            assert result.exists is True

            # Test with large binary fixture
            large_binary_hash = self.cache_fixtures.get_fixture_hash("binary_large")
            large_binary_id   = self.cache_fixtures.get_fixture_id("binary_large")

            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(large_binary_hash),
                                                namespace  = self.fixtures_namespace)
            assert result.exists is True

            result = _.exists__id__cache_id(cache_id  = large_binary_id        ,
                                            namespace = self.fixtures_namespace)
            assert result.exists is True

    def test_exists__id__cache_id__response_schema(self):                                 # Test Schema__Cache__Exists__Response for cache_id
        with self.routes as _:
            fixture_id = self.cache_fixtures.get_fixture_id("string_simple")

            result = _.exists__id__cache_id(cache_id  = fixture_id              ,
                                            namespace = self.fixtures_namespace)

            assert type(result)      is Schema__Cache__Exists__Response
            assert result.exists     is True
            assert result.cache_id   == fixture_id
            assert result.cache_hash is None                                          # Not set for cache_id check
            assert result.namespace  == self.fixtures_namespace

    def test_exists__hash__cache_hash__response_schema(self):                         # Test Schema__Cache__Exists__Response for hash
        with self.routes as _:
            fixture_hash = self.cache_fixtures.get_fixture_hash("string_simple")

            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                namespace  = self.fixtures_namespace)

            assert type(result)      is Schema__Cache__Exists__Response
            assert result.exists     is True
            assert result.cache_id   is None                                          # Not set for hash check
            assert result.cache_hash == fixture_hash
            assert result.namespace  == self.fixtures_namespace