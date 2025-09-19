from unittest                                                                            import TestCase
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.utils.Objects                                                           import base_classes, __
from mgraph_ai_service_cache.fast_api.routes.Routes__Exists                              import Routes__Exists, TAG__ROUTES_EXISTS, PREFIX__ROUTES_EXISTS, BASE_PATH__ROUTES_EXISTS, ROUTES_PATHS__EXISTS
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from tests.unit.Service__Fast_API__Test_Objs                                             import setup__service_fast_api_test_objs


class test_Routes__Exists(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service_fast_api_test_objs()
        cls.cache_fixtures     = cls.test_objs.cache_fixtures
        cls.fixtures_bucket    = cls.cache_fixtures.fixtures_bucket
        cls.fixtures_namespace = cls.cache_fixtures.namespace
        cls.cache_service      = Cache__Service(default_bucket=cls.fixtures_bucket)
        cls.routes             = Routes__Exists(cache_service=cls.cache_service)

        # Test data
        cls.test_namespace     = Safe_Str__Id("test-exists")                         # Use different namespace for test-specific data
        cls.test_hash          = Safe_Str__Cache_Hash("0000000000000000")           # Known non-existent hash

    def test__init__(self):                                                           # Test initialization
        with Routes__Exists() as _:
            assert type(_)               is Routes__Exists
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert _.tag                 == TAG__ROUTES_EXISTS
            assert _.prefix              == PREFIX__ROUTES_EXISTS
            assert type(_.cache_service) is Cache__Service

            # Use .obj() for comprehensive comparison
            assert _.obj() ==__(tag           ='exists',
                                prefix        = '/{namespace}',
                                router        = 'APIRouter',
                                cache_service = __(default_bucket    = 'mgraph-ai-cache',
                                                   default_ttl_hours = 24,
                                                   cache_handlers    = __(),
                                                   hash_config       = __(algorithm='sha256', length=16),
                                                   hash_generator    = __(config=__(algorithm='sha256', length=16))),
                                app          = None ,
                                filter_tag   = True )

    def test__class_constants(self):                                                  # Test module-level constants
        assert TAG__ROUTES_EXISTS       == 'exists'
        assert PREFIX__ROUTES_EXISTS    == '/{namespace}'
        assert BASE_PATH__ROUTES_EXISTS == '/{namespace}/exists/'
        assert ROUTES_PATHS__EXISTS     == ['/{namespace}/exists/hash/{cache_hash}']

    def test_exists__hash__cache_hash(self):                                          # Test hash existence check using fixtures
        with self.routes as _:
            # Use a fixture that we know exists
            fixture_hash = self.cache_fixtures.get_fixture_hash("string_simple")
            #fixture_id   = self.cache_fixtures.get_fixture_id("string_simple")

            # Check it exists in fixtures namespace
            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                namespace  = self.fixtures_namespace)

            assert result == {"exists"    : True                    ,
                             "hash"      : fixture_hash             ,
                             "namespace" : str(self.fixtures_namespace)}

            # Check non-existent hash
            non_existent = _.exists__hash__cache_hash(cache_hash = self.test_hash      ,
                                                      namespace  = self.test_namespace)

            assert non_existent == {"exists"    : False                   ,
                                   "hash"      : str(self.test_hash)     ,
                                   "namespace" : str(self.test_namespace)}

    def test_exists__hash__cache_hash__default_namespace(self):                       # Test default namespace handling
        with self.routes as _:
            # Test with None namespace (should use default)
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash,
                                                namespace  = None          )

            assert result == {"exists"    : False           ,
                             "hash"      : str(self.test_hash),
                             "namespace" : "default"        }

    def test_exists__hash__cache_hash__multiple_namespaces(self):                     # Test namespace isolation using fixtures
        with self.routes as _:
            # Get a fixture hash that exists in the fixtures namespace
            fixture_hash = self.cache_fixtures.get_fixture_hash("json_simple")

            # Check exists in fixtures namespace
            result_fixtures = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                         namespace  = self.fixtures_namespace)
            assert result_fixtures["exists"] is True

            # Check doesn't exist in test namespace
            result_test = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                     namespace  = self.test_namespace)
            assert result_test["exists"] is False

    def test_type_enforcement(self):                                                  # Test type safety of parameters
        with self.routes as _:
            # Valid Safe_Str__Cache_Hash
            valid_hash = Safe_Str__Cache_Hash("abc0123456789def")
            result = _.exists__hash__cache_hash(cache_hash = valid_hash        ,
                                                namespace  = self.test_namespace)
            assert type(result) is dict
            assert result["hash"] == str(valid_hash)

            # Valid Safe_Str__Id namespace
            valid_namespace = Safe_Str__Id("valid-namespace")
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash    ,
                                                namespace  = valid_namespace   )
            assert result["namespace"] == str(valid_namespace)

            # Type_Safe should auto-convert compatible types
            # String to Safe_Str__Cache_Hash (auto-conversion)
            result = _.exists__hash__cache_hash(cache_hash = "stringhash123456",
                                                namespace  = self.test_namespace)
            assert result["hash"] == "stringhash123456"                              # Auto-converted

            # String to Safe_Str__Id (auto-conversion)
            result = _.exists__hash__cache_hash(cache_hash = self.test_hash    ,
                                                namespace  = "string-namespace")
            assert result["namespace"] == "string-namespace"                         # Auto-converted

    def test__integration_with_cache_service__using_fixtures(self):                   # Test integration with fixtures data
        with self.routes as _:
            # Test with all fixture types
            fixtures_to_test = ["string_simple", "json_complex", "binary_small"]

            for fixture_name in fixtures_to_test:
                with self.subTest(fixture=fixture_name):
                    fixture_hash = self.cache_fixtures.get_fixture_hash(fixture_name)

                    # Check exists in fixtures namespace
                    check_result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(fixture_hash),
                                                              namespace  = self.fixtures_namespace)
                    assert check_result["exists"] is True

                    # Verify we can retrieve the data too
                    fixture_id = self.cache_fixtures.get_fixture_id(fixture_name)
                    retrieve_result = self.cache_service.retrieve_by_id(fixture_id, self.fixtures_namespace)
                    assert retrieve_result is not None

    def test__edge_cases_with_fixtures(self):                                         # Test edge cases using fixtures
        with self.routes as _:
            # Test with empty JSON fixture
            empty_json_hash = self.cache_fixtures.get_fixture_hash("json_empty")

            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(empty_json_hash),
                                                namespace  = self.fixtures_namespace)
            assert result["exists"] is True

            # Test with large binary fixture
            large_binary_hash = self.cache_fixtures.get_fixture_hash("binary_large")

            result = _.exists__hash__cache_hash(cache_hash = Safe_Str__Cache_Hash(large_binary_hash),
                                                namespace  = self.fixtures_namespace)
            assert result["exists"] is True