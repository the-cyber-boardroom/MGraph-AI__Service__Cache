from unittest                                                                            import TestCase
from osbot_fast_api_serverless.utils.testing.skip_tests                                  import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                              import __, __SKIP__
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.utils.Objects                                                           import base_classes
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                   import Cache__Service__Store
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response                import Schema__Cache__Store__Response
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.schemas.errors.Schema__Cache__Error__Invalid_Input          import Schema__Cache__Error__Invalid_Input
from tests.unit.Service__Cache__Test_Objs                                                import setup__service__cache__test_objs


class test_Cache__Service__Store(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service__cache__test_objs()                  # Reuse shared test objects
        cls.cache_fixtures     = cls.test_objs.cache_fixtures                         # Use shared fixtures

        # Service using fixtures bucket
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.store_service      = Cache__Service__Store(cache_service = cls.cache_service)

        # Use different namespace to avoid conflicts with fixtures
        cls.test_namespace     = Safe_Str__Id("test-store-service")

        # Reuse fixture test data
        cls.test_string        = cls.cache_fixtures.get_fixture_data('string_simple')
        cls.test_json          = cls.cache_fixtures.get_fixture_data('json_complex')  # Use complex for variety
        cls.test_binary        = cls.cache_fixtures.get_fixture_data('binary_small')

        # Track created IDs for cleanup
        cls.created_cache_ids  = []
        cls.created_namespaces = []

    def _track_cache_id(self, response: Schema__Cache__Store__Response            # Helper to track created items
                        ) -> Schema__Cache__Store__Response:
        if response and response.cache_id:
            if response.cache_id not in self.created_cache_ids:
                self.created_cache_ids.append(response.cache_id)
            if response.namespace not in self.created_namespaces:
                self.created_namespaces.append(response.namespace)
        return response

    def test__init__(self):                                                          # Test auto-initialization
        with Cache__Service__Store() as _:
            assert type(_) is Cache__Service__Store
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

            assert _.obj() == __(cache_service = __(cache_config      =__(storage_mode='memory',
                                                    default_bucket    = None,
                                                    default_ttl_hours = 24  ,
                                                    local_disk_path   = None ,
                                                    sqlite_path       = None ,
                                                    zip_path          = None ),
                                                    cache_handlers    = __()                    ,
                                                    hash_config       = __(algorithm = 'sha256', length = 16),
                                                    hash_generator    = __(config = __(algorithm = 'sha256', length = 16))))

    def test_store_string(self):                                                     # Test string storage using fixture data
        with self.store_service as _:
            result = _.store_string(data      = self.test_string                    ,
                                    strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                    namespace = self.test_namespace                  )
            self._track_cache_id(result)

            assert type(result)            is Schema__Cache__Store__Response
            assert type(result.cache_id  ) is Random_Guid
            assert type(result.cache_hash) is Safe_Str__Cache_Hash
            assert result.namespace        == self.test_namespace
            assert result.size              > 0

    def test_store_string__empty(self):                                              # Test empty string handling
        with self.store_service as _:
            result = _.store_string(data      = ""                                  ,
                                   strategy  = Enum__Cache__Store__Strategy.DIRECT ,
                                   namespace = self.test_namespace                 )

            assert result is None                                                    # Service returns None for invalid

    def test_store_string__with_cache_key(self):                                     # Test with custom cache key
        with self.store_service as _:
            cache_key = Safe_Str__File__Path("custom/path/to/file")
            file_id   = Safe_Str__Id("custom-id")

            result = _.store_string(data      = self.test_string                       ,
                                    strategy  = Enum__Cache__Store__Strategy.KEY_BASED ,
                                    namespace = self.test_namespace                    ,
                                    cache_key = cache_key                              ,
                                    file_id   = file_id                                )
            self._track_cache_id(result)

            assert type(result)    is Schema__Cache__Store__Response

            expected_hash = _.cache_service.hash_from_string(cache_key)         # Hash based on cache_key not data
            assert result.cache_hash == expected_hash

    def test_store_json__using_fixture(self):                                        # Test JSON storage with fixture
        with self.store_service as _:
            result = _.store_json(data      = self.test_json                             ,
                                 strategy  = Enum__Cache__Store__Strategy.TEMPORAL_LATEST,
                                 namespace = self.test_namespace                         )
            self._track_cache_id(result)

            assert type(result)            is Schema__Cache__Store__Response
            assert type(result.cache_id  ) is Random_Guid
            assert type(result.cache_hash) is Safe_Str__Cache_Hash
            assert result.size             > 0

    def test_store_json__empty_object(self):                                         # Test empty JSON
        with self.store_service as _:
            empty_json = self.cache_fixtures.get_fixture_data('json_empty')          # Use fixture empty JSON

            result = _.store_json(data      = empty_json                           ,
                                  strategy  = Enum__Cache__Store__Strategy.DIRECT  ,
                                  namespace = self.test_namespace                  )
            self._track_cache_id(result)

            assert type(result) is Schema__Cache__Store__Response
            assert result.size  >= 2                                                 # At least {}

    def test_store_binary__using_fixture(self):                                      # Test binary with fixture data
        with self.store_service as _:
            result = _.store_binary(data      = self.test_binary                   ,
                                   strategy  = Enum__Cache__Store__Strategy.DIRECT,
                                   namespace = self.test_namespace                 )
            self._track_cache_id(result)

            assert type(result)          is Schema__Cache__Store__Response
            assert type(result.cache_id) is Random_Guid
            assert result.size           == len(self.test_binary)

    def test_store_binary__compressed(self):                                         # Test compression
        import gzip

        with self.store_service as _:
            # Use larger fixture for compression test
            original_data   = self.cache_fixtures.get_fixture_data('binary_large')
            compressed_data = gzip.compress(original_data)

            result = _.store_binary(data             = compressed_data                    ,
                                   strategy         = Enum__Cache__Store__Strategy.TEMPORAL,
                                   namespace        = self.test_namespace                 ,
                                   content_encoding = "gzip"                             )
            self._track_cache_id(result)

            assert type(result) is Schema__Cache__Store__Response
            assert result.size  < len(original_data)

    def test_get_invalid_input_error(self):                                          # Test error building
        with self.store_service as _:
            error = _.get_invalid_input_error(field_name    = Safe_Str__Id("data")        ,
                                              field_value   = ""                          ,
                                              expected_type = Safe_Str__Id("non-empty string"),
                                              message       = "Data cannot be empty"      )

            assert type(error)         is Schema__Cache__Error__Invalid_Input
            assert error.error_type    == "INVALID_INPUT"
            assert error.field_name    == "data"
            assert error.field_value   is None
            assert error.expected_type == "non-empty_string"
            assert error.message       == "Data cannot be empty"

            assert error.obj() == __(error_type    = 'INVALID_INPUT'      ,
                                     message       = 'Data cannot be empty',
                                     timestamp     = __SKIP__              ,
                                     request_id    = __SKIP__              ,
                                     field_name    = 'data'                ,
                                     field_value   = None                  ,
                                     expected_type = 'non-empty_string'    ,
                                     constraints   = None                  )

    def test_store_all_strategies(self):                                             # Test all strategies
        skip__if_not__in_github_actions()
        strategies = [Enum__Cache__Store__Strategy.DIRECT              ,
                      Enum__Cache__Store__Strategy.TEMPORAL            ,
                      Enum__Cache__Store__Strategy.TEMPORAL_LATEST     ,
                      Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED  ]

        with self.store_service as _:
            for strategy in strategies:
                with self.subTest(strategy=strategy):
                    namespace = Safe_Str__Id(f"strat-{strategy.value}")

                    result = _.store_string(data      = self.test_string,
                                           strategy  = strategy         ,
                                           namespace = namespace        )
                    self._track_cache_id(result)

                    assert type(result)          is Schema__Cache__Store__Response
                    assert type(result.cache_id) is Random_Guid

    def test_store_duplicate_data(self):                                             # Test duplicate storage
        skip__if_not__in_github_actions()
        with self.store_service as _:
            # Use fixture string for consistency
            result1 = _.store_string(data      = self.test_string                   ,
                                    strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                    namespace = self.test_namespace                  )
            self._track_cache_id(result1)

            result2 = _.store_string(data      = self.test_string                   ,
                                    strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                    namespace = self.test_namespace                  )
            self._track_cache_id(result2)

            assert result1.cache_hash  == result2.cache_hash                                  # Same hash
            assert result1.cache_id    != result2.cache_id                              # Different IDs

    def test_store_with_different_fixtures(self):                                    # Test variety of fixture data
        skip__if_not__in_github_actions()
        fixtures_to_test = [('string_medium', 'string'),
                            ('json_simple'  , 'json'  ),
                            ('binary_medium', 'binary')]

        with self.store_service as _:
            for fixture_name, data_type in fixtures_to_test:
                with self.subTest(fixture=fixture_name):
                    data = self.cache_fixtures.get_fixture_data(fixture_name)

                    if data_type == 'string':
                        result = _.store_string(data      = data                ,
                                               namespace = self.test_namespace  )
                    elif data_type == 'json':
                        result = _.store_json(data      = data                 ,
                                             namespace = self.test_namespace   )
                    else:  # binary
                        result = _.store_binary(data      = data               ,
                                               namespace = self.test_namespace )

                    self._track_cache_id(result)
                    assert type(result) is Schema__Cache__Store__Response

    def test_default_namespace(self):                                                # Test default namespace
        with self.store_service as _:
            result = _.store_string(data     = "test with default namespace"       ,
                                   strategy = Enum__Cache__Store__Strategy.DIRECT )
            self._track_cache_id(result)

            assert type(result) is Schema__Cache__Store__Response