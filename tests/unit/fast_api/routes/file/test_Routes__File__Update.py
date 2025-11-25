from unittest                                                                            import TestCase
from fastapi                                                                             import HTTPException
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from osbot_fast_api_serverless.utils.testing.skip_tests                                  import skip__if_not__in_github_actions
from osbot_utils.testing.__                                                              import __
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.utils.Objects                                                           import base_classes
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Update                   import Routes__File__Update
from mgraph_ai_service_cache.service.cache.update.Cache__Service__Update                 import Cache__Service__Update
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Store__Response         import Schema__Cache__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Update__Response        import Schema__Cache__Update__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy     import Enum__Cache__Store__Strategy
from tests.unit.Service__Cache__Test_Objs                                                import setup__service__cache__test_objs


class test_Routes__File__Update(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service__cache__test_objs()                  # Reuse shared test objects
        cls.cache_fixtures     = cls.test_objs.cache_fixtures                         # Use shared fixtures

        # Service using fixtures bucket
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.routes             = Routes__File__Update(cache_service = cls.cache_service)

        # Use different namespace to avoid conflicts
        cls.test_namespace     = Safe_Str__Id("test-routes-update")

        # Test data versions
        cls.test_string_v1     = "original string data"
        cls.test_string_v2     = "updated string data"
        cls.test_json_v1       = {"version": 1, "status": "original"}
        cls.test_json_v2       = {"version": 2, "status": "updated"}
        cls.test_binary_v1     = bytes(range(10))
        cls.test_binary_v2     = bytes(range(20, 30))

        # Track created IDs for cleanup
        cls.created_cache_ids  = []
        cls.created_namespaces = []

    def _track_cache_id(self, response: Schema__Cache__Store__Response              # Helper to track created items
                        ) -> Schema__Cache__Store__Response:
        if response and response.cache_id:
            if response.cache_id not in self.created_cache_ids:
                self.created_cache_ids.append(response.cache_id)
            if response.namespace not in self.created_namespaces:
                self.created_namespaces.append(response.namespace)
        return response

    def _create_test_entry(self, data, strategy=Enum__Cache__Store__Strategy.TEMPORAL,  # Helper to create initial entries
                                 namespace=None):
        namespace = namespace or self.test_namespace

        if isinstance(data, bytes):                                                  # Calculate hash based on data type
            cache_hash = self.cache_service.hash_from_bytes(data)
        elif isinstance(data, dict):
            cache_hash = self.cache_service.hash_from_json(data)
        else:
            cache_hash = self.cache_service.hash_from_string(data)

        result = self.cache_service.store_with_strategy(                            # Create initial entry
            storage_data = data       ,
            cache_hash   = cache_hash ,
            strategy     = strategy   ,
            namespace    = namespace  )

        self._track_cache_id(result)
        return result

    def test__init__(self):                                                          # Test auto-initialization
        with Routes__File__Update() as _:
            assert type(_) is Routes__File__Update
            assert base_classes(_)       == [Fast_API__Routes, Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

            assert _.obj() == __(tag='update',
                                 prefix='/{namespace}/update',
                                 router='APIRouter',
                                 route_registration=__(analyzer=__(),
                                                       converter=__(),
                                                       wrapper_creator=__(converter=__()),
                                                       route_parser=__()),
                                 cache_service=__(cache_config=__(storage_mode='memory',
                                                                  default_bucket=None,
                                                                  default_ttl_hours=24,
                                                                  local_disk_path=None,
                                                                  sqlite_path=None,
                                                                  zip_path=None),
                                                  cache_handlers=__(),
                                                  hash_config=__(algorithm='sha256', length=16),
                                                  hash_generator=__(config=__(algorithm='sha256', length=16))),
                                 app=None,
                                 filter_tag=True)

    def test_update_service(self):                                                   # Test update_service property
        with self.routes as _:
            update_service = _.update_service()

            assert type(update_service) is Cache__Service__Update
            assert update_service.cache_service is _.cache_service                   # Same cache service instance

    def test_update__string(self):                                                   # Test string update route
        with self.routes as _:
            create_result = self._create_test_entry(self.test_string_v1)            # Create initial entry
            cache_id      = create_result.cache_id
            original_hash = create_result.cache_hash

            update_result = _.update__string(data      = self.test_string_v2  ,     # Update via route
                                            cache_id  = cache_id              ,
                                            namespace = self.test_namespace   )


            assert type(update_result)            is Schema__Cache__Update__Response
            assert update_result.cache_id         == cache_id                              # Same ID
            assert update_result.cache_hash       == original_hash                         # V1: hash unchanged
            assert type(update_result.cache_hash) is Safe_Str__Cache_Hash
            assert update_result.namespace        == self.test_namespace
            assert update_result.updated_content  == True                            # Content updated
            assert update_result.updated_hash     == False                           # V1: hash not updated
            assert update_result.updated_metadata == False                           # V1: metadata not updated
            assert update_result.updated_id_ref   == False                           # V1: ID ref not updated

            # Verify content updated
            retrieved = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieved['data'] == self.test_string_v2

    def test_update__string__empty_data(self):                                       # Test string update with empty data
        with self.routes as _:
            create_result = self._create_test_entry(self.test_string_v1)
            cache_id      = create_result.cache_id

            # Try to update with empty string
            try:
                _.update__string(data      = ""                 ,
                                cache_id  = cache_id           ,
                                namespace = self.test_namespace)
                assert False, "Should have raised HTTPException"
            except HTTPException as e:
                assert e.status_code              == 400
                assert e.detail['error_type']     == "INVALID_INPUT"
                assert e.detail['message']        == "String data cannot be empty"
                assert e.detail['field_name']     == "data"

    def test_update__json(self):                                                     # Test JSON update route
        with self.routes as _:
            create_result = self._create_test_entry(self.test_json_v1)              # Create initial entry
            cache_id      = create_result.cache_id
            original_hash = create_result.cache_hash

            update_result = _.update__json(data      = self.test_json_v2    ,       # Update via route
                                          cache_id  = cache_id              ,
                                          namespace = self.test_namespace   )

            assert type(update_result)      is Schema__Cache__Update__Response
            assert update_result.cache_id   == cache_id                              # Same ID
            assert update_result.cache_hash == original_hash                         # V1: hash unchanged
            assert update_result.namespace  == self.test_namespace
            assert update_result.updated_content == True

            # Verify content updated
            retrieved = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieved['data'] == self.test_json_v2

    def test_update__binary(self):                                                   # Test binary update route
        with self.routes as _:
            create_result = self._create_test_entry(self.test_binary_v1)            # Create initial entry
            cache_id      = create_result.cache_id
            original_hash = create_result.cache_hash

            update_result = _.update__binary(body      = self.test_binary_v2 ,      # Update via route
                                            cache_id  = cache_id             ,
                                            namespace = self.test_namespace  )

            assert type(update_result) is Schema__Cache__Update__Response
            assert update_result.cache_id   == cache_id                              # Same ID
            assert update_result.cache_hash == original_hash                         # V1: hash unchanged
            assert update_result.size       == len(self.test_binary_v2)
            assert update_result.updated_content == True

            # Verify content updated
            retrieved = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieved['data'] == self.test_binary_v2

    def test_update__binary__empty_data(self):                                       # Test binary update with empty data
        with self.routes as _:
            create_result = self._create_test_entry(self.test_binary_v1)
            cache_id      = create_result.cache_id

            # Try to update with empty bytes
            try:
                _.update__binary(body      = b""                ,
                                cache_id  = cache_id           ,
                                namespace = self.test_namespace)
                assert False, "Should have raised HTTPException"
            except HTTPException as e:
                assert e.status_code              == 400
                assert e.detail['error_type']     == "INVALID_INPUT"
                assert e.detail['message']        == "Binary data cannot be empty"
                assert e.detail['field_name']     == "body"

    def test__execute_update__string(self):                                          # Test _execute_update with string
        with self.routes as _:
            create_result = self._create_test_entry(self.test_string_v1)
            cache_id      = create_result.cache_id

            result = _._execute_update(cache_id  = cache_id            ,
                                      namespace = self.test_namespace  ,
                                      data      = self.test_string_v2  )

            assert type(result) is Schema__Cache__Update__Response                  # Changed type
            assert result.cache_id  == cache_id
            assert result.namespace == self.test_namespace

    def test__execute_update__json(self):                                            # Test _execute_update with JSON
        with self.routes as _:
            create_result = self._create_test_entry(self.test_json_v1)
            cache_id      = create_result.cache_id

            result = _._execute_update(cache_id  = cache_id            ,
                                      namespace = self.test_namespace  ,
                                      data      = self.test_json_v2    )

            assert type(result) is Schema__Cache__Update__Response                  # Changed type
            assert result.cache_id  == cache_id
            assert result.namespace == self.test_namespace

    def test__execute_update__binary(self):                                          # Test _execute_update with binary
        with self.routes as _:
            create_result = self._create_test_entry(self.test_binary_v1)
            cache_id      = create_result.cache_id

            result = _._execute_update(cache_id  = cache_id            ,
                                      namespace = self.test_namespace  ,
                                      data      = self.test_binary_v2  )

            assert type(result) is Schema__Cache__Update__Response                  # Changed type
            assert result.cache_id  == cache_id
            assert result.size      == len(self.test_binary_v2)

    def test__execute_update__nonexistent_entry(self):                               # Test updating non-existent entry
        with self.routes as _:
            nonexistent_id = Random_Guid()

            try:
                _._execute_update(cache_id  = nonexistent_id   ,
                                 namespace = self.test_namespace,
                                 data      = "some data"        )
                assert False, "Should have raised HTTPException"
            except HTTPException as e:
                assert e.status_code              == 500
                assert e.detail['error_type']     == "UPDATE_FAILED"
                assert e.detail['cache_id']       == str(nonexistent_id)

    def test_update__all_data_types_sequential(self):                                # Test updating through all data types sequentially
        skip__if_not__in_github_actions()
        with self.routes as _:
            create_result = self._create_test_entry(self.test_string_v1)            # Create as string
            cache_id      = create_result.cache_id
            original_hash = create_result.cache_hash

            # Update to different string
            update_1 = _.update__string(data      = "second version"     ,
                                       cache_id  = cache_id              ,
                                       namespace = self.test_namespace   )
            assert update_1.cache_id   == cache_id
            assert update_1.cache_hash == original_hash                              # V1: hash unchanged

            # Update to different string again
            update_2 = _.update__string(data      = "third version"      ,
                                       cache_id  = cache_id              ,
                                       namespace = self.test_namespace   )
            assert update_2.cache_id   == cache_id
            assert update_2.cache_hash == original_hash                              # V1: hash still unchanged

            # Verify final content
            retrieved = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieved['data'] == "third version"

    def test_update__preserves_strategy(self):                                       # Test that updates preserve storage strategy
        skip__if_not__in_github_actions()
        with self.routes as _:
            # Create with specific strategy
            create_result = self._create_test_entry(self.test_json_v1                    ,
                                                   strategy  = Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED)
            cache_id = create_result.cache_id

            # Update
            update_result = _.update__json(data      = self.test_json_v2    ,
                                          cache_id  = cache_id              ,
                                          namespace = self.test_namespace   )

            assert update_result.cache_id == cache_id

            # Verify strategy preserved
            config = self.cache_service.retrieve_by_id__config(cache_id, self.test_namespace)
            assert config.strategy == Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED

    def test_update__different_strategies(self):                                     # Test updates work with all strategies
        skip__if_not__in_github_actions()
        strategies = [Enum__Cache__Store__Strategy.DIRECT              ,
                      Enum__Cache__Store__Strategy.TEMPORAL            ,
                      Enum__Cache__Store__Strategy.TEMPORAL_LATEST     ,
                      Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED  ]

        with self.routes as _:
            for strategy in strategies:
                with self.subTest(strategy=strategy):
                    namespace = Safe_Str__Id(f"route-strat-{strategy.value}")

                    # Create with strategy
                    create_result = self._create_test_entry(self.test_string_v1,
                                                          strategy  = strategy  ,
                                                          namespace = namespace )
                    cache_id = create_result.cache_id

                    # Update via route
                    update_result = _.update__string(data      = self.test_string_v2,
                                                    cache_id  = cache_id            ,
                                                    namespace = namespace           )

                    assert update_result.cache_id   == cache_id
                    assert update_result.namespace  == namespace

                    # Verify strategy preserved
                    config = self.cache_service.retrieve_by_id__config(cache_id, namespace)
                    assert config.strategy == strategy

    def test_update__with_fixtures(self):                                            # Test updates using fixture data
        skip__if_not__in_github_actions()
        with self.routes as _:
            # Create with fixture
            fixture_v1 = self.cache_fixtures.get_fixture_data('json_simple')
            create_result = self._create_test_entry(fixture_v1)
            cache_id = create_result.cache_id

            # Update with different fixture
            fixture_v2 = self.cache_fixtures.get_fixture_data('json_complex')
            update_result = _.update__json(data      = fixture_v2        ,
                                          cache_id  = cache_id           ,
                                          namespace = self.test_namespace)

            assert update_result.cache_id == cache_id

            # Verify content is complex fixture
            retrieved = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert retrieved['data'] == fixture_v2