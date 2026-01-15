from unittest                                                                            import TestCase
from osbot_utils.testing.__                                                              import __, __SKIP__
from osbot_utils.testing.__helpers                                                       import obj
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.utils.Objects                                                           import base_classes
from memory_fs.path_handlers.Path__Handler__Temporal                                     import Path__Handler__Temporal
from mgraph_ai_service_cache.service.cache.update.Cache__Service__Update                 import Cache__Service__Update
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Store__Response         import Schema__Cache__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Update__Response        import Schema__Cache__Update__Response
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy     import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__File__Refs         import Schema__Cache__File__Refs
from tests.unit.Service__Cache__Test_Objs                                                import setup__service__cache__test_objs


class test_Cache__Service__Update(TestCase):

    @classmethod
    def setUpClass(cls):                                                              # ONE-TIME expensive setup
        cls.test_objs          = setup__service__cache__test_objs()                  # Reuse shared test objects
        cls.cache_fixtures     = cls.test_objs.cache_fixtures                         # Use shared fixtures

        # Service using fixtures bucket
        cls.cache_service      = cls.cache_fixtures.cache_service
        cls.update_service     = Cache__Service__Update(cache_service = cls.cache_service)

        # Use different namespace to avoid conflicts with fixtures
        cls.test_namespace     = Safe_Str__Id("test-update-service")
        cls.path_now           = Path__Handler__Temporal().path_now()

        # Reuse fixture test data
        cls.test_string_v1     = cls.cache_fixtures.get_fixture_data('string_simple')
        cls.test_string_v2     = "updated string data for testing"
        cls.test_json_v1       = cls.cache_fixtures.get_fixture_data('json_simple')
        cls.test_json_v2       = {"updated": True, "version": 2, "count": 100}
        cls.test_binary_v1     = cls.cache_fixtures.get_fixture_data('binary_small')
        cls.test_binary_v2     = bytes(range(10, 20))

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
                                 namespace=None, cache_key=None, file_id=None):
        namespace = namespace or self.test_namespace

        if isinstance(data, bytes):                                                  # Determine data type and calculate hash
            cache_hash = self.cache_service.hash_from_bytes(data)
        elif isinstance(data, dict):
            cache_hash = self.cache_service.hash_from_json(data)
        else:
            cache_hash = self.cache_service.hash_from_string(data)

        result = self.cache_service.store_with_strategy(                            # Create entry
            storage_data = data       ,
            cache_hash   = cache_hash ,
            strategy     = strategy   ,
            namespace    = namespace  ,
            cache_key    = cache_key  ,
            file_id      = file_id    )

        self._track_cache_id(result)
        return result

    def test__init__(self):                                                          # Test auto-initialization
        with Cache__Service__Update() as _:
            assert type(_) is Cache__Service__Update
            assert base_classes(_)       == [Type_Safe, object]
            assert type(_.cache_service) is Cache__Service

            assert _.obj() == __(cache_service = __(cache_config      =__(storage_mode      = 'memory',
                                                                          default_bucket    = None    ,
                                                                          default_ttl_hours = 24      ,
                                                                          local_disk_path   = None    ,
                                                                          sqlite_path       = None    ,
                                                                          zip_path          = None    ),
                                                    cache_handlers    = __()                               ,
                                                    hash_config       = __(algorithm = 'sha256', length = 16),
                                                    hash_generator    = __(config = __(algorithm = 'sha256', length = 16))))

    def test_update_by_id__string(self):                                             # Test updating string data
        with self.update_service as _:
            create_result    = self._create_test_entry(self.test_string_v1)            # Create initial entry
            cache_id         = create_result.cache_id
            cache_hash       = create_result.cache_hash
            namespace        = self.test_namespace

            retrieved_before = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)     # get the file contents before the update

            update_result = _.update_by_id(cache_id  = cache_id             ,        # Update entry
                                           namespace = namespace            ,
                                           data      = self.test_string_v2  )

            assert type(update_result) is Schema__Cache__Update__Response
            assert update_result.obj() == __(cache_id         = cache_id    ,
                                             cache_hash       = cache_hash  ,
                                             namespace        = namespace   ,
                                             paths            = [f'{namespace}/data/temporal/{self.path_now}/{cache_id}.json'],
                                             size             = 31          ,
                                             timestamp        = __SKIP__    ,
                                             updated_content  = True        ,
                                             updated_hash     = False       ,
                                             updated_metadata = False       ,
                                             updated_id_ref   = False       )

            assert update_result.cache_id         == cache_id                        # Same ID
            assert update_result.cache_hash       == cache_hash                      # Same hash
            assert type(update_result.cache_hash) is Safe_Str__Cache_Hash
            assert update_result.namespace        == self.test_namespace
            assert update_result.size             > 0


            # Verify content updated
            retrieved_after = self.cache_service.retrieve_by_id(cache_id, self.test_namespace)
            assert type(retrieved_after) is dict        # todo: refacator the retrieve_by_id to return a type safe class
            assert retrieved_after['data'] == self.test_string_v2

            assert retrieved_before['metadata'] == retrieved_after['metadata']
            assert obj(retrieved_before)        == __( data               = 'test retrieve string data'                        ,
                                                       metadata           = __(cache_hash        = cache_hash                 ,
                                                                               cache_key         = ''                         ,
                                                                               cache_id          = cache_id                   ,
                                                                               content_encoding  = None                       ,
                                                                               file_id           = cache_id                   ,
                                                                               file_type         = 'json'                     ,
                                                                               json_field_path   = None                       ,
                                                                               namespace         = 'test-update-service'      ,
                                                                               stored_at         = __SKIP__                   ,
                                                                               strategy          = 'temporal')                ,
                                                       data_type          = 'string'                                          ,
                                                       content_encoding   = None                                              )

            assert obj(retrieved_after)          == __(data               = 'updated string data for testing'                 ,
                                                       metadata           = __(cache_hash        = cache_hash                 ,
                                                                               cache_key         = ''                         ,
                                                                               cache_id          = cache_id                   ,
                                                                               content_encoding  = None                       ,
                                                                               file_id           = cache_id                   ,
                                                                               file_type         = 'json'                     ,
                                                                               json_field_path   = None                       ,
                                                                               namespace         = 'test-update-service'      ,
                                                                               stored_at         = __SKIP__                   ,
                                                                               strategy          = 'temporal')                ,
                                                       data_type          = 'string'                                          ,
                                                       content_encoding   = None                                              )




    def test_update_by_id__json(self):                                               # Test updating JSON data
        with self.update_service as _:
            create_result = self._create_test_entry(self.test_json_v1)              # Create initial entry
            cache_id      = create_result.cache_id
            cache_hash    = create_result.cache_hash
            namespace     = self.test_namespace

            retrieved_before = self.cache_service.retrieve_by_id(cache_id, namespace)

            update_result = _.update_by_id(cache_id  = cache_id            ,        # Update entry
                                           namespace = namespace            ,
                                           data      = self.test_json_v2    )

            assert type(update_result) is Schema__Cache__Update__Response
            assert update_result.obj() == __(cache_id         = cache_id    ,
                                             cache_hash       = cache_hash  ,
                                             namespace        = namespace   ,
                                             paths            = [f'{namespace}/data/temporal/{self.path_now}/{cache_id}.json'],
                                             size             = __SKIP__    ,
                                             timestamp        = __SKIP__    ,
                                             updated_content  = True        ,
                                             updated_hash     = False       ,
                                             updated_metadata = False       ,
                                             updated_id_ref   = False       )

            # Verify content updated
            retrieved_after = self.cache_service.retrieve_by_id(cache_id, namespace)
            assert retrieved_after['data'] == self.test_json_v2
            assert retrieved_before['metadata'] == retrieved_after['metadata']       # Metadata unchanged

    def test_update_by_id__binary(self):                                             # Test updating binary data
        with self.update_service as _:
            create_result = self._create_test_entry(self.test_binary_v1)            # Create initial entry
            cache_id      = create_result.cache_id
            cache_hash    = create_result.cache_hash
            namespace     = self.test_namespace

            retrieved_before = self.cache_service.retrieve_by_id(cache_id, namespace)

            update_result = _.update_by_id(cache_id  = cache_id            ,        # Update entry
                                           namespace = namespace            ,
                                           data      = self.test_binary_v2  )

            assert type(update_result) is Schema__Cache__Update__Response
            assert update_result.obj() == __(cache_id         = cache_id    ,
                                             cache_hash       = cache_hash  ,
                                             namespace        = namespace   ,
                                             paths            = [f'{namespace}/data/temporal/{self.path_now}/{cache_id}.bin'],
                                             size             = 10          ,        # len(bytes(range(10, 20)))
                                             timestamp        = __SKIP__    ,
                                             updated_content  = True        ,
                                             updated_hash     = False       ,
                                             updated_metadata = False       ,
                                             updated_id_ref   = False       )

            # Verify content updated
            retrieved_after = self.cache_service.retrieve_by_id(cache_id, namespace)
            assert retrieved_after['data'] == self.test_binary_v2
            assert retrieved_before['metadata'] == retrieved_after['metadata']       # Metadata unchanged

    def test_update_by_id__nonexistent_entry(self):                                  # Test updating non-existent entry
        with self.update_service as _:
            nonexistent_id = Random_Guid()

            result = _.update_by_id(cache_id  = nonexistent_id     ,
                                   namespace = self.test_namespace ,
                                   data      = "some data"         )

            assert result is None                                                    # Returns None for non-existent

    def test_update_by_id__preserves_strategy(self):                                 # Test that update preserves original strategy
        with self.update_service as _:
            # Create entry with KEY_BASED strategy
            cache_key     = Safe_Str__File__Path("test/path/to/file")
            file_id       = Safe_Str__Id("test-file-id")
            create_result = self._create_test_entry(self.test_string_v1         ,
                                                   strategy  = Enum__Cache__Store__Strategy.KEY_BASED,
                                                   cache_key = cache_key         ,
                                                   file_id   = file_id           )
            cache_id = create_result.cache_id

            # Update entry
            update_result = _.update_by_id(cache_id  = cache_id            ,
                                          namespace = self.test_namespace  ,
                                          data      = self.test_string_v2  )

            assert update_result.cache_id == cache_id



            refs    = self.cache_service.retrieve_by_id__refs(cache_id, self.test_namespace)
            assert refs.strategy   == Enum__Cache__Store__Strategy.KEY_BASED
            assert refs.cache_id    == cache_id

    def test_update_by_id__multiple_updates(self):                                   # Test multiple consecutive updates
        with self.update_service as _:
            create_result = self._create_test_entry(self.test_json_v1)              # Create initial entry
            cache_id      = create_result.cache_id
            cache_hash    = create_result.cache_hash
            namespace     = self.test_namespace

            # First update
            update_v1 = {"version": 2}
            result_v2 = _.update_by_id(cache_id  = cache_id            ,
                                       namespace = namespace           ,
                                       data      = update_v1           )

            assert type(result_v2) is Schema__Cache__Update__Response
            assert result_v2.cache_id   == cache_id
            assert result_v2.cache_hash == cache_hash                                # V1: hash unchanged
            assert result_v2.updated_content  == True
            assert result_v2.updated_hash     == False
            assert result_v2.updated_metadata == False

            # Second update
            update_v2 = {"version": 3}
            result_v3 = _.update_by_id(cache_id  = cache_id            ,
                                       namespace = namespace           ,
                                       data      = update_v2           )

            assert type(result_v3) is Schema__Cache__Update__Response
            assert result_v3.cache_id   == cache_id
            assert result_v3.cache_hash == cache_hash                                # V1: hash unchanged
            assert result_v3.updated_content == True

            # Third update
            update_v3 = {"version": 4}
            result_v4 = _.update_by_id(cache_id  = cache_id            ,
                                       namespace = namespace           ,
                                       data      = update_v3           )

            assert type(result_v4) is Schema__Cache__Update__Response
            assert result_v4.cache_id   == cache_id
            assert result_v4.cache_hash == cache_hash                                # V1: hash unchanged

            # Verify final content
            retrieved = self.cache_service.retrieve_by_id(cache_id, namespace)
            assert retrieved['data'] == update_v3

    def test_update_by_id__different_strategies(self):                               # Test updates work with all strategies
        #skip__if_not__in_github_actions()
        strategies = [Enum__Cache__Store__Strategy.DIRECT              ,
                      Enum__Cache__Store__Strategy.TEMPORAL            ,
                      Enum__Cache__Store__Strategy.TEMPORAL_LATEST     ,
                      Enum__Cache__Store__Strategy.TEMPORAL_VERSIONED  ]

        with self.update_service as _:
            for strategy in strategies:
                namespace = Safe_Str__Id(f"upd-strat-{strategy.value}")

                # Create with strategy
                create_result = self._create_test_entry(self.test_string_v1,
                                                      strategy  = strategy  ,
                                                      namespace = namespace )
                cache_id = create_result.cache_id

                # Update
                update_result = _.update_by_id(cache_id  = cache_id            ,
                                              namespace = namespace            ,
                                              data      = self.test_string_v2  )

                assert update_result.cache_id == cache_id
                assert update_result.namespace == namespace

                # Verify strategy preserved
                refs = self.cache_service.retrieve_by_id__refs(cache_id, namespace)
                assert type(refs)   is Schema__Cache__File__Refs
                assert refs.strategy == strategy

    def test_update_by_id__same_data(self):                                          # Test updating with identical data
        with self.update_service as _:
            # Create entry
            create_result = self._create_test_entry(self.test_string_v1)
            cache_id      = create_result.cache_id
            original_hash = create_result.cache_hash

            # Update with same data
            update_result = _.update_by_id(cache_id  = cache_id            ,
                                          namespace = self.test_namespace  ,
                                          data      = self.test_string_v1  )

            assert update_result.cache_id   == cache_id
            assert update_result.cache_hash == original_hash                         # Same hash since data unchanged

    def test_update_by_id__with_cache_key(self):                                     # Test update with cache_key in original
        with self.update_service as _:
            cache_key = Safe_Str__File__Path("documents/reports/2025")

            # Create with cache_key
            create_result = self._create_test_entry(self.test_json_v1              ,
                                                   strategy  = Enum__Cache__Store__Strategy.KEY_BASED,
                                                   cache_key = cache_key            )
            cache_id = create_result.cache_id

            # Update
            update_result = _.update_by_id(cache_id  = cache_id            ,
                                          namespace = self.test_namespace  ,
                                          data      = self.test_json_v2    )

            assert update_result.cache_id == cache_id

            # Verify cache_key preserved
            config = self.cache_service.retrieve_by_id__config(cache_id, self.test_namespace)
            assert config.file_key == cache_key