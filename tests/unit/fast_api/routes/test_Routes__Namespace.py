from unittest                                                                           import TestCase
from osbot_fast_api.api.routes.Fast_API__Routes                                         import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix              import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                 import Safe_Str__Fast_API__Route__Tag
from osbot_utils.testing.__                                                             import __, __SKIP__
from osbot_utils.testing.__helpers                                                      import obj
from osbot_utils.type_safe.Type_Safe                                                    import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id         import Safe_Str__Id
from osbot_utils.utils.Objects                                                          import base_classes
from mgraph_ai_service_cache.fast_api.routes.Routes__Namespace                          import (Routes__Namespace        ,
                                                                                                TAG__ROUTES_NAMESPACE       ,
                                                                                                PREFIX__ROUTES_NAMESPACE    ,
                                                                                                ROUTES_PATHS__NAMESPACE     )
from mgraph_ai_service_cache.fast_api.routes.file.Routes__File__Store                   import Routes__File__Store
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy    import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Service                               import Cache__Service
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                      import CACHE__TEST__FIXTURES__NAMESPACE
from tests.unit.Service__Cache__Test_Objs                                               import setup__service__cache__test_objs


class test_Routes__Namespace(TestCase):

    @classmethod
    def setUpClass(cls):                                                                        # ONE-TIME expensive setup
        cls.test_objs         = setup__service__cache__test_objs()
        cls.cache_fixtures    = cls.test_objs.cache_fixtures
        cls.cache_service     = cls.cache_fixtures.cache_service
        cls.routes_namespace  = Routes__Namespace(cache_service = cls.cache_service)
        cls.routes_store      = Routes__File__Store(cache_service = cls.cache_service)

        # Create test namespaces with different content
        cls.test_namespace_1  = Safe_Str__Id("test-namespace-stats-1")
        cls.test_namespace_2  = Safe_Str__Id("test-namespace-stats-2")
        cls.empty_namespace   = Safe_Str__Id("test-namespace-empty"  )

        # Populate test namespaces
        cls._setup_test_namespaces()

    @classmethod
    def _setup_test_namespaces(cls):                                                            # Create test data in namespaces
        # Namespace 1: Multiple entries with different strategies
        for i in range(3):
            cls.routes_store.store__string(data      = f"test string {i}"               ,
                                          strategy  = Enum__Cache__Store__Strategy.DIRECT,
                                          namespace = cls.test_namespace_1                )

        for i in range(2):
            cls.routes_store.store__json(data      = {"test": f"json {i}"}                 ,
                                        strategy  = Enum__Cache__Store__Strategy.TEMPORAL,
                                        namespace = cls.test_namespace_1                    )

        # Namespace 2: Different number of entries
        for i in range(5):
            cls.routes_store.store__string(data      = f"namespace 2 data {i}"                      ,
                                          strategy  = Enum__Cache__Store__Strategy.TEMPORAL_LATEST,
                                          namespace = cls.test_namespace_2                            )

        # Empty namespace intentionally left empty

    def test__init__(self):                                                                     # Test initialization and structure
        with Routes__Namespace() as _:
            assert type(_)                  is Routes__Namespace
            assert base_classes(_)          == [Fast_API__Routes, Type_Safe, object]
            assert type(_.tag)              is Safe_Str__Fast_API__Route__Tag
            assert type(_.prefix)           is Safe_Str__Fast_API__Route__Prefix
            assert _.tag                    == TAG__ROUTES_NAMESPACE
            assert _.prefix                 == PREFIX__ROUTES_NAMESPACE
            assert type(_.cache_service)    is Cache__Service

    def test__class_constants(self):                                                            # Test module-level constants
        assert TAG__ROUTES_NAMESPACE       == 'namespace'
        assert PREFIX__ROUTES_NAMESPACE    == '/{namespace}'
        assert len(ROUTES_PATHS__NAMESPACE) == 3

        # Verify each route path
        assert ROUTES_PATHS__NAMESPACE[0]  == '/{namespace}/file-hashes'
        assert ROUTES_PATHS__NAMESPACE[1]  == '/{namespace}/file-ids'
        assert ROUTES_PATHS__NAMESPACE[2]  == '/{namespace}/stats'

    def test_file_hashes(self):                                                                 # Test retrieving file hashes from namespace
        with self.routes_namespace as _:
            # Test with populated namespace
            hashes = _.file_hashes(namespace = self.test_namespace_1)

            assert type(hashes)    is list
            assert len(hashes)     > 0                                                          # Should have hashes from setup
            assert hashes         == ['0a21e4b2ab9875fe',
                                      '0b0b6f97b6f431a2',
                                      '0b567d1d7a557520',
                                      '132203e31afa2772',
                                      '6814849ac17d0df6']
            assert all(isinstance(h, str) for h in hashes)                                      # All hashes should be strings

            for hash_value in hashes:                                                           # Verify hashes are valid (non-empty strings)
                assert len(hash_value) > 0
                assert ' ' not in hash_value                                                    # No spaces in hashes

    def test_file_hashes__empty_namespace(self):                                                # Test file_hashes with empty namespace
        with self.routes_namespace as _:
            hashes = _.file_hashes(namespace = self.empty_namespace)

            assert type(hashes)    is list
            assert len(hashes)     == 0                                                         # Empty namespace has no hashes

    def test_file_hashes__default_namespace(self):                                              # Test file_hashes with default namespace
        with self.routes_namespace as _:
            namespace = CACHE__TEST__FIXTURES__NAMESPACE                                        # we can't use DEFAULT_CACHE__NAMESPACE because there are no files in there (when running this directly)
            hashes    = _.file_hashes(namespace = namespace)                                    # cache test namespace should have fixtures from cache_fixtures (but CACHE__TEST__FIXTURES__NAMESPACE  is like the DEFAULT_CACHE__NAMESPACE)

            assert type(hashes)    is list
            assert len(hashes)     > 0                                                          # Fixtures should be present

    def test_file_ids(self):                                                                    # Test retrieving file IDs from namespace
        with self.routes_namespace as _:
            # Test with populated namespace
            file_ids = _.file_ids(namespace = self.test_namespace_1)

            assert type(file_ids)  is list
            assert len(file_ids)   > 0                                                          # Should have IDs from setup
            assert all(isinstance(id, str) for id in file_ids)                                  # All IDs should be strings

            # Verify IDs are valid GUIDs
            for file_id in file_ids:
                assert len(file_id) == 36                                                       # GUID format length
                assert file_id.count('-') == 4                                                  # GUID has 4 dashes

    def test_file_ids__empty_namespace(self):                                                   # Test file_ids with empty namespace
        with self.routes_namespace as _:
            file_ids = _.file_ids(namespace = self.empty_namespace)

            assert type(file_ids)  is list
            assert len(file_ids)   == 0                                                         # Empty namespace has no IDs

    def test_file_ids__default_namespace(self):                                                 # Test file_ids with default namespace
        with self.routes_namespace as _:
            file_ids = _.file_ids(namespace = CACHE__TEST__FIXTURES__NAMESPACE)                 # CACHE__TEST__FIXTURES__NAMESPACE namespace should have fixtures

            assert type(file_ids)  is list
            assert len(file_ids)   > 0                                                          # Fixtures should be present

    def test_bug__stats(self):                                                                       # Test retrieving namespace statistics
        with self.routes_namespace as _:
            stats = _.stats(namespace = self.test_namespace_1)

            # Verify stats structure
            assert type(stats)              is dict
            from osbot_utils.type_safe.primitives.core.Safe_UInt import Safe_UInt
            assert obj(stats)               == __(namespace                 = 'test-namespace-stats-1',
                                                  ttl_hours                 = Safe_UInt(24)           ,
                                                  direct_files              = __SKIP__  ,                   # BUG (these totals are for all namespaces, not just test_namespace_1)
                                                  temporal_files            = __SKIP__  ,                   # BUG
                                                  temporal_latest_files     = __SKIP__  ,                   # BUG
                                                  temporal_versioned_files  = __SKIP__  ,                   # BUG
                                                  refs_hash_files           = __SKIP__  ,                   # BUG
                                                  refs_id_files             = __SKIP__  ,                   # BUG
                                                  total_files               = __SKIP__  )                   # BUG


            assert 'namespace'                in stats
            assert 'ttl_hours'                in stats
            assert 'direct_files'             in stats
            assert 'temporal_files'           in stats
            assert 'temporal_latest_files'    in stats
            assert 'temporal_versioned_files' in stats
            assert 'refs_hash_files'          in stats
            assert 'refs_id_files'            in stats
            assert 'total_files'              in stats

            # Verify basic values
            assert stats['namespace']       == str(self.test_namespace_1)
            assert type(stats['ttl_hours']) is Safe_UInt
            assert stats['ttl_hours']       > 0

            # Verify counts are integers and non-negative
            assert type(stats['direct_files'])             is int
            assert type(stats['temporal_files'])           is int
            assert type(stats['temporal_latest_files'])    is int
            assert type(stats['temporal_versioned_files']) is int
            assert type(stats['refs_hash_files'])          is int
            assert type(stats['refs_id_files'])            is int
            assert type(stats['total_files'])              is int

            assert stats['direct_files']                  >= 0
            assert stats['temporal_files']                >= 0
            assert stats['temporal_latest_files']         >= 0
            assert stats['temporal_versioned_files']      >= 0
            assert stats['refs_hash_files']               >= 0
            assert stats['refs_id_files']                 >= 0
            assert stats['total_files']                   > 0                                   # Should have files from setup

    def test__bug__stats__empty_namespace(self):                                                      # Test stats for empty namespace
        with self.routes_namespace as _:
            stats = _.stats(namespace = self.empty_namespace)

            assert type(stats)              is dict
            assert stats['namespace']       == str(self.empty_namespace)
            # assert stats['total_files']     == 0                              #  BUG
            # assert stats['direct_files']    == 0                              #  BUG
            # assert stats['temporal_files']  == 0                              #  BUG

            assert stats['total_files']     != 0                                #  BUG
            assert stats['direct_files']    != 0                                #  BUG
            assert stats['temporal_files']  != 0                                #  BUG

    def test_stats__default_namespace(self):                                                    # Test stats for default namespace
        with self.routes_namespace as _:
            stats = _.stats(namespace = None)                                                   # None should use default

            assert type(stats)              is dict
            assert stats['namespace']       == 'default'
            assert stats['total_files']     > 0                                                 # Fixtures should be present

    def test_stats__total_calculation(self):                                                    # Test that total_files equals sum of components
        with self.routes_namespace as _:
            stats = _.stats(namespace = self.test_namespace_1)

            calculated_total = (stats['direct_files']            +
                                stats['temporal_files']           +
                                stats['temporal_latest_files']    +
                                stats['temporal_versioned_files'] +
                                stats['refs_hash_files']          +
                                stats['refs_id_files'])

            assert stats['total_files']     == calculated_total                                 # Total should match sum

    def test__bug__stats__different_namespaces(self):                                                 # Test stats for different namespaces have different counts
        with self.routes_namespace as _:
            stats_1 = _.stats(namespace = self.test_namespace_1)
            stats_2 = _.stats(namespace = self.test_namespace_2)

            # Different namespaces should have different file counts
            assert stats_1['namespace']     != stats_2['namespace']
            #assert stats_1['total_files']   != stats_2['total_files']                           # BUG    # We created different amounts

            # Verify namespace 2 has more files (5 vs 5 entries)
            #assert stats_2['total_files']   > stats_1['total_files']                            # BUG

            assert stats_1['total_files']   == stats_2['total_files']                            # BUG
            assert stats_2['total_files']   == stats_1['total_files']                            # BUG

    def test_stats__error_handling(self):                                                       # Test error handling in stats endpoint
        with self.routes_namespace as _:
            # Test with valid namespace - should not error
            stats = _.stats(namespace = Safe_Str__Id("valid-namespace"))

            assert type(stats)              is dict
            assert 'error'                  not in stats

    def test_file_hashes__consistency(self):                                                    # Test that file_hashes returns consistent results
        with self.routes_namespace as _:
            hashes_1 = _.file_hashes(namespace = self.test_namespace_1)
            hashes_2 = _.file_hashes(namespace = self.test_namespace_1)

            # Should return same results when called twice
            assert hashes_1                 == hashes_2
            assert len(hashes_1)            == len(hashes_2)

    def test_file_ids__consistency(self):                                                       # Test that file_ids returns consistent results
        with self.routes_namespace as _:
            ids_1 = _.file_ids(namespace = self.test_namespace_1)
            ids_2 = _.file_ids(namespace = self.test_namespace_1)

            # Should return same results when called twice
            assert ids_1                    == ids_2
            assert len(ids_1)               == len(ids_2)

    def test_file_hashes__sorted_output(self):                                                  # Test that file_hashes returns sorted list
        with self.routes_namespace as _:
            hashes = _.file_hashes(namespace = self.test_namespace_1)

            if len(hashes) > 1:
                # Verify list is sorted
                sorted_hashes = sorted(hashes)
                assert hashes               == sorted_hashes

    def test_file_ids__sorted_output(self):                                                     # Test that file_ids returns sorted list
        with self.routes_namespace as _:
            file_ids = _.file_ids(namespace = self.test_namespace_1)

            if len(file_ids) > 1:
                # Verify list is sorted
                sorted_ids = sorted(file_ids)
                assert file_ids             == sorted_ids

    def test_stats__namespace_isolation(self):                                                  # Test that namespaces are properly isolated
        with self.routes_namespace as _:
            # Get file IDs from different namespaces
            ids_1 = _.file_ids(namespace = self.test_namespace_1)
            ids_2 = _.file_ids(namespace = self.test_namespace_2)

            # IDs should be different (no overlap)
            ids_set_1 = set(ids_1)
            ids_set_2 = set(ids_2)

            overlap = ids_set_1.intersection(ids_set_2)
            assert len(overlap)             == 0                                                # No IDs should be shared

    def test_file_hashes__namespace_isolation(self):                                            # Test that hashes are namespace-isolated
        with self.routes_namespace as _:
            # Get hashes from different namespaces
            hashes_1 = _.file_hashes(namespace = self.test_namespace_1)
            hashes_2 = _.file_hashes(namespace = self.test_namespace_2)

            # Different namespaces can share hashes (same content)
            # but should have different total structures
            assert type(hashes_1)           is list
            assert type(hashes_2)           is list
            assert hashes_1 != hashes_2

    def test_stats__refs_files_present(self):                                                   # Test that reference files are counted
        with self.routes_namespace as _:
            stats = _.stats(namespace = self.test_namespace_1)

            # If we have data files, we should have reference files
            if stats['total_files'] > 0:
                assert stats['refs_id_files']   > 0                                             # Should have by-id refs
                assert stats['refs_hash_files'] > 0                                             # Should have by-hash refs

    def test_setup_routes(self):                                                                # Test that routes are properly configured
        with self.routes_namespace as _:
            # Verify route methods exist
            assert hasattr(_, 'file_hashes')
            assert hasattr(_, 'file_ids')
            assert hasattr(_, 'stats')
            assert hasattr(_, 'setup_routes')

            # Verify methods are callable
            assert callable(_.file_hashes)
            assert callable(_.file_ids)
            assert callable(_.stats)
            assert callable(_.setup_routes)