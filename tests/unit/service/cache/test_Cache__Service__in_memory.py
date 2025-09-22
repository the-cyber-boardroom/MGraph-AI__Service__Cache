# import pytest
# from unittest                                                                             import TestCase
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                     import Random_Guid
# from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id           import Safe_Str__Id
# from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Storage_Mode                import Enum__Cache__Storage_Mode
# from mgraph_ai_service_cache.service.cache.Cache__Service                                 import Cache__Service
#
# class test_Cache__Service__in_memory(TestCase):                                             # Unit tests that run entirely in memory - no external dependencies
#
#     @classmethod
#     def setUpClass(cls):                                                                    # ONE-TIME setup
#         cls.cache_service = Cache__Service(storage_mode=Enum__Cache__Storage_Mode.MEMORY)   # Explicitly set memory mode for unit tests
#         cls.test_namespace = Safe_Str__Id("test-memory")
#
#     def setUp(self):                                                                        # PER-TEST setup
#         self.test_id = Random_Guid()
#
#     def test__storage_mode(self):                                                    # Verify we're in memory mode
#         with self.cache_service as _:
#             assert _.storage_mode == Enum__Cache__Storage_Mode.MEMORY
#
#             # Get storage info
#             info = _.get_storage_info()
#             assert info['storage_mode'] == 'memory'
#             assert 's3_bucket' not in info                                          # No S3 config in memory mode
#
#     def test__store_and_retrieve(self):                                             # Fast memory operations
#         with self.cache_service as _:
#             # Store data
#             test_data = {"test": "data", "id": str(self.test_id)}
#             cache_hash = _.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             result = _.store_with_strategy(storage_data = test_data,
#                                           cache_hash   = cache_hash,
#                                           cache_id     = cache_id,
#                                           strategy     = "direct",
#                                           namespace    = self.test_namespace)
#
#             assert result.cache_id == cache_id
#             assert result.hash == cache_hash
#
#             # Retrieve by ID - should be instant in memory
#             retrieved = _.retrieve_by_id(cache_id, self.test_namespace)
#             assert retrieved['data'] == test_data
#
#             # Retrieve by hash - also instant
#             retrieved = _.retrieve_by_hash(cache_hash, self.test_namespace)
#             assert retrieved['data'] == test_data
#
#     def test__multiple_namespaces(self):                                            # Test isolation between namespaces
#         with self.cache_service as _:
#             ns1 = Safe_Str__Id("namespace-1")
#             ns2 = Safe_Str__Id("namespace-2")
#
#             data1 = {"namespace": "one"}
#             data2 = {"namespace": "two"}
#
#             # Store in different namespaces
#             hash1 = _.hash_from_json(data1)
#             hash2 = _.hash_from_json(data2)
#
#             _.store_with_strategy(storage_data=data1, cache_hash=hash1, namespace=ns1, strategy="direct")
#             _.store_with_strategy(storage_data=data2, cache_hash=hash2, namespace=ns2, strategy="direct")
#
#             # Each namespace has its own data
#             result1 = _.retrieve_by_hash(hash1, ns1)
#             result2 = _.retrieve_by_hash(hash2, ns2)
#
#             assert result1['data'] == data1
#             assert result2['data'] == data2
#
#             # Cross-namespace retrieval fails
#             assert _.retrieve_by_hash(hash1, ns2) is None
#             assert _.retrieve_by_hash(hash2, ns1) is None
#
#     def test__performance(self):                                                    # Memory mode should be very fast
#         import time
#
#         with self.cache_service as _:
#             # Store 100 items
#             start = time.time()
#
#             for i in range(100):
#                 data = {"item": i, "data": f"test-{i}"}
#                 hash_val = _.hash_from_json(data)
#                 _.store_with_strategy(storage_data=data,
#                                      cache_hash=hash_val,
#                                      strategy="direct",
#                                      namespace=self.test_namespace)
#
#             elapsed = time.time() - start
#
#             # Should be very fast in memory (< 100ms for 100 items)
#             assert elapsed < 0.1, f"Too slow: {elapsed}s for 100 items"
#
#             # Verify we can retrieve them all quickly
#             start = time.time()
#             for i in range(100):
#                 data = {"item": i, "data": f"test-{i}"}
#                 hash_val = _.hash_from_json(data)
#                 result = _.retrieve_by_hash(hash_val, self.test_namespace)
#                 assert result['data'] == data
#
#             elapsed = time.time() - start
#             assert elapsed < 0.05, f"Retrieval too slow: {elapsed}s for 100 items"