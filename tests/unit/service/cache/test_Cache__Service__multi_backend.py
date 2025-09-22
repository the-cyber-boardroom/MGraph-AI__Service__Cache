# import time
# import tempfile
# from unittest                                                                import TestCase
# from osbot_utils.testing.__                                                  import __, __SKIP__
# from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid        import Random_Guid
# from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id import Safe_Str__Id
# from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Storage_Mode  import Enum__Cache__Storage_Mode
# from mgraph_ai_service_cache.service.cache.Cache__Config                    import Cache__Config
# from mgraph_ai_service_cache.service.cache.Cache__Service                   import Cache__Service
#
# class test_Cache__Service__Multi_Backend(TestCase):
#     """Integration tests showing different storage backends"""
#
#     def test_memory_backend(self):                                         # Test pure memory storage
#         config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY)
#         service = Cache__Service(cache_config=config)
#
#         with service as _:
#             # Verify configuration
#             assert _.cache_config.storage_mode == Enum__Cache__Storage_Mode.MEMORY
#             info = _.get_storage_info()
#             assert info['storage_mode'] == 'memory'
#
#             # Test operations
#             namespace = Safe_Str__Id("memory-test")
#             test_data = {"backend": "memory", "fast": True}
#             cache_hash = _.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             # Store should be very fast
#             start = time.time()
#             _.store_with_strategy(storage_data = test_data,
#                                  cache_hash   = cache_hash,
#                                  cache_id     = cache_id,
#                                  strategy     = "direct",
#                                  namespace    = namespace)
#             store_time = time.time() - start
#
#             # Retrieve should be instant
#             start = time.time()
#             result = _.retrieve_by_id(cache_id, namespace)
#             retrieve_time = time.time() - start
#
#             assert result['data'] == test_data
#             assert store_time < 0.01                                       # Should be < 10ms
#             assert retrieve_time < 0.01                                    # Should be < 10ms
#
#             # Clean up
#             _.delete_by_id(cache_id, namespace)
#
#     def test_local_disk_backend(self):                                     # Test local disk storage
#         temp_dir = tempfile.mkdtemp()
#
#         config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.LOCAL_DISK,
#                               local_disk_path=temp_dir)
#         service = Cache__Service(cache_config=config)
#
#         with service as _:
#             # Verify configuration
#             assert _.cache_config.storage_mode == Enum__Cache__Storage_Mode.LOCAL_DISK
#             info = _.get_storage_info()
#             assert info['storage_mode']    == 'local_disk'
#             assert info['local_disk_path'] == temp_dir
#
#             # Test persistence
#             namespace = Safe_Str__Id("disk-test")
#             test_data = {"backend": "local_disk", "persistent": True}
#             cache_hash = _.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             # Store data
#             _.store_with_strategy(storage_data = test_data,
#                                  cache_hash   = cache_hash,
#                                  cache_id     = cache_id,
#                                  strategy     = "direct",
#                                  namespace    = namespace)
#
#             # Verify files are on disk
#             import os
#             assert os.path.exists(temp_dir)
#
#             # Files should be in nested directories
#             # Note: actual path structure depends on Memory_FS implementation
#
#             # Retrieve data
#             result = _.retrieve_by_id(cache_id, namespace)
#             assert result['data'] == test_data
#
#             # Clean up
#             _.delete_by_id(cache_id, namespace)
#
#             # Clean up temp directory
#             import shutil
#             shutil.rmtree(temp_dir)
#
#     def test_sqlite_backend__in_memory(self):                              # Test SQLite in-memory storage
#         config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.SQLITE,
#                               sqlite_path=':memory:')
#         service = Cache__Service(cache_config=config)
#
#         with service as _:
#             # Verify configuration
#             assert _.cache_config.storage_mode == Enum__Cache__Storage_Mode.SQLITE
#             info = _.get_storage_info()
#             assert info['storage_mode'] == 'sqlite'
#             assert info['sqlite_path']  == ':memory:'
#
#             # Test structured storage
#             namespace = Safe_Str__Id("sqlite-test")
#             test_data = {"backend": "sqlite", "indexed": True}
#             cache_hash = _.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             # Store data
#             _.store_with_strategy(storage_data = test_data,
#                                  cache_hash   = cache_hash,
#                                  cache_id     = cache_id,
#                                  strategy     = "temporal",
#                                  namespace    = namespace)
#
#             # SQLite provides ACID compliance
#             result = _.retrieve_by_id(cache_id, namespace)
#             assert result['data'] == test_data
#
#             # Clean up
#             _.delete_by_id(cache_id, namespace)
#
#     def test_sqlite_backend__file(self):                                   # Test SQLite file-based storage
#         temp_file = tempfile.mktemp(suffix='.db')
#
#         config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.SQLITE,
#                               sqlite_path=temp_file)
#         service = Cache__Service(cache_config=config)
#
#         with service as _:
#             # Store data
#             namespace = Safe_Str__Id("sqlite-file-test")
#             test_data = {"backend": "sqlite_file", "persistent": True}
#             cache_hash = _.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             _.store_with_strategy(storage_data = test_data,
#                                  cache_hash   = cache_hash,
#                                  cache_id     = cache_id,
#                                  strategy     = "direct",
#                                  namespace    = namespace)
#
#             # Verify file was created
#             import os
#             assert os.path.exists(temp_file)
#             assert os.path.getsize(temp_file) > 0
#
#             # Data should be retrievable
#             result = _.retrieve_by_id(cache_id, namespace)
#             assert result['data'] == test_data
#
#             # Clean up
#             _.delete_by_id(cache_id, namespace)
#             os.unlink(temp_file)
#
#     def test_zip_backend(self):                                            # Test ZIP archive storage
#         temp_file = tempfile.mktemp(suffix='.zip')
#
#         config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.ZIP,
#                               zip_path=temp_file)
#         service = Cache__Service(cache_config=config)
#
#         with service as _:
#             # Verify configuration
#             assert _.cache_config.storage_mode == Enum__Cache__Storage_Mode.ZIP
#             info = _.get_storage_info()
#             assert info['storage_mode'] == 'zip'
#             assert info['zip_path']     == temp_file
#
#             # Test compressed storage
#             namespace = Safe_Str__Id("zip-test")
#             test_data = {"backend": "zip", "compressed": True,
#                         "data": "x" * 1000}  # Compressible data
#             cache_hash = _.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             # Store data
#             _.store_with_strategy(storage_data = test_data,
#                                  cache_hash   = cache_hash,
#                                  cache_id     = cache_id,
#                                  strategy     = "direct",
#                                  namespace    = namespace)
#
#             # Verify ZIP file was created
#             import os
#             assert os.path.exists(temp_file)
#             file_size = os.path.getsize(temp_file)
#             assert file_size > 0
#
#             # ZIP should compress the data
#             # (actual compression depends on ZIP implementation)
#
#             # Data should be retrievable
#             result = _.retrieve_by_id(cache_id, namespace)
#             assert result['data'] == test_data
#
#             # Clean up
#             _.delete_by_id(cache_id, namespace)
#             os.unlink(temp_file)
#
#     def test_switching_backends_comparison(self):                          # Compare performance across backends
#         namespace = Safe_Str__Id("perf-test")
#         test_data = {"test": "data" * 100}                                # Medium-sized data
#
#         results = {}
#
#         # Test each backend
#         backends = [
#             (Enum__Cache__Storage_Mode.MEMORY, {}),
#             (Enum__Cache__Storage_Mode.SQLITE, {'sqlite_path': ':memory:'}),
#             (Enum__Cache__Storage_Mode.LOCAL_DISK, {'local_disk_path': tempfile.mkdtemp()}),
#         ]
#
#         for storage_mode, kwargs in backends:
#             config = Cache__Config(storage_mode=storage_mode, **kwargs)
#             service = Cache__Service(cache_config=config)
#
#             cache_hash = service.hash_from_json(test_data)
#             cache_id = Random_Guid()
#
#             # Measure store time
#             start = time.time()
#             service.store_with_strategy(storage_data = test_data,
#                                        cache_hash   = cache_hash,
#                                        cache_id     = cache_id,
#                                        strategy     = "direct",
#                                        namespace    = namespace)
#             store_time = time.time() - start
#
#             # Measure retrieve time
#             start = time.time()
#             result = service.retrieve_by_id(cache_id, namespace)
#             retrieve_time = time.time() - start
#
#             assert result['data'] == test_data
#
#             results[storage_mode.value] = {
#                 'store_ms': store_time * 1000,
#                 'retrieve_ms': retrieve_time * 1000
#             }
#
#             # Clean up
#             service.delete_by_id(cache_id, namespace)
#
#         # Memory should be fastest
#         assert results['memory']['store_ms'] < results['sqlite']['store_ms']
#         assert results['memory']['retrieve_ms'] < results['local_disk']['retrieve_ms']
#
#         # All should be reasonably fast (< 100ms)
#         for backend, times in results.items():
#             assert times['store_ms'] < 100, f"{backend} too slow: {times['store_ms']}ms"
#             assert times['retrieve_ms'] < 100, f"{backend} too slow: {times['retrieve_ms']}ms"
#
#     def test_namespace_isolation(self):                                    # Test namespace isolation across backends
#         config = Cache__Config(storage_mode=Enum__Cache__Storage_Mode.MEMORY)
#         service = Cache__Service(cache_config=config)
#
#         with service as _:
#             ns1 = Safe_Str__Id("namespace-1")
#             ns2 = Safe_Str__Id("namespace-2")
#
#             # Store in namespace 1
#             data1 = {"namespace": 1}
#             hash1 = _.hash_from_json(data1)
#             id1 = Random_Guid()
#             _.store_with_strategy(storage_data=data1, cache_hash=hash1,
#                                  cache_id=id1, strategy="direct", namespace=ns1)
#
#             # Store in namespace 2
#             data2 = {"namespace": 2}
#             hash2 = _.hash_from_json(data2)
#             id2 = Random_Guid()
#             _.store_with_strategy(storage_data=data2, cache_hash=hash2,
#                                  cache_id=id2, strategy="direct", namespace=ns2)
#
#             # Each namespace has its own data
#             result1 = _.retrieve_by_id(id1, ns1)
#             result2 = _.retrieve_by_id(id2, ns2)
#
#             assert result1['data'] == data1
#             assert result2['data'] == data2
#
#             # Cross-namespace retrieval fails
#             assert _.retrieve_by_id(id1, ns2) is None
#             assert _.retrieve_by_id(id2, ns1) is None
#
#             # Clean up
#             _.delete_by_id(id1, ns1)
#             _.delete_by_id(id2, ns2)