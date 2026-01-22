# ═══════════════════════════════════════════════════════════════════════════════
# Tests for Cache__Service__In_Memory
# Validates in-memory cache service wrapper functionality
# ═══════════════════════════════════════════════════════════════════════════════

from unittest                                                                                   import TestCase
from fastapi                                                                                    import FastAPI
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service
from mgraph_ai_service_cache.service.cache.in_memory.Cache__Service__In_Memory                  import Cache__Service__In_Memory
from mgraph_ai_service_cache.service.cache.in_memory.Schema__Cache__Service__In_Memory__Config  import Schema__Cache__Service__In_Memory__Config
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Storage_Mode               import Enum__Cache__Storage_Mode
from osbot_utils.utils.Objects                                                                  import base_classes
from osbot_utils.type_safe.Type_Safe                                                            import Type_Safe


class test_Cache__Service__In_Memory(TestCase):

    def test__init__(self):                                                     # Test auto-initialization
        with Cache__Service__In_Memory() as _:
            assert type(_)              is Cache__Service__In_Memory
            assert type(_.config)       is Schema__Cache__Service__In_Memory__Config
            assert base_classes(_)      == [Type_Safe, object]
            assert _.cache_service      is None                                 # Before setup
            assert _.fast_api_app       is None                                 # Before setup

    def test__config__defaults(self):                                           # Test config default values
        with Schema__Cache__Service__In_Memory__Config() as _:
            assert _.enable_api_key is False
            assert _.storage_mode   == Enum__Cache__Storage_Mode.MEMORY

    def test__setup(self):                                                      # Test setup creates service and app
        with Cache__Service__In_Memory().setup() as _:
            assert _.cache_service      is not None
            assert _.fast_api_app       is not None
            assert type(_.cache_service) is Cache__Service
            assert type(_.fast_api_app)  is FastAPI

    def test__setup__returns_self(self):                                        # Test fluent interface
        in_memory = Cache__Service__In_Memory()
        result    = in_memory.setup()
        assert result is in_memory


