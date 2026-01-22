# ═══════════════════════════════════════════════════════════════════════════════
# Cache__Service__In_Memory
# In-memory cache service wrapper for testing and embedded usage
# Provides FastAPI app injectable into any client via TestClient
# ═══════════════════════════════════════════════════════════════════════════════

from typing                                                                                         import Any
from mgraph_ai_service_cache_client.client.client_contract.Cache__Service__Fast_API__Client         import Cache__Service__Fast_API__Client
from mgraph_ai_service_cache_client.client.client_contract.Cache__Service__Fast_API__Client__Config import Cache__Service__Fast_API__Client__Config
from osbot_utils.type_safe.Type_Safe                                                                import Type_Safe
from mgraph_ai_service_cache.fast_api.Cache_Service__Fast_API                                       import Cache_Service__Fast_API
from mgraph_ai_service_cache.service.cache.Cache__Config                                            import Cache__Config
from mgraph_ai_service_cache.service.cache.Cache__Service                                           import Cache__Service
from mgraph_ai_service_cache.service.cache.in_memory.Schema__Cache__Service__In_Memory__Config      import Schema__Cache__Service__In_Memory__Config
from osbot_fast_api_serverless.fast_api.Serverless__Fast_API__Config                                import Serverless__Fast_API__Config


class Cache__Service__In_Memory(Type_Safe):                                                                 # In-memory cache service wrapper
    config        : Schema__Cache__Service__In_Memory__Config                                               # Configuration for in-memory mode
    cache_service : Cache__Service                              = None                                                 # Backend cache service (set by setup)
    fast_api_app  : Any                                         = None                                                 # FastAPI app (set by setup)
    cache_client  : Cache__Service__Fast_API__Client            = None                                                 # Client for cache operations (set by setup)

    def setup(self) -> 'Cache__Service__In_Memory':                                                         # Initialize in-memory service
        self.setup_cache_service()
        self.setup_fast_api_app()
        self.setup_cache_client()
        return self

    def setup_cache_service(self):                                                                          # Create backend cache service
        cache_config       = Cache__Config(storage_mode  = self.config.storage_mode)
        self.cache_service = Cache__Service(cache_config = cache_config            )

    def setup_fast_api_app(self):                                                                           # Create FastAPI app
        serverless_config = Serverless__Fast_API__Config(enable_api_key = self.config.enable_api_key)
        cache_fast_api    = Cache_Service__Fast_API     (config         = serverless_config         ,
                                                         cache_service  = self.cache_service        )
        cache_fast_api.setup()
        self.fast_api_app = cache_fast_api.app()

    def setup_cache_client(self):                                                                           # Create client pointing to in-memory app
        config            = Cache__Service__Fast_API__Client__Config(fast_api_app = self.fast_api_app)
        self.cache_client = Cache__Service__Fast_API__Client        (config       = config           )


def cache_client__in_memory():                                                                              # Convenience function
    return Cache__Service__In_Memory().setup().cache_client