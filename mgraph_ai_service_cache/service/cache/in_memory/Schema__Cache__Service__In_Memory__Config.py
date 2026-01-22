# ═══════════════════════════════════════════════════════════════════════════════
# Schema__Cache__Service__In_Memory__Config
# Configuration schema for in-memory cache service instances
# ═══════════════════════════════════════════════════════════════════════════════

from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Storage_Mode import Enum__Cache__Storage_Mode


class Schema__Cache__Service__In_Memory__Config(Type_Safe):                     # Config for in-memory cache service
    enable_api_key : bool                      = False                          # Disable API key for testing
    storage_mode   : Enum__Cache__Storage_Mode = Enum__Cache__Storage_Mode.MEMORY  # Use memory storage
