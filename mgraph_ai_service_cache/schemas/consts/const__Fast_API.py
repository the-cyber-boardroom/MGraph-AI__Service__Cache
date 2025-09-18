from fastapi                                                     import Path
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service import DEFAULT_CACHE__NAMESPACE

FAST_API__PARAM__NAMESPACE = Path(..., example=DEFAULT_CACHE__NAMESPACE )