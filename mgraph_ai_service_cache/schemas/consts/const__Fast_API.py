from fastapi                                                     import Path
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service import DEFAULT_CACHE__NAMESPACE

FAST_API__PARAM__NAMESPACE                  = Path(..., example=DEFAULT_CACHE__NAMESPACE )      # note: although we get a warning, examples is not currently working
ENV_VAR__CACHE__SERVICE__BUCKET_NAME        = 'CACHE__SERVICE__BUCKET_NAME'
ENV_VAR__CACHE__SERVICE__DEFAULT_TTL_HOURS  = 'CACHE__SERVICE__DEFAULT_TTL_HOURS'

DEFAULT__CACHE__SERVICE__BUCKET_NAME        = "mgraph-ai-cache"
DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS  = 24

CACHE__TEST__FIXTURES__BUCKET_NAME          = "test-cache-fixtures"
CACHE__TEST__FIXTURES__NAMESPACE            = "fixtures-namespace"
