from typing                                                                          import Any
from fastapi                                                                         import HTTPException, Body
from osbot_fast_api.api.decorators.route_path                                        import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                      import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix           import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag              import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                    import cache_on_self
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id      import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                       import type_safe
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Update__Response    import Schema__Cache__Update__Response
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                   import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                            import Cache__Service
from mgraph_ai_service_cache.service.cache.update.Cache__Service__Update             import Cache__Service__Update

TAG__ROUTES_UPDATE                  = 'update'
PREFIX__ROUTES_UPDATE               = '/{namespace}/update'
BASE_PATH__ROUTES_UPDATE            = f'{PREFIX__ROUTES_UPDATE}/{{cache_id}}'
ROUTES_PATHS__UPDATE                = [ BASE_PATH__ROUTES_UPDATE + '/string'   ,
                                        BASE_PATH__ROUTES_UPDATE + '/json'     ,
                                        BASE_PATH__ROUTES_UPDATE + '/binary'   ]


class Routes__File__Update(Fast_API__Routes):                                   # FastAPI routes for updating existing cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_UPDATE
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_UPDATE
    cache_service : Cache__Service                                              # Dependency injection for cache service

    @cache_on_self
    def update_service(self) -> Cache__Service__Update:                         # Service layer for update operations
        return Cache__Service__Update(cache_service=self.cache_service)

    @route_path("/update/{cache_id}/string")
    def update__string(self,
                       data      : str           = Body(...)                     ,
                       cache_id  : Random_Guid   = None                          ,
                       namespace : Safe_Str__Id  = FAST_API__PARAM__NAMESPACE
                  ) -> Schema__Cache__Update__Response:                          # Update existing cache entry with string data

        if not data:                                                            # Validate input
            error_detail = { "error_type"    : "INVALID_INPUT"                       ,
                             "message"       : "String data cannot be empty"         ,
                             "field_name"    : "data"                                }
            raise HTTPException(status_code=400, detail=error_detail)

        return self._execute_update(cache_id  = cache_id  ,
                                    namespace = namespace ,
                                    data      = data      )

    @route_path("/update/{cache_id}/json")
    def update__json(self,
                     data      : dict          = Body(...)                     ,
                     cache_id  : Random_Guid   = None                          ,
                     namespace : Safe_Str__Id  = FAST_API__PARAM__NAMESPACE
                ) -> Schema__Cache__Update__Response:                            # Update existing cache entry with JSON data

        return self._execute_update(cache_id  = cache_id  ,
                                    namespace = namespace ,
                                    data      = data      )

    @route_path("/update/{cache_id}/binary")
    def update__binary(self,
                       body      : bytes         = Body(..., media_type="application/octet-stream"),
                       cache_id  : Random_Guid   = None                          ,
                       namespace : Safe_Str__Id  = FAST_API__PARAM__NAMESPACE
                  ) -> Schema__Cache__Update__Response:                          # Update existing cache entry with binary data

        if not body:                                                            # Validate input
            error_detail = { "error_type"    : "INVALID_INPUT"                       ,
                             "message"       : "Binary data cannot be empty"         ,
                             "field_name"    : "body"                                }
            raise HTTPException(status_code=400, detail=error_detail)

        return self._execute_update(cache_id  = cache_id  ,
                                    namespace = namespace ,
                                    data      = body      )

    @type_safe
    def _execute_update(self,
                        cache_id  : Random_Guid   ,
                        namespace : Safe_Str__Id  ,
                        data      : Any           ,
                   ) -> Schema__Cache__Update__Response:                         # Common update logic for all data types

        result = self.update_service().update_by_id(cache_id  = cache_id  ,
                                                    namespace = namespace ,
                                                    data      = data      )

        if result is None:                                                      # Handle update failure
            error_detail = { "error_type" : "UPDATE_FAILED"                          ,
                             "message"    : f"Failed to update cache entry {cache_id}",
                             "cache_id"   : str(cache_id)                            }
            raise HTTPException(status_code=500, detail=error_detail)

        return result

    def setup_routes(self):                                                     # Configure all update routes
        self.add_route_post(self.update__string)
        self.add_route_post(self.update__json  )
        self.add_route_post(self.update__binary)