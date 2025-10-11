from fastapi                                                                          import HTTPException, Request, Body
from mgraph_ai_service_cache.service.cache.Cache__Service                             import Cache__Service
from osbot_fast_api.api.decorators.route_path                                         import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                       import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix            import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag               import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                     import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path     import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id       import Safe_Str__Id
from mgraph_ai_service_cache_client.schemas.cache.consts__Cache_Service               import DEFAULT_CACHE__STORE__STRATEGY
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy  import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Json__Field_Path import Safe_Str__Json__Field_Path
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                    import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.store.Cache__Service__Store                import Cache__Service__Store
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Store__Response      import Schema__Cache__Store__Response

TAG__ROUTES_STORE                  = 'store'
PREFIX__ROUTES_STORE               = '/{namespace}/{strategy}'
ROUTES_PATHS__STORE                = [ f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'string'                  ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'string/{cache_key:path}' ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'json'                    ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'json/{cache_key:path}'   ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'binary'                  ,
                                       f'{PREFIX__ROUTES_STORE}/{TAG__ROUTES_STORE}/' + 'binary/{cache_key:path}' ]


class Routes__File__Store(Fast_API__Routes):                                                                  # FastAPI routes for cache store operations
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_STORE
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_STORE
    cache_service : Cache__Service                                                                     # get cache_service via Dependency Injection

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @cache_on_self
    def store_service(self):                                                                            # Service layer for business logic
        return Cache__Service__Store(cache_service=self.cache_service)                                  # create Cache__Service__Store object (once, using the shared Cache_Service)


    def store__string(self, data      : str = Body(...),
                            strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                            namespace : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE
                       ) -> Schema__Cache__Store__Response:                                                         # Store string data

        if not data:                                                                                                # Validate input
            error = self.store_service().get_invalid_input_error( field_name    = "data"                       ,      # todo: see if we should support this case where the files are empty
                                                                  expected_type = "non-empty string"           ,
                                                                  message       = "String data cannot be empty")
            raise HTTPException(status_code=400, detail=error.json())

        # Use service layer
        result = self.store_service().store_string(data      = data       ,
                                                 strategy  = strategy   ,
                                                 namespace = namespace  )

        if result is None:
            raise HTTPException(status_code=500, detail="Failed to store data")

        return result

    @route_path("/store/string/{cache_key:path}")
    def store__string__cache_key(self, data       : str                          = Body(...)                        ,
                                       namespace  : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE         ,
                                       strategy   : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY   ,
                                       cache_key  : Safe_Str__File__Path         = None,
                                       file_id    : Safe_Str__Id                 = None
                                  ) -> Schema__Cache__Store__Response:                                  # Store string with semantic key

        if not data:                                                                                    # Validate input
            error = self.store_service().get_invalid_input_error(field_name    = "data"                       ,
                                                                 expected_type = "non-empty string"           ,
                                                                 message       = "String data cannot be empty")
            raise HTTPException(status_code=400, detail=error.json())

        if not cache_key:
            error = self.store_service().get_invalid_input_error(field_name    = "cache_key"                              ,
                                                                 expected_type = "valid file path"                        ,
                                                                 message       = "Cache key is required for this endpoint")
            raise HTTPException(status_code=400, detail=error.json())

        result = self.store_service().store_string(data      = data       ,                               # Use service layer
                                                   strategy  = strategy   ,                               # todo: we should we using a Type_Safe class for these params
                                                   namespace = namespace  ,
                                                   cache_key = cache_key  ,
                                                   file_id   = file_id    )

        if result is None:
            raise HTTPException(status_code=500, detail="Failed to store data")

        return result

    def store__json(self, data     : dict,                                                                          # JSON can be empty object, that's valid
                          strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                          namespace : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE
                     ) -> Schema__Cache__Store__Response:                                                           # Store JSON data

        result = self.store_service().store_json(data      = data     ,                                               # Use service layer
                                               strategy  = strategy ,                                               # todo: we should we using a Type_Safe class for these params
                                               namespace = namespace)

        if result is None:
            raise HTTPException(status_code=500, detail="Failed to store data")

        return result

    @route_path("/store/json/{cache_key:path}")
    def store__json__cache_key(self, data            : dict                         = Body(...)                     ,
                                     namespace       : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE    ,
                                     strategy        : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                                     cache_key       : Safe_Str__File__Path         = None                          ,
                                     file_id         : Safe_Str__Id                 = None                          ,
                                     json_field_path : Safe_Str__Json__Field_Path   = None
                                ) -> Schema__Cache__Store__Response:                                                # Store JSON with semantic key

        if not cache_key:                                                                                           # todo: check this path, since I think this path can't be reached (due to how FastAPI handles routes)
            error = self.store_service().get_invalid_input_error(field_name    = "cache_key",
                                                               expected_type = "valid file path",
                                                               message       = "Cache key is required for this endpoint")
            raise HTTPException(status_code=400, detail=error.json())

        result = self.store_service().store_json(data            = data     ,                                               # Use service layer
                                                 strategy        = strategy ,                                               # todo: we should we using a Type_Safe class for these params
                                                 namespace       = namespace,
                                                 cache_key       = cache_key,
                                                 json_field_path = json_field_path,
                                                 file_id         = file_id  )

        if result is None:
            raise HTTPException(status_code=500, detail="Failed to store data")

        return result

    def store__binary(self, request  : Request,
                            body     : bytes                        = Body(..., media_type="application/octet-stream"),
                            strategy : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                            namespace: Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE
                       ) -> Schema__Cache__Store__Response:                                                         # Store binary data

        if not body:                                                                                                # Validate input
            error = self.store_service().get_invalid_input_error(field_name    = "body"                       ,
                                                               expected_type = "non-empty binary data"      ,
                                                               message       = "Binary data cannot be empty")
            raise HTTPException(status_code=400, detail=error.json())

        # todo: we need to refactor this gzip/encoding support into separate endpoint
        content_encoding = request.headers.get('content-encoding')                                                  # Check for compression

        result = self.store_service().store_binary(data             = body            ,                               # Use service layer
                                                 strategy         = strategy        ,                               # todo: we should we using a Type_Safe class for these params
                                                 namespace        = namespace       ,
                                                 content_encoding = content_encoding)

        if result is None:
            raise HTTPException(status_code=500, detail="Failed to store data")

        return result

    @route_path("/store/binary/{cache_key:path}")
    def store__binary__cache_key(self, body       : bytes                        = Body(..., media_type="application/octet-stream"),
                                       namespace  : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE      ,
                                       strategy   : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                                       cache_key  : Safe_Str__File__Path         = None,
                                       file_id    : Safe_Str__Id                 = None,
                                       request    : Request                      = None
                                  ) -> Schema__Cache__Store__Response:                                              # Store binary with semantic key

        if not body:                                                                                                # Validate input
            error = self.store_service().get_invalid_input_error(field_name    = "body"                       ,       # todo: see if we should support this case where the files are empty
                                                               expected_type = "non-empty binary data"      ,
                                                               message       = "Binary data cannot be empty")
            raise HTTPException(status_code=400, detail=error.json())

        if not cache_key:
            error = self.store_service().get_invalid_input_error(field_name    = "cache_key"                              ,
                                                               expected_type = "valid file path"                        ,
                                                               message       = "Cache key is required for this endpoint")
            raise HTTPException(status_code=400, detail=error.json())

        # todo: we need to refactor this gzip/encoding support into separate endpoint
        content_encoding = request.headers.get('content-encoding') if request else None                             # Check for compression

        result = self.store_service().store_binary(data             = body            ,                               # Use service layer
                                                   strategy         = strategy        ,                               # todo: we should we using a Type_Safe class for these params
                                                   namespace        = namespace       ,
                                                   cache_key        = cache_key       ,
                                                   file_id          = file_id         ,
                                                   content_encoding = content_encoding)

        if result is None:
            raise HTTPException(status_code=500, detail="Failed to store data")

        return result

    def setup_routes(self):                                                             # Configure all routes
        self.add_route_post(self.store__string              )                           # String endpoints
        self.add_route_post(self.store__string__cache_key   )

        self.add_route_post(self.store__json                )
        self.add_route_post(self.store__json__cache_key     )                           # JSON endpoints

        self.add_route_post(self.store__binary              )                           # Binary endpoints
        self.add_route_post(self.store__binary__cache_key   )