from fastapi                                                                                    import HTTPException, Response, Body
from osbot_fast_api.api.decorators.route_path                                                   import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                 import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                      import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                         import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                               import cache_on_self
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                           import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                 import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path               import Safe_Str__File__Path
from osbot_utils.utils.Zip                                                                      import zip_bytes_empty
from mgraph_ai_service_cache_client.schemas.cache.consts__Cache_Service                         import DEFAULT_CACHE__ZIP__STRATEGY
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                              import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store                       import Cache__Service__Zip__Store
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Operations                  import Cache__Service__Zip__Operations
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Batch                       import Cache__Service__Zip__Batch
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Store__Request        import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Store__Response       import Schema__Cache__Zip__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Request    import Schema__Cache__Zip__Operation__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Response   import Schema__Cache__Zip__Operation__Response
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Batch__Request        import Schema__Cache__Zip__Batch__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Batch__Response       import Schema__Cache__Zip__Batch__Response


TAG__ROUTES_ZIP    = Safe_Str__Fast_API__Route__Tag('zip')
PREFIX__ROUTES_ZIP = Safe_Str__Fast_API__Route__Prefix('/{namespace}')
BASE_PATH__ZIP     = f'{PREFIX__ROUTES_ZIP}/{TAG__ROUTES_ZIP}'

ROUTES_PATHS__ZIP = [ f'{PREFIX__ROUTES_ZIP}/{{strategy}}/zip/create/{{cache_key:path}}/{{file_id}}'        ,    # create new zip (empty)
                      f'{PREFIX__ROUTES_ZIP}/{{strategy}}/zip/store/{{cache_key:path}}/{{file_id}}'          ,    # Store new zip
                      f'{BASE_PATH__ZIP}/{{cache_id}}/files/list'                                       ,    # List files in zip
                      f'{BASE_PATH__ZIP}/{{cache_id}}/file/retrieve/{{file_path:path}}'                 ,    # Get specific file from zip
                      f'{BASE_PATH__ZIP}/{{cache_id}}/file/add/from/string/{{file_path:path}}'          ,    # Add file from string
                      f'{BASE_PATH__ZIP}/{{cache_id}}/file/add/from/bytes/{{file_path:path}}'           ,    # Add file from bytes
                      f'{BASE_PATH__ZIP}/{{cache_id}}/file/delete/{{file_path:path}}'                   ,    # Delete file from zip
                      f'{BASE_PATH__ZIP}/{{cache_id}}/batch/operations'                                 ,    # Batch operations
                      f'{BASE_PATH__ZIP}/{{cache_id}}/retrieve'                                         ]    # Retrieve entire zip

class Routes__Zip(Fast_API__Routes):                                       # FastAPI routes for zip operations
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_ZIP
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_ZIP
    cache_service : Cache__Service                                         # Dependency injection

    @cache_on_self
    def zip_store_service(self) -> Cache__Service__Zip__Store:             # Service for storing zips
        return Cache__Service__Zip__Store(cache_service=self.cache_service)

    @cache_on_self
    def zip_ops_service(self) -> Cache__Service__Zip__Operations:          # Service for zip operations
        return Cache__Service__Zip__Operations(cache_service=self.cache_service)

    @cache_on_self
    def zip_batch_service(self) -> Cache__Service__Zip__Batch:             # Service for batch operations
        return Cache__Service__Zip__Batch(cache_service=self.cache_service)

    @route_path("/{strategy}/zip/create/{cache_key:path}/{file_id}")
    def zip_create(self,namespace  : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE  ,
                        strategy   : Enum__Cache__Store__Strategy = DEFAULT_CACHE__ZIP__STRATEGY,
                        cache_key  : Safe_Str__File__Path         = None,
                        file_id    : Safe_Str__Id                 = None
                   ) -> Schema__Cache__Zip__Store__Response:                # Store a new zip file

        zip_bytes = zip_bytes_empty()
        request = Schema__Cache__Zip__Store__Request(zip_bytes = zip_bytes        ,
                                                     cache_key = cache_key   ,
                                                     file_id   = file_id     ,
                                                     namespace = namespace   ,
                                                     strategy = strategy     )
        return self.zip_store_service().store_zip(request)


    @route_path("/{strategy}/zip/store/{cache_key:path}/{file_id}")
    def zip_store(self, body       : bytes = Body(..., media_type="application/zip"),
                        namespace  : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE     ,
                        strategy   : Enum__Cache__Store__Strategy = DEFAULT_CACHE__ZIP__STRATEGY   ,
                        cache_key  : Safe_Str__File__Path         = None,
                        file_id    : Safe_Str__Id                 = None
                   ) -> Schema__Cache__Zip__Store__Response:                # Store a new zip file

        request = Schema__Cache__Zip__Store__Request(zip_bytes = body        ,
                                                     cache_key = cache_key   ,
                                                     file_id   = file_id     ,
                                                     strategy  = strategy    ,
                                                     namespace = namespace   )
        response = self.zip_store_service().store_zip(request)
        if not response.success:                                                        # Route decides the HTTP semantics
            if response.error_type == "INVALID_INPUT":                                  # todo: move these strings to Enum
                raise HTTPException(status_code=400, detail=response.error_message)
            elif response.error_type == "INVALID_ZIP_FORMAT":
                raise HTTPException(status_code=400, detail=response.error_message)
            else:
                raise HTTPException(status_code=500, detail=response.error_message)
        return response

    @route_path("/zip/{cache_id}/files/list")
    def zip_files_list(self, cache_id : Random_Guid,
                             namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                        ) -> Schema__Cache__Zip__Operation__Response:       # List files in stored zip

        request = Schema__Cache__Zip__Operation__Request(cache_id  = cache_id   ,
                                                         operation = "list"     ,
                                                         namespace = namespace  )

        result = self.zip_ops_service().perform_operation(request)

        if not result.success:
            raise HTTPException(status_code=404, detail=result.error_details)

        return result

    @route_path("/zip/{cache_id}/file/retrieve/{file_path:path}")
    def zip_file_retrieve(self, cache_id  : Random_Guid,
                           file_path : Safe_Str__File__Path,
                           namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                      ) -> Response:                                         # Get specific file from zip

        request = Schema__Cache__Zip__Operation__Request(cache_id  = cache_id   ,
                                                         operation = "get"      ,
                                                         file_path = file_path  ,
                                                         namespace = namespace  )

        result = self.zip_ops_service().perform_operation(request)

        if not result.success:
            raise HTTPException(status_code=404, detail=result.error_details)

        return Response(content=result.file_content, media_type="application/octet-stream")

    @route_path("/zip/{cache_id}/file/add/from/string/{file_path:path}")
    def zip_file_add_from_string(self, cache_id  : Random_Guid,
                                       body      : str = Body(...),
                                       file_path : Safe_Str__File__Path = None,
                                       namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                  ) -> Schema__Cache__Zip__Operation__Response:                 # Add file to zip (from string)
        body_as_bytes = body.encode("utf-8")
        return self.zip_file_add_from_bytes(cache_id = cache_id  ,
                                            body = body_as_bytes ,
                                            file_path = file_path ,
                                            namespace = namespace ,)

    @route_path("/zip/{cache_id}/file/add/from/bytes/{file_path:path}")
    def zip_file_add_from_bytes(self, cache_id  : Random_Guid,
                                      body      : bytes = Body(...),
                                      file_path : Safe_Str__File__Path = None,
                                      namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                 ) -> Schema__Cache__Zip__Operation__Response:         # Add file to zip

        if not file_path:
            raise HTTPException(status_code=400, detail="file_path required")

        request = Schema__Cache__Zip__Operation__Request(cache_id     = cache_id   ,
                                                         operation    = "add"      ,
                                                         file_path    = file_path  ,
                                                         file_content = body       ,
                                                         namespace    = namespace)

        result = self.zip_ops_service().perform_operation(request)

        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_details)

        return result

    @route_path("/zip/{cache_id}/file/delete/{file_path:path}")
    def zip_file_delete(self, cache_id  : Random_Guid,
                              file_path : Safe_Str__File__Path,
                              namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                         ) -> Schema__Cache__Zip__Operation__Response:      # Remove file from zip

        request = Schema__Cache__Zip__Operation__Request(cache_id  = cache_id   ,
                                                         operation = "remove"   ,
                                                         file_path = file_path  ,
                                                         namespace = namespace  )

        result = self.zip_ops_service().perform_operation(request)

        if not result.success:
            raise HTTPException(status_code=400, detail=result.error_details)

        return result

    @route_path("/zip/{cache_id}/batch/operations")
    def batch_operations(self, request  : Schema__Cache__Zip__Batch__Request,
                               cache_id : Random_Guid,
                               namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                          ) -> Schema__Cache__Zip__Batch__Response:         # Perform batch operations

        request.cache_id  = cache_id                                        # Ensure cache_id and namespace match  #todo: review this pattern, since it very weird to accept a value that is not used (in this case request.cache_id)
        request.namespace = namespace

        try:
            return self.zip_batch_service().perform_batch(request)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Batch operation failed: {str(e)}")

    @route_path("/zip/{cache_id}/retrieve")
    def zip_retrieve(self, cache_id : Random_Guid,
                           namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                      ) -> Response:                                         # Download entire zip file

        result = self.cache_service.retrieve_by_id(cache_id, namespace)

        if not result:
            raise HTTPException(status_code=404, detail="Zip file not found")

        if result.get('data_type') != 'binary':
            raise HTTPException(status_code=400, detail="Cached item is not a binary file")

        zip_bytes = result.get('data')

        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename={cache_id}.zip"}
        )

    def setup_routes(self):                                                # Configure all routes
        self.add_route_post  (self.zip_create              )
        self.add_route_post  (self.zip_store               )
        self.add_route_get   (self.zip_retrieve            )
        self.add_route_get   (self.zip_files_list          )
        self.add_route_get   (self.zip_file_retrieve       )
        self.add_route_post  (self.zip_file_add_from_bytes )
        self.add_route_post  (self.zip_file_add_from_string)
        self.add_route_delete(self.zip_file_delete         )
        self.add_route_post  (self.batch_operations        )

