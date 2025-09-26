from fastapi                                                                             import HTTPException, Response, Body
from osbot_fast_api.api.decorators.route_path                                            import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                            import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                               import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                        import cache_on_self
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                              import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Store                import Cache__Service__Zip__Store
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Operations           import Cache__Service__Zip__Operations
from mgraph_ai_service_cache.service.cache.zip.Cache__Service__Zip__Batch                import Cache__Service__Zip__Batch
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Request        import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Response       import Schema__Cache__Zip__Store__Response
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Request    import Schema__Cache__Zip__Operation__Request
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Operation__Response   import Schema__Cache__Zip__Operation__Response
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Request        import Schema__Cache__Zip__Batch__Request
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Batch__Response       import Schema__Cache__Zip__Batch__Response


TAG__ROUTES_ZIP    = Safe_Str__Fast_API__Route__Tag('zip')
PREFIX__ROUTES_ZIP = Safe_Str__Fast_API__Route__Prefix('/{namespace}')
BASE_PATH__ZIP     = f'{PREFIX__ROUTES_ZIP}/{TAG__ROUTES_ZIP}'

ROUTES_PATHS__ZIP = [ f'{BASE_PATH__ZIP}/store'                       ,    # Store new zip
                     f'{BASE_PATH__ZIP}/{{cache_id}}/list'           ,    # List files in zip
                     f'{BASE_PATH__ZIP}/{{cache_id}}/file/'           ,    # Get/add/remove single file
                     f'{BASE_PATH__ZIP}/{{cache_id}}/batch'          ,    # Batch operations
                     f'{BASE_PATH__ZIP}/{{cache_id}}/download'       ]    # Download entire zip


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

    @route_path("/zip/store")
    def store_zip(self, body      : bytes = Body(..., media_type="application/zip"),
                       namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE,
                       cache_key  : Safe_Str__File__Path = None,
                       file_id    : Safe_Str__Id = None
                  ) -> Schema__Cache__Zip__Store__Response:                # Store a new zip file

        request = Schema__Cache__Zip__Store__Request(zip_bytes = body        ,
                                                     cache_key = cache_key   ,
                                                     file_id   = file_id     ,
                                                     namespace = namespace   )
        try:
            return self.zip_store_service().store_zip(request)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to store zip: {str(e)}")

    @route_path("/zip/{cache_id}/list")
    def list_zip_files(self, cache_id : Random_Guid,
                             namespace: Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                        ) -> Schema__Cache__Zip__Operation__Response:       # List files in stored zip

        request = Schema__Cache__Zip__Operation__Request(cache_id  = cache_id   ,
                                                         operation = "list"     ,
                                                         namespace = namespace  )

        result = self.zip_ops_service().perform_operation(request)

        if not result.success:
            raise HTTPException(status_code=404, detail=result.error_details)

        return result

    @route_path("/zip/{cache_id}/file")
    def get_zip_file(self, cache_id  : Random_Guid,
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

    @route_path("/zip/{cache_id}/file/{file_path:path}")
    def add_zip_file(self, cache_id  : Random_Guid,
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

    @route_path("/zip/{cache_id}/file/{file_path:path}")
    def remove_zip_file(self, cache_id  : Random_Guid,
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

    @route_path("/zip/{cache_id}/batch")
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

    @route_path("/zip/{cache_id}/download")
    def download_zip(self, cache_id : Random_Guid,
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
        self.add_route_post  (self.store_zip        )
        self.add_route_get   (self.list_zip_files   )
        self.add_route_get   (self.get_zip_file     )
        self.add_route_post  (self.add_zip_file     )
        self.add_route_delete(self.remove_zip_file  )
        self.add_route_post  (self.batch_operations )
        self.add_route_get   (self.download_zip     )
