from fastapi                                                                               import HTTPException
from osbot_fast_api.api.routes.Fast_API__Routes                                            import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                              import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                                 import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                          import cache_on_self
from osbot_utils.type_safe.primitives.core.Safe_UInt                                       import Safe_UInt
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path          import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                      import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id            import Safe_Str__Id
from mgraph_ai_service_cache.schemas.cache.data.Schema__Cache__Data__Retrieve__Request     import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type                    import Enum__Cache__Data_Type
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                  import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Retrieve             import Cache__Service__Data__Retrieve

TAG__ROUTES_DELETE__DATA       = Safe_Str__Fast_API__Route__Tag('delete-data')
PREFIX__ROUTES_DELETE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_DELETE__DATA = f'{PREFIX__ROUTES_DELETE__DATA}/{TAG__ROUTES_DELETE__DATA}/'
ROUTES_PATHS__DELETE__DATA     = [ BASE_PATH__ROUTES_DELETE__DATA + 'file',
                                   BASE_PATH__ROUTES_DELETE__DATA + 'all' ]


class Routes__Data__Delete(Fast_API__Routes):                                                   # FastAPI routes for deleting data files from cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_DELETE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_DELETE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def retrieve_service(self) -> Cache__Service__Data__Retrieve:                               # Reuse retrieve service for delete operations
        return Cache__Service__Data__Retrieve(cache_service=self.cache_service)

    def delete__data__file(self, cache_id     : Random_Guid             = None                          ,
                                 namespace    : Safe_Str__Id            = FAST_API__PARAM__NAMESPACE    ,
                                 data_key     : Safe_Str__File__Path    = None                          ,
                                 data_file_id : Safe_Str__Id            = None                          ,
                                 data_type    : Enum__Cache__Data_Type = None
                          ) -> dict:                                                                     # Delete specific data file from cache entry
        try:
            if not data_file_id:
                raise ValueError("data_file_id is required to delete a specific data file")

            deleted   = False
            data_type_used = None

            if not data_type:                                                                           # Try to detect type if not provided
                for try_type in [Enum__Cache__Data_Type.JSON   ,
                                Enum__Cache__Data_Type.STRING ,
                                Enum__Cache__Data_Type.BINARY ]:
                    request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id    ,
                                                                     data_type    = try_type    ,
                                                                     data_key     = data_key    ,
                                                                     data_file_id = data_file_id,
                                                                     namespace    = namespace   )
                    if self.retrieve_service().delete_data(request):
                        deleted = True
                        data_type_used = try_type
                        break
            else:
                request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id    ,
                                                                 data_type    = data_type   ,
                                                                 data_key     = data_key    ,
                                                                 data_file_id = data_file_id,
                                                                 namespace    = namespace   )
                deleted = self.retrieve_service().delete_data(request)
                data_type_used = data_type

                if not deleted:
                    error_detail = { "error_type"   : "NOT_FOUND"                        ,
                                    "message"       : f"Data file {data_file_id} not found",
                                    "cache_id"      : str(cache_id)                     ,
                                    "data_file_id"  : str(data_file_id)                 }
                    raise HTTPException(status_code=404, detail=error_detail)

            return { "status"        : "success"                               ,
                    "message"       : "Data file deleted successfully"        ,
                    "cache_id"      : str(cache_id)                           ,
                    "data_file_id"  : str(data_file_id)                       ,
                    "data_type"     : str(data_type_used) if data_type_used else None,
                    "namespace"     : str(namespace)                          }

        except ValueError as e:
            error_detail = { "error_type" : "INVALID_INPUT",
                            "message"    : str(e)          }
            raise HTTPException(status_code=400, detail=error_detail)

    def delete__all__data__files(self, cache_id  : Random_Guid          = None                          ,
                                       namespace : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                       data_key  : Safe_Str__File__Path = None
                                ) -> dict:                                                               # Delete all data files for cache entry
        try:
            file_refs = self.retrieve_service().retrieve_service().retrieve_by_id__refs(cache_id = cache_id ,
                                                                                        namespace = namespace)

            if not file_refs or not file_refs.file_paths.data_folders:
                return { "status"        : "success"                      ,
                        "message"       : "No data files to delete"      ,
                        "cache_id"      : str(cache_id)                  ,
                        "deleted_count" : 0                              ,
                        "namespace"     : str(namespace)                 }

            handler        = self.cache_service.get_or_create_handler(namespace)
            deleted_count  = Safe_UInt(0)
            deleted_files  = []

            for data_folder in file_refs.file_paths.data_folders:                                      # Process each data folder
                if data_key:
                    target_folder = Safe_Str__File__Path(f"{data_folder}/{data_key}")                  # Delete only files under specific data_key
                else:
                    target_folder = data_folder                                                        # Delete all data files

                if hasattr(handler.storage_backend, 'folder__files'):                                  # List and delete all files in folder
                    files = handler.storage_backend.folder__files(folder_path      = target_folder,
                                                                  return_full_path = True         )

                    for file_path in files:
                        if handler.storage_backend.file__delete(file_path):
                            deleted_count += 1
                            deleted_files.append(str(file_path))

            return { "status"        : "success"                              ,
                    "message"       : f"Deleted {deleted_count} data files"  ,
                    "cache_id"      : str(cache_id)                          ,
                    "deleted_count" : int(deleted_count)                     ,
                    "deleted_files" : deleted_files                          ,
                    "namespace"     : str(namespace)                         }

        except ValueError as e:
            error_detail = { "error_type" : "INVALID_INPUT",
                            "message"    : str(e)          }
            raise HTTPException(status_code=400, detail=error_detail)

    def setup_routes(self):                                                     # Configure all data deletion routes
        self.add_route_delete(self.delete__data__file       )
        self.add_route_delete(self.delete__all__data__files )