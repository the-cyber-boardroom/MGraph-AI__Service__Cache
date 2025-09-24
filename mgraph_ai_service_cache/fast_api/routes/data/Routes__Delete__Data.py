from fastapi                                                                       import HTTPException
from osbot_fast_api.api.decorators.route_path                                      import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                    import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                      import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                         import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                  import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path  import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid              import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id    import Safe_Str__Id
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                        import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                          import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve__Data import Cache__Service__Retrieve__Data

TAG__ROUTES_DELETE__DATA       = Safe_Str__Fast_API__Route__Tag('delete-data')
PREFIX__ROUTES_DELETE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_DELETE__DATA = f'{PREFIX__ROUTES_DELETE__DATA}/{TAG__ROUTES_DELETE__DATA}/'
ROUTES_PATHS__DELETE__DATA     = [ BASE_PATH__ROUTES_DELETE__DATA + 'file',
                                   BASE_PATH__ROUTES_DELETE__DATA + 'all' ]


class Routes__Delete__Data(Fast_API__Routes):                                                   # FastAPI routes for deleting data files
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_DELETE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_DELETE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def retrieve_service(self) -> Cache__Service__Retrieve__Data:                               # Reuse retrieve service for delete ops
        return Cache__Service__Retrieve__Data(cache_service=self.cache_service)

    @route_path("/delete/data/file")
    def delete__data__file(self, cache_id     : Random_Guid                                   ,
                                 namespace    : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                 data_key     : Safe_Str__File__Path         = None           ,
                                 data_file_id : Safe_Str__Id                 = None
                           ) -> dict:                                                            # Delete specific data file
        try:
            if not data_file_id:
                raise ValueError("data_file_id is required to delete a specific data file")

            deleted = self.retrieve_service().delete_data_file(cache_id     = cache_id        ,
                                                              data_key     = data_key          ,
                                                              data_file_id = data_file_id      ,
                                                              namespace    = namespace        )

            if not deleted:
                raise HTTPException(status_code = 404                                         ,
                                   detail   = { "error_type"     : "NOT_FOUND"               ,
                                               "message"        : f"Data file {data_file_id} not found",
                                               "cache_id"       : str(cache_id)              ,
                                               "data_file_id"   : str(data_file_id)          })

            return { "status"        : "success"                            ,
                    "message"       : "Data file deleted successfully"     ,
                    "cache_id"      : str(cache_id)                        ,
                    "data_file_id"  : str(data_file_id)                    ,
                    "namespace"     : str(namespace)                       }

        except ValueError as e:
            raise HTTPException(status_code = 400                       ,
                               detail   = { "error_type" : "INVALID_INPUT",
                                           "message"    : str(e)          })

    @route_path("/delete/data/all")
    def delete__all__data__files(self, cache_id  : Random_Guid                                ,
                                       namespace : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                       data_key  : Safe_Str__File__Path         = None
                                 ) -> dict:                                                      # Delete all data files for cache entry
        try:
            deleted_count = self.retrieve_service().delete_all_data_files(cache_id  = cache_id,
                                                                         data_key  = data_key  ,
                                                                         namespace = namespace)

            return { "status"         : "success"                                ,
                    "message"        : f"Deleted {deleted_count} data files"   ,
                    "cache_id"       : str(cache_id)                           ,
                    "deleted_count"  : int(deleted_count)                      ,
                    "namespace"      : str(namespace)                          }

        except ValueError as e:
            raise HTTPException(status_code = 400                       ,
                               detail   = { "error_type" : "INVALID_INPUT",
                                           "message"    : str(e)          })

    def setup_routes(self) -> 'Routes__Delete__Data':                                           # Configure all data deletion routes
        self.add_route_delete(self.delete__data__file       )
        self.add_route_delete(self.delete__all__data__files )
        return self