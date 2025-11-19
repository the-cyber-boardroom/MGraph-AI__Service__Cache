from fastapi                                                                                      import HTTPException
from osbot_fast_api.api.decorators.route_path                                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                   import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                        import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                           import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                                 import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                 import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                             import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                   import Safe_Str__Id
from mgraph_ai_service_cache_client.schemas.cache.data.Schema__Cache__Data__Retrieve__Request     import Schema__Cache__Data__Retrieve__Request
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Data_Type                    import Enum__Cache__Data_Type
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                                import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                         import Cache__Service
from mgraph_ai_service_cache.service.cache.data.Cache__Service__Data__Delete                      import Cache__Service__Data__Delete

TAG__ROUTES_DELETE__DATA       = Safe_Str__Fast_API__Route__Tag('data')
PREFIX__ROUTES_DELETE__DATA    = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_DELETE__DATA = f'{PREFIX__ROUTES_DELETE__DATA}/{TAG__ROUTES_DELETE__DATA}/'

ROUTES_PATHS__DELETE__DATA = [ BASE_PATH__ROUTES_DELETE__DATA + 'delete/{data_type}/{data_file_id}'                  ,
                               BASE_PATH__ROUTES_DELETE__DATA + 'delete/{data_type}/{data_key:path}/{data_file_id}' ,
                               BASE_PATH__ROUTES_DELETE__DATA + 'delete/all'                                        ,
                               BASE_PATH__ROUTES_DELETE__DATA + 'delete/all/{data_key:path}'                        ]


class Routes__Data__Delete(Fast_API__Routes):                                                   # FastAPI routes for deleting data files from cache entries
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_DELETE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_DELETE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def delete_service(self) -> Cache__Service__Data__Delete:                                   # Service layer for delete operations
        return Cache__Service__Data__Delete(cache_service=self.cache_service)

    @route_path("/data/delete/{data_type}/{data_file_id}")
    def delete__data__file__with__id(self, cache_id     : Random_Guid            = None                          ,
                                           namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                           data_type    : Enum__Cache__Data_Type = None                          ,
                                           data_file_id : Safe_Str__Id           = None
                                     ) -> dict:
        return self.delete__data__file__with__id_and_key(cache_id     = cache_id    ,
                                                         namespace    = namespace   ,
                                                         data_key     = ''          ,
                                                         data_type    = data_type   ,
                                                         data_file_id = data_file_id)

    @route_path("/data/delete/{data_type}/{data_key:path}/{data_file_id}")
    def delete__data__file__with__id_and_key(self, cache_id     : Random_Guid            = None                          ,
                                                   namespace    : Safe_Str__Id           = FAST_API__PARAM__NAMESPACE    ,
                                                   data_type    : Enum__Cache__Data_Type = None                          ,
                                                   data_key     : Safe_Str__File__Path   = None                          ,
                                                   data_file_id : Safe_Str__Id           = None
                                             ) -> dict:                                                                   # Delete specific data file

        if not data_file_id:
            error_detail = { "error_type" : "INVALID_INPUT",
                            "message"    : "data_file_id is required to delete a specific data file"}
            raise HTTPException(status_code=400, detail=error_detail)

        if not data_type:
            error_detail = { "error_type" : "INVALID_INPUT",
                            "message"    : "data_type is required to delete a specific data file"}
            raise HTTPException(status_code=400, detail=error_detail)

        request = Schema__Cache__Data__Retrieve__Request(cache_id     = cache_id    ,
                                                         data_type    = data_type   ,
                                                         data_key     = data_key    ,
                                                         data_file_id = data_file_id,
                                                         namespace    = namespace   )

        deleted = self.delete_service().delete_data_file(request)

        if not deleted:
            error_detail = { "error_type"   : "NOT_FOUND"                                       ,           # todo: refactor to use Type_Safe classes
                             "message"      : f"Data file {data_file_id} not found"             ,
                             "cache_id"     : str(cache_id)                                     ,
                             "data_file_id" : str(data_file_id)                                 ,
                             "data_type"    : data_type.value                                   }
            raise HTTPException(status_code=404, detail=error_detail)

        return { "status"        : "success"                                                    ,           # todo: refactor to use Type_Safe classes
                "message"       : "Data file deleted successfully"                             ,
                "cache_id"      : str(cache_id)                                                ,
                "data_file_id"  : str(data_file_id)                                            ,
                "data_type"     : data_type.value                                              ,
                "data_key"      : str(data_key) if data_key else None                          ,
                "namespace"     : str(namespace)                                               }

    @route_path("/data/delete/all")
    def delete__all__data__files(self, cache_id  : Random_Guid = None                          ,
                                       namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                ) -> dict:
        return self.delete__all__data__files__with__key(cache_id  = cache_id ,
                                                        namespace = namespace,
                                                        data_key  = ''       )

    @route_path("/data/delete/all/{data_key:path}")
    def delete__all__data__files__with__key(self, cache_id  : Random_Guid          = None                          ,
                                                  namespace : Safe_Str__Id         = FAST_API__PARAM__NAMESPACE    ,
                                                  data_key  : Safe_Str__File__Path = None
                                           ) -> dict:                                                               # Delete all data files
        result = self.delete_service().delete_all_data_files(cache_id  = cache_id ,
                                                             namespace = namespace,
                                                             data_key  = data_key )

        deleted_count = result.deleted_count

        if deleted_count == 0:
            return { "status"        : "success"                                                ,               # todo: refactor to use Type_Safe classes
                    "message"       : "No data files to delete"                                ,
                    "cache_id"      : str(cache_id)                                            ,
                    "deleted_count" : 0                                                        ,
                    "data_key"      : str(data_key) if data_key else None                      ,
                    "namespace"     : str(namespace)                                           }

        return { "status"        : "success"                                                   ,
                "message"       : f"Deleted {deleted_count} data files"                       ,                 # todo: refactor to use Type_Safe classes
                "cache_id"      : str(cache_id)                                               ,
                "deleted_count" : deleted_count                                               ,
                "deleted_files" : result.deleted_files                                        ,
                "data_key"      : str(data_key) if data_key else None                         ,
                "namespace"     : str(namespace)                                              }

    def setup_routes(self):                                                                     # Configure all data deletion routes

        self.add_route_delete(self.delete__all__data__files               )                 # IMPORTANT: from a routing point of view, this needs to be added before the delete__data__file__with__id
        self.add_route_delete(self.delete__all__data__files__with__key    )                 #            so that the /all/ is picked up
        self.add_route_delete(self.delete__data__file__with__id           )
        self.add_route_delete(self.delete__data__file__with__id_and_key   )