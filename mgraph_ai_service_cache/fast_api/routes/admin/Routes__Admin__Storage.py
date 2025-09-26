from fastapi                                                                      import Response
from typing                                                                       import List, Dict, Any
from osbot_fast_api.api.decorators.route_path                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                   import Fast_API__Routes
from osbot_utils.decorators.methods.cache_on_self                                 import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now  import Timestamp_Now
from osbot_utils.utils.Json                                                       import bytes_to_json, json_to_str
from mgraph_ai_service_cache_client.schemas.cache.consts__Cache_Service                  import DEFAULT__HTTP_CODE__FILE_NOT_FOUND
from mgraph_ai_service_cache.service.cache.Cache__Service                         import Cache__Service

TAG__ROUTES_STORAGE   = 'admin/storage'

# todo:review the use of namespace in path, since it creates an inconsistency without adding a lot of value
ROUTES_PATHS__STORAGE = [ f'/{TAG__ROUTES_STORAGE}/bucket-name'                          ,  # Current bucket name
                          f'/{TAG__ROUTES_STORAGE}/file/exists/{{path:path}}'            ,  # File Exists
                          f'/{TAG__ROUTES_STORAGE}/file/bytes/{{path:path}}'             ,  # File contents (as bytes)
                          f'/{TAG__ROUTES_STORAGE}/file/json/{{path:path}}'              ,  # File contents (as json)
                          f'/{TAG__ROUTES_STORAGE}/files/parent-path'                    ,  # Files Parent Path
                          f'/{TAG__ROUTES_STORAGE}/files/all/{{path:path}}'              ,  # Files All Path
                          f'/{TAG__ROUTES_STORAGE}/folders'                              ,  # Folders
                          f'/{TAG__ROUTES_STORAGE}/{{path:path}}'                        ]  # File Delete

# todo: move to different Fast_API server/endpoint and add admin authorization
class Routes__Admin__Storage(Fast_API__Routes):
    tag           : str          = TAG__ROUTES_STORAGE
    cache_service : Cache__Service

    @cache_on_self
    def storage_fs(self):
        return self.cache_service.storage_fs()

    def bucket_name(self):
        storage = self.storage_fs()
        if hasattr(storage, 's3_bucket'):
            bucket_name = storage.s3_bucket
        else:
            bucket_name = 'NA'
        return {'bucket-name': bucket_name }

    @route_path("/file/exists/{path:path}")
    def file__exists(self, path):
        exists = self.storage_fs().file__exists(path)
        return dict(exists = exists,
                    path  =  path )

    @route_path("/file/bytes/{path:path}")
    def file__bytes(self, path):
        file_bytes = self.storage_fs().file__bytes(path)
        return Response(file_bytes, media_type='application/octet-stream')

    @route_path("/file/json/{path:path}")
    def file__json(self, path):
        file_bytes = self.storage_fs().file__bytes(path)
        if file_bytes:
            file_json   = bytes_to_json(file_bytes)
            return file_json
        error_data    = dict( error_type = "FILE_NOT_FOUND" ,
                              resource   = "file"           ,
                              message    = "The requested file does not exist in storage",
                              path       = path             )

        return Response(json_to_str(error_data)                           ,
                        status_code = DEFAULT__HTTP_CODE__FILE_NOT_FOUND  ,          # using 404 to indicate file not found
                        media_type  = 'application/json')

    def files__parent_path(self, path            : Safe_Str__File__Path = ''   ,
                                 return_full_path: bool                 = False
                            ) -> List:
        folders = self.storage_fs().folder__files(folder_path=path, return_full_path=return_full_path)
        return folders

    # todo: as it is this could have major performance implications (for large namespaces
    #       this is also a good example of the kind of method/capability that we could run on a schedule and return the cached version

    @route_path("/files/all/{path:path}")
    def files__all__path(self, path     : Safe_Str__File__Path = ''
                          ) -> dict:
        files      = self.storage_fs().folder__files__all(parent_folder=path)
        file_count = len(files)
        return dict(timestamp  = Timestamp_Now(),
                    file_count = file_count     ,
                    files      = sorted(files)  )

    def folders(self, path            : Safe_Str__File__Path = ''   ,
                      return_full_path: bool                 = False
                 ) -> List:
        folders = self.storage_fs().folder__folders(parent_folder=path, return_full_path=return_full_path)
        return folders

    @route_path('/{path:path}')
    def delete__file(self, path : Safe_Str__File__Path) -> Dict[str, Any]:
        return self.cache_service.storage_fs().file__delete(path)


    def setup_routes(self):
        self.add_route_get   (self.bucket_name             )
        self.add_route_get   (self.file__exists            )
        self.add_route_get   (self.file__bytes             )
        self.add_route_get   (self.file__json              )
        self.add_route_get   (self.files__parent_path      )
        self.add_route_get   (self.files__all__path        )
        self.add_route_get   (self.folders                 )

        self.add_route_delete(self.delete__file    )
