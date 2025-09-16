from fastapi                                                                      import Response
from typing                                                                       import List
from osbot_fast_api.api.decorators.route_path                                     import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                   import Fast_API__Routes
from osbot_utils.decorators.methods.cache_on_self                                 import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                 import Safe_Id
from osbot_utils.type_safe.primitives.domains.identifiers.Timestamp_Now           import Timestamp_Now
from osbot_utils.utils.Files                                                      import path_combine_safe
from osbot_utils.utils.Json                                                       import bytes_to_json
from mgraph_ai_service_cache.service.cache.Cache__Service                         import Cache__Service

TAG__ROUTES_STORAGE   = 'admin/storage'

# todo:review the use of namespace in path, since it creates an inconsistency without adding a lot of value
ROUTES_PATHS__STORAGE = [ f'/{TAG__ROUTES_STORAGE}/bucket-name'                          ,  # Current bucket name
                          f'/{TAG__ROUTES_STORAGE}/file/exists/{{path:path}}'            ,  # File Exists
                          f'/{TAG__ROUTES_STORAGE}/file/bytes/{{path:path}}'             ,  # File contents (as bytes)
                          f'/{TAG__ROUTES_STORAGE}/file/json/{{path:path}}'              ,  # File contents (as json)
                          f'/{TAG__ROUTES_STORAGE}/files/parent-path'                    ,  # Files Parent Path
                          f'/{TAG__ROUTES_STORAGE}/{{namespace}}/files/all'              ,  # Namespace Files All
                          f'/{TAG__ROUTES_STORAGE}/{{namespace}}/files/all/{{path:path}}',  # Files All Path
                          f'/{TAG__ROUTES_STORAGE}/folders'                              ]  # Folders]

# todo: move to different Fast_API server/endpoint and add admin authorization
class Routes__Storage(Fast_API__Routes):
    tag           : str          = TAG__ROUTES_STORAGE
    cache_service : Cache__Service

    @cache_on_self
    def storage_fs(self):
        return self.cache_service.storage_fs()

    def bucket_name(self):
        return self.storage_fs().s3_bucket

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
        file_json   = bytes_to_json(file_bytes)
        return file_json

    def files__parent_path(self, path            : Safe_Str__File__Path = ''   ,
                                 return_full_path: bool                 = False
                            ) -> List:
        folders = self.storage_fs().folder__files(folder_path=path, return_full_path=return_full_path)
        return folders

    # todo: as it is this could have major performance implications (for large namespaces
    #       this is also a good example of the kind of method/capability that we could run on a schedule and return the cached version

    def __namespace__files__all(self, namespace: Safe_Id) -> dict:
        return self.files__all__path(namespace=namespace)

    @route_path("/{namespace}/files/all/{path:path}")
    def files__all__path(self, namespace: Safe_Id              = 'default' ,
                               path     : Safe_Str__File__Path = ''
                          ) -> dict:
        parent_folder = path_combine_safe(str(namespace), str(path))
        files      = self.storage_fs().folder__files__all(parent_folder=parent_folder)
        file_count = len(files)
        return dict(timestamp  = Timestamp_Now(),
                    file_count = file_count     ,
                    files      = sorted(files)  )

    def folders(self, path            : Safe_Str__File__Path = ''   ,
                      return_full_path: bool                 = False
                 ) -> List:
        folders = self.storage_fs().folder__folders(parent_folder=path, return_full_path=return_full_path)
        return folders



    def setup_routes(self):
        self.add_route_get(self.bucket_name             )
        self.add_route_get(self.file__exists            )
        self.add_route_get(self.file__bytes             )
        self.add_route_get(self.file__json              )
        self.add_route_get(self.files__parent_path      )
        self.add_route_get(self.__namespace__files__all )
        self.add_route_get(self.files__all__path        )
        self.add_route_get(self.folders                 )
