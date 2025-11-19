from fastapi                                                                                                 import Response
from typing                                                                                                  import List, Dict, Any
from osbot_fast_api.api.decorators.route_path                                                                import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                                              import Fast_API__Routes
from osbot_utils.decorators.methods.cache_on_self                                                            import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path                            import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now                             import Timestamp_Now
from osbot_utils.utils.Json                                                                                  import bytes_to_json, json_to_str
from mgraph_ai_service_cache_client.schemas.cache.consts__Cache_Service                                      import DEFAULT__HTTP_CODE__FILE_NOT_FOUND
from mgraph_ai_service_cache.service.cache.Cache__Service                                                    import Cache__Service
from mgraph_ai_service_cache_client.schemas.routes.admin.Schema__Routes__Admin__Storage__Files_All__Response import Schema__Routes__Admin__Storage__Files_All__Response

TAG__ROUTES_STORAGE   = 'admin/storage'

# todo:review the use of namespace in path, since it creates an inconsistency without adding a lot of value
ROUTES_PATHS__STORAGE = [ f'/{TAG__ROUTES_STORAGE}/bucket-name'                          ,  # Current bucket name
                          f'/{TAG__ROUTES_STORAGE}/file/exists/{{path:path}}'            ,  # File Exists
                          f'/{TAG__ROUTES_STORAGE}/file/bytes/{{path:path}}'             ,  # File contents (as bytes)
                          f'/{TAG__ROUTES_STORAGE}/file/json/{{path:path}}'              ,  # File contents (as json)
                          f'/{TAG__ROUTES_STORAGE}/files/in/{{path:path}}'               ,  # Files in Path
                          f'/{TAG__ROUTES_STORAGE}/files/all/{{path:path}}'              ,  # Files all Path
                          f'/{TAG__ROUTES_STORAGE}/folders/{{path:path}}'                ,  # Folders
                          f'/{TAG__ROUTES_STORAGE}/file/delete/{{path:path}}'            ]  # File Delete

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

    @route_path("/files/in/{path:path}")
    def files__in(self,                                               # List files in path - optionally recursive
                  path            : Safe_Str__File__Path = ''   ,
                  return_full_path: bool                 = False ,
                  recursive       : bool                 = False
              ) -> List:

        if recursive:                                                                                       # Use recursive method - returns all files under path
            files = self.storage_fs().folder__files__all(parent_folder=path)                                # Note: folder__files__all always returns full paths
            return sorted(files)
        else:
            files = self.storage_fs().folder__files(folder_path=path, return_full_path=return_full_path)    # Use non-recursive method - returns files in directory only
            return files

    # todo: as it is this could have major performance implications (for large namespaces
    #       this is also a good example of the kind of method/capability that we could run on a schedule and return the cached version
    # todo: see if we need this since we can also get all files recursively using files__in__path
    @route_path("/files/all/{path:path}")
    def files__all(self,
                   path     : Safe_Str__File__Path = ''
              ) -> Schema__Routes__Admin__Storage__Files_All__Response:
        files      = self.storage_fs().folder__files__all(parent_folder=path)
        file_count = len(files)
        return Schema__Routes__Admin__Storage__Files_All__Response(timestamp  = Timestamp_Now(),
                                                                   file_count = file_count     ,
                                                                   files      = sorted(files)  )

    # todo: move the logic of this folders_path into a services method (since this type of logic should not be in the Routes class
    @route_path('/folders/{path:path}')
    def folders(self,
                path            : Safe_Str__File__Path = ''    ,                                    # List folders in path - optionally recursive
                return_full_path: bool                 = False ,
                recursive       : bool                 = False
          ) -> List:
        if recursive:                                                                                           # Recursive: get all folders under path
            all_folders = []
            folders = self.storage_fs().folder__folders(parent_folder=path, return_full_path=True)
            all_folders.extend(folders)

            for folder in folders:                                                                              # Recursively get subfolders
                subfolders = self.folders(path=folder, return_full_path=True, recursive=True)
                all_folders.extend(subfolders)

            result = sorted(list(set(all_folders)))                                                               # Remove duplicates and sort
            if not return_full_path and path:
                base_path = path if path.endswith('/') else path + '/'                                                # Remove the base path prefix from all results
                result = [folder.replace(base_path, '', 1) if folder.startswith(base_path) else folder
                          for folder in result ]
            return result
        else:
            folders = self.storage_fs().folder__folders(parent_folder=path, return_full_path=return_full_path)  # Non-recursive: get folders in this directory only
            return folders

    @route_path('/file/delete/{path:path}')
    def file__delete(self,
                     path : Safe_Str__File__Path
                ) -> Dict:
        result = self.cache_service.storage_fs().file__delete(path)
        return dict(path    = path,
                    deleted = result )


    def setup_routes(self):
        self.add_route_get   (self.bucket_name  )
        self.add_route_get   (self.file__exists )
        self.add_route_get   (self.file__bytes  )
        self.add_route_get   (self.file__json   )
        self.add_route_get   (self.files__in    )
        self.add_route_get   (self.files__all   )
        self.add_route_get   (self.folders      )
        self.add_route_delete(self.file__delete )
