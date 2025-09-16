from typing import List

from osbot_fast_api.api.routes.Fast_API__Routes import Fast_API__Routes
from osbot_utils.decorators.methods.cache_on_self import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path

from mgraph_ai_service_cache.service.cache.Cache__Service import Cache__Service

TAG__ROUTES_STORAGE                  = 'admin/storage'
ROUTES_PATHS__STORAGE                = [ f'/{TAG__ROUTES_STORAGE}/folders' ]

# todo: move to different Fast_API server/endpoint and add admin authorization
class Routes__Storage(Fast_API__Routes):
    tag           : str          = TAG__ROUTES_STORAGE
    cache_service : Cache__Service

    @cache_on_self
    def storage_fs(self):
        return self.cache_service.storage_fs()

    def folders(self, parent_folder: Safe_Str__File__Path='/',
                      return_full_path: bool = False
                  ) -> List:

        return self.storage_fs().folder_list(parent_folder=parent_folder, return_full_path=return_full_path)

    def setup_routes(self):
        self.add_route_get(self.folders)
