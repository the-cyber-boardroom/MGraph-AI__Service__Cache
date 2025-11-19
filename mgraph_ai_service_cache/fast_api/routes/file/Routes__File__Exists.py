from typing                                                                              import Dict
from osbot_fast_api.api.routes.Fast_API__Routes                                          import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix               import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                  import Safe_Str__Fast_API__Route__Tag
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service

TAG__ROUTES_EXISTS                  = 'exists'
PREFIX__ROUTES_EXISTS               = '/{namespace}'
BASE_PATH__ROUTES_EXISTS            = f'{PREFIX__ROUTES_EXISTS}/{TAG__ROUTES_EXISTS}/'
ROUTES_PATHS__EXISTS                = [ BASE_PATH__ROUTES_EXISTS + 'hash/{cache_hash}']


class Routes__File__Exists(Fast_API__Routes):
    tag           : Safe_Str__Fast_API__Route__Tag     = TAG__ROUTES_EXISTS
    prefix        : Safe_Str__Fast_API__Route__Prefix  =  PREFIX__ROUTES_EXISTS
    cache_service : Cache__Service

    # this was on Routes__Retrieve (but needs to be here on Routes__Exists)
    # @route_path("/retrieve/exists/{cache_hash}")
    # def retrieve__exists__cache_hash(self, cache_hash : Safe_Str__Cache_Hash,
    #                                        namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
    #                                   ) -> Schema__Cache__Exists__Response:                                                 # Check if entry exists
    #
    #     exists = self.retrieve_service().check_exists(cache_hash, namespace)                                                  # todo: this should return the type Schema__Cache__Exists__Response
    #
    #     return Schema__Cache__Exists__Response(exists     = exists     ,                                                    # todo: we should need to do this conversion here
    #                                            cache_hash = cache_hash ,
    #                                            namespace  = namespace  )
    # todo: refactor code below with the version above (which is using Schema__Cache__Exists__Response)
    def exists__hash__cache_hash(self, cache_hash  : Safe_Str__Cache_Hash                                   ,  # Check if hash exists
                                       namespace   : Safe_Str__Id = None
                                  ) -> Dict[str, bool]:
        namespace = namespace or Safe_Str__Id("default")                                                             # todo: refactor to use static var for default namespace
        handler   = self.cache_service.get_or_create_handler(namespace)
        file_id   = Safe_Str__Id(cache_hash)                                                                         # cast hash to File_Id (which is what file__json will use)

        with handler.fs__refs_hash.file__json(file_id) as ref_fs:
            exists = ref_fs.exists()

        return {"exists"     : exists         ,
                "cache_hash" : str(cache_hash),
                "namespace"  : str(namespace )}

    def setup_routes(self):
        self.add_route_get(self.exists__hash__cache_hash)