from osbot_fast_api.api.routes.Fast_API__Routes                                                 import Fast_API__Routes
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Prefix                      import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.api.schemas.safe_str.Safe_Str__Fast_API__Route__Tag                         import Safe_Str__Fast_API__Route__Tag
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__Exists__Response          import Schema__Cache__Exists__Response
from osbot_utils.type_safe.primitives.domains.identifiers.Cache_Id                              import Cache_Id
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id                 import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash        import Safe_Str__Cache_Hash
from mgraph_ai_service_cache_client.schemas.consts.const__Fast_API                              import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                       import Cache__Service

TAG__ROUTES_EXISTS                  = 'exists'
PREFIX__ROUTES_EXISTS               = '/{namespace}'
BASE_PATH__ROUTES_EXISTS            = f'{PREFIX__ROUTES_EXISTS}/{TAG__ROUTES_EXISTS}/'
ROUTES_PATHS__EXISTS                = [ BASE_PATH__ROUTES_EXISTS + 'hash/{cache_hash}',
                                        BASE_PATH__ROUTES_EXISTS + 'id/{cache_id}'    ]


class Routes__File__Exists(Fast_API__Routes):
    tag           : Safe_Str__Fast_API__Route__Tag     = TAG__ROUTES_EXISTS
    prefix        : Safe_Str__Fast_API__Route__Prefix  = PREFIX__ROUTES_EXISTS
    cache_service : Cache__Service


    def exists__id__cache_id(self, cache_id  : Cache_Id                             ,       # Check if cache_id exists
                                   namespace : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                              ) -> Schema__Cache__Exists__Response:
        handler = self.cache_service.get_or_create_handler(namespace)
        file_id = Safe_Str__Id(cache_id)

        with handler.fs__refs_id.file__json__single(file_id) as ref_fs:
            exists = ref_fs.exists()

        return Schema__Cache__Exists__Response(exists    = exists   ,
                                               cache_id  = cache_id ,
                                               namespace = namespace)


    def exists__hash__cache_hash(self, cache_hash : Safe_Str__Cache_Hash                    ,   # Check if hash exists
                                       namespace  : Safe_Str__Id = FAST_API__PARAM__NAMESPACE
                                ) -> Schema__Cache__Exists__Response:
        handler = self.cache_service.get_or_create_handler(namespace)
        file_id = Safe_Str__Id(cache_hash)

        with handler.fs__refs_hash.file__json__single(file_id) as ref_fs:
            exists = ref_fs.exists()

        return Schema__Cache__Exists__Response(exists     = exists    ,
                                               cache_hash = cache_hash,
                                               namespace  = namespace )

    def setup_routes(self):
        self.add_route_get(self.exists__id__cache_id    )
        self.add_route_get(self.exists__hash__cache_hash)