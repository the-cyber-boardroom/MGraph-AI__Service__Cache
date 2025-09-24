from typing                                                                                  import List
from fastapi                                                                                 import HTTPException, Response
from osbot_fast_api.api.decorators.route_path                                               import route_path
from osbot_fast_api.api.routes.Fast_API__Routes                                             import Fast_API__Routes
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Prefix                               import Safe_Str__Fast_API__Route__Prefix
from osbot_fast_api.schemas.Safe_Str__Fast_API__Route__Tag                                  import Safe_Str__Fast_API__Route__Tag
from osbot_utils.decorators.methods.cache_on_self                                           import cache_on_self
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path           import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                       import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id             import Safe_Str__Id
from mgraph_ai_service_cache.schemas.consts.const__Fast_API                                 import FAST_API__PARAM__NAMESPACE
from mgraph_ai_service_cache.service.cache.Cache__Service                                   import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve__Data import Cache__Service__Retrieve__Data

TAG__ROUTES_RETRIEVE__DATA    = Safe_Str__Fast_API__Route__Tag('retrieve-data')
PREFIX__ROUTES_RETRIEVE__DATA = Safe_Str__Fast_API__Route__Prefix('/{namespace}/cache/{cache_id}')
BASE_PATH__ROUTES_RETRIEVE__DATA = f'{PREFIX__ROUTES_RETRIEVE__DATA}/{TAG__ROUTES_RETRIEVE__DATA}/'

ROUTES_PATHS__RETRIEVE__DATA = [ BASE_PATH__ROUTES_RETRIEVE__DATA + 'file'      ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'file/json' ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'file/string',
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'file/binary',
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'list'      ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'count'     ,
                                 BASE_PATH__ROUTES_RETRIEVE__DATA + 'size'      ]


class Routes__Retrieve__Data(Fast_API__Routes):                                                 # FastAPI routes for retrieving data files
    tag           : Safe_Str__Fast_API__Route__Tag    = TAG__ROUTES_RETRIEVE__DATA
    prefix        : Safe_Str__Fast_API__Route__Prefix = PREFIX__ROUTES_RETRIEVE__DATA
    cache_service : Cache__Service                                                              # Dependency injection for cache service

    @cache_on_self
    def retrieve_service(self) -> Cache__Service__Retrieve__Data:                               # Service layer for data retrieval logic
        return Cache__Service__Retrieve__Data(cache_service=self.cache_service)

    @route_path("/retrieve/data/file")
    def retrieve__data__file(self, cache_id     : Random_Guid                                  ,
                                   namespace    : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                   data_key     : Safe_Str__File__Path         = None          ,
                                   data_file_id : Safe_Str__Id                 = None
                             ) -> Schema__Data__File__Content:                                  # Retrieve data file with auto type detection
        try:
            result = self.retrieve_service().retrieve_data(cache_id     = cache_id             ,
                                                          data_key     = data_key               ,
                                                          data_file_id = data_file_id           ,
                                                          namespace    = namespace              )

            if result is None:
                raise HTTPException(status_code = 404                                          ,
                                   detail   = { "error_type"     : "NOT_FOUND"                ,
                                               "message"        : f"Data file not found"      ,
                                               "cache_id"       : str(cache_id)               ,
                                               "data_file_id"   : str(data_file_id) if data_file_id else None})
            return result

        except ValueError as e:
            raise HTTPException(status_code = 400                       ,
                               detail   = { "error_type" : "INVALID_INPUT",
                                           "message"    : str(e)          })

    @route_path("/retrieve/data/file/json")
    def retrieve__data__file__json(self, cache_id     : Random_Guid                            ,
                                         namespace    : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                         data_key     : Safe_Str__File__Path         = None    ,
                                         data_file_id : Safe_Str__Id                 = None
                                   ) -> dict:                                                    # Retrieve data as JSON format
        result = self.retrieve__data__file(cache_id, namespace, data_key, data_file_id)

        from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type import Enum__Cache__Data_Type
        if result.data_type != Enum__Cache__Data_Type.JSON:
            raise HTTPException(status_code = 415                                              ,
                               detail   = { "error_type"   : "UNSUPPORTED_MEDIA_TYPE"         ,
                                           "message"      : f"Data file is {result.data_type}, not JSON",
                                           "actual_type"  : str(result.data_type)             })
        return result.data

    @route_path("/retrieve/data/file/string")
    def retrieve__data__file__string(self, cache_id     : Random_Guid                          ,
                                           namespace    : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                           data_key     : Safe_Str__File__Path         = None  ,
                                           data_file_id : Safe_Str__Id                 = None
                                     ) -> Response:                                             # Retrieve data as plain text
        result = self.retrieve__data__file(cache_id, namespace, data_key, data_file_id)

        from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type import Enum__Cache__Data_Type
        if result.data_type == Enum__Cache__Data_Type.STRING:
            content = result.data
        elif result.data_type == Enum__Cache__Data_Type.JSON:
            import json
            content = json.dumps(result.data)
        else:  # BINARY
            try:
                content = result.data.decode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                import base64
                content = base64.b64encode(result.data).decode('utf-8')

        return Response(content=content, media_type="text/plain")

    @route_path("/retrieve/data/file/binary")
    def retrieve__data__file__binary(self, cache_id     : Random_Guid                          ,
                                           namespace    : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                           data_key     : Safe_Str__File__Path         = None  ,
                                           data_file_id : Safe_Str__Id                 = None
                                     ) -> Response:                                             # Retrieve data as binary
        result = self.retrieve__data__file(cache_id, namespace, data_key, data_file_id)

        from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type import Enum__Cache__Data_Type
        if result.data_type == Enum__Cache__Data_Type.BINARY:
            content = result.data
        elif result.data_type == Enum__Cache__Data_Type.STRING:
            content = result.data.encode('utf-8')
        else:  # JSON
            import json
            content = json.dumps(result.data).encode('utf-8')

        return Response(content=content, media_type="application/octet-stream")

    @route_path("/retrieve/data/list")
    def retrieve__data__list(self, cache_id  : Random_Guid                                     ,
                                   namespace : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                   data_key  : Safe_Str__File__Path         = None
                             ) -> List[Schema__Data__File__Info]:                               # List all data files for cache entry
        try:
            data_files = self.retrieve_service().list_data_files(cache_id  = cache_id         ,
                                                                 data_key  = data_key          ,
                                                                 namespace = namespace         )
            return data_files

        except ValueError as e:
            raise HTTPException(status_code = 400                       ,
                               detail   = { "error_type" : "INVALID_INPUT",
                                           "message"    : str(e)          })

    @route_path("/retrieve/data/count")
    def retrieve__data__count(self, cache_id  : Random_Guid                                    ,
                                    namespace : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE,
                                    data_key  : Safe_Str__File__Path         = None
                              ) -> dict:                                                         # Count data files for cache entry
        try:
            count = self.retrieve_service().count_data_files(cache_id  = cache_id             ,
                                                            data_key  = data_key               ,
                                                            namespace = namespace             )

            return { "count"     : int(count)        ,
                    "cache_id"  : str(cache_id)     ,
                    "namespace" : str(namespace)    }

        except ValueError as e:
            raise HTTPException(status_code = 400                       ,
                               detail   = { "error_type" : "INVALID_INPUT",
                                           "message"    : str(e)          })

    @route_path("/retrieve/data/size")
    def retrieve__data__size(self, cache_id  : Random_Guid                                     ,
                                   namespace : Safe_Str__Id                 = FAST_API__PARAM__NAMESPACE
                             ) -> dict:                                                          # Get total size of data folder
        try:
            total_size = self.retrieve_service().get_data_folder_size(cache_id  = cache_id     ,
                                                                     namespace = namespace     )

            return { "total_size" : int(total_size)   ,
                    "cache_id"   : str(cache_id)     ,
                    "namespace"  : str(namespace)    }

        except ValueError as e:
            raise HTTPException(status_code = 400                       ,
                               detail   = { "error_type" : "INVALID_INPUT",
                                           "message"    : str(e)          })

    def setup_routes(self) -> 'Routes__Retrieve__Data':                                         # Configure all data retrieval routes
        self.add_route_get(self.retrieve__data__file        )
        self.add_route_get(self.retrieve__data__file__json  )
        self.add_route_get(self.retrieve__data__file__string)
        self.add_route_get(self.retrieve__data__file__binary)
        self.add_route_get(self.retrieve__data__list        )
        self.add_route_get(self.retrieve__data__count       )
        self.add_route_get(self.retrieve__data__size        )
        return self