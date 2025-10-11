import gzip
from osbot_utils.type_safe.Type_Safe                                                   import Type_Safe
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text           import Safe_Str__Text
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path      import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id        import Safe_Str__Id
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                         import type_safe
from mgraph_ai_service_cache_client.schemas.cache.consts__Cache_Service                import DEFAULT_CACHE__STORE__STRATEGY, DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache_client.schemas.cache.enums.Enum__Cache__Store__Strategy   import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache_client.schemas.cache.safe_str.Safe_Str__Json__Field_Path  import Safe_Str__Json__Field_Path
from mgraph_ai_service_cache_client.schemas.errors.Schema__Cache__Error__Invalid_Input import Schema__Cache__Error__Invalid_Input
from mgraph_ai_service_cache.service.cache.Cache__Service                              import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Store__Response       import Schema__Cache__Store__Response


class Cache__Service__Store(Type_Safe):                                               # Service layer for cache store operations
    cache_service : Cache__Service                                                    # Underlying cache service

    @type_safe
    def store_string(self, data      : str                                                          ,       # todo: we should we using a Type_Safe class for these params
                           strategy  : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                           namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE      ,
                           cache_key : Safe_Str__File__Path         = None                          ,
                           file_id   : Safe_Str__Id                 = None
                     ) -> Schema__Cache__Store__Response:                               # Store string data

        if not data:                                                                    # Validate input
            return None                                                                 # Let route handler decide error response

        cache_hash = self.cache_service.hash_from_string(data)

        return self.cache_service.store_with_strategy(storage_data = data       ,       # todo: convert this into a Type_Safe object
                                                      cache_hash   = cache_hash ,
                                                      cache_key    = cache_key  ,
                                                      file_id      = file_id    ,
                                                      strategy     = strategy   ,
                                                      namespace    = namespace  )

    @type_safe
    def store_json(self, data             : dict                         = None                          ,
                         strategy         : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                         namespace        : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE      ,
                         cache_key        : Safe_Str__File__Path         = None                          ,
                         file_id          : Safe_Str__Id                 = None                          ,
                         json_field_path  : Safe_Str__Json__Field_Path   = None                          ,
                    ) -> Schema__Cache__Store__Response:                                                # Store JSON data

        if json_field_path:                                                                             # Field-based hashing
            cache_hash = self.cache_service.hash_from_json_field(data=data, json_field=json_field_path)
        else:
            cache_hash = self.cache_service.hash_from_json(data)

        return self.cache_service.store_with_strategy(storage_data    = data           ,        # todo: review refactoring opportunity with store_string since a lot of the code in this method is very similar
                                                      cache_hash      = cache_hash     ,
                                                      cache_key       = cache_key      ,
                                                      file_id         = file_id        ,
                                                      json_field_path = json_field_path,
                                                      strategy        = strategy       ,
                                                      namespace       = namespace      )

    @type_safe
    def store_binary(self, data             : bytes,
                           strategy          : Enum__Cache__Store__Strategy = DEFAULT_CACHE__STORE__STRATEGY,
                           namespace         : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE      ,
                           cache_key         : Safe_Str__File__Path         = None                          ,
                           file_id           : Safe_Str__Id                 = None                          ,
                           content_encoding  : str                          = None
                      ) -> Schema__Cache__Store__Response:                               # Store binary data

        if not data:                                                                    # Validate input
            return None                                                                 # Let route handler decide error response

        # todo: review the move of this gzip capability into a separate store_binary__gzip method (to keep the paths clean
        if content_encoding == 'gzip':                                                  # Handle compression
            decompressed = gzip.decompress(data)                                        # get decompressed data so that we can use it to calculate the hash
            cache_hash = self.cache_service.hash_from_bytes(decompressed)               # todo: question: if we are storing the compressed bytes, shouldn't the hash be calculated from those compressed bytes?
            storage_data = data                                                         # Store compressed
        else:
            cache_hash = self.cache_service.hash_from_bytes(data)
            storage_data = data


        return self.cache_service.store_with_strategy(storage_data     = storage_data    ,  # todo: when looking at the refactoring opportunities, since most of these values are being passed through
                                                      cache_hash       = cache_hash      ,  #       these store_* functions, it would be better to have a domain class that would hold those values
                                                      cache_key        = cache_key       ,  #          a) don't fit in that domain class
                                                      file_id          = file_id         ,  #          b) have been modified in this function and need to be passed to the next function
                                                      strategy         = strategy        ,
                                                      namespace        = namespace       ,
                                                      content_encoding = content_encoding )


    @type_safe
    def get_invalid_input_error(self, field_name    : Safe_Str__Id,
                                      field_value   : Safe_Str__Text = None,
                                      expected_type : Safe_Str__Id   = None,
                                      message       : Safe_Str__Text = None
                                 ) -> Schema__Cache__Error__Invalid_Input:                                                       # Build invalid input error
        return Schema__Cache__Error__Invalid_Input( error_type    = "INVALID_INPUT"                                       ,     # todo: refactor to error type to Enum
                                                    message       = message or f"Invalid value for field {field_name}"    ,
                                                    field_name    = field_name                                            ,
                                                    field_value   = str(field_value) if field_value else None             ,
                                                    expected_type = expected_type                                         )