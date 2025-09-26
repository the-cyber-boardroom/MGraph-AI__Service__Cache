from osbot_utils.type_safe.Type_Safe                                                 import Type_Safe
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                       import type_safe
from osbot_utils.utils.Zip                                                           import zip_bytes__file_list
from mgraph_ai_service_cache.service.cache.Cache__Service                            import Cache__Service
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Request    import Schema__Cache__Zip__Store__Request
from mgraph_ai_service_cache.schemas.cache.zip.Schema__Cache__Zip__Store__Response   import Schema__Cache__Zip__Store__Response


class Cache__Service__Zip__Store(Type_Safe):                                            # Service layer for storing zip files
    cache_service : Cache__Service                                                      # Underlying cache service

    @type_safe
    def store_zip(self, request: Schema__Cache__Zip__Store__Request
                   ) -> Schema__Cache__Zip__Store__Response:                             # Store zip file in cache

        if not request.zip_bytes:
            return Schema__Cache__Zip__Store__Response(success       = False,
                                                       namespace     = request.namespace,
                                                       error_type    = "INVALID_INPUT",
                                                       error_message = "Zip bytes cannot be empty")

        try:
            file_list = zip_bytes__file_list(request.zip_bytes)                        # Validate it's a valid zip
        except Exception as e:
            return Schema__Cache__Zip__Store__Response(success       = False                        ,
                                                       namespace     = request.namespace            ,
                                                       error_type    = "INVALID_ZIP_FORMAT"         ,
                                                       error_message = f"Invalid zip file: {str(e)}")
            raise ValueError(f"Invalid zip file: {str(e)}")

        # todo: review this hash_from_bytes workflow, since it is looking that same zip with same content has different hashes
        cache_hash = self.cache_service.hash_from_bytes(request.zip_bytes)              # Calculate hash of zip content
        #cache_id   = request.file_id or Random_Guid()                                   # Use provided ID or generate

        store_result = self.cache_service.store_with_strategy(storage_data     = request.zip_bytes    ,              # Store using cache service
                                                              cache_hash       = cache_hash           ,
                                                              #cache_id         = request.file_id      ,
                                                              cache_key        = request.cache_key    ,
                                                              file_id          = request.file_id      ,
                                                              namespace        = request.namespace    ,
                                                              strategy         = request.strategy     )

        if not store_result:                                                                                # Handle storage failure
            raise RuntimeError("Failed to store zip file in cache")

        return Schema__Cache__Zip__Store__Response(cache_id         = store_result.cache_id   ,             # Build response
                                                   cache_hash       = store_result.cache_hash ,
                                                   namespace        = store_result.namespace  ,
                                                   paths            = store_result.paths      ,
                                                   size             = store_result.size       ,
                                                   file_count       = len(file_list)          ,
                                                   success          = True              )