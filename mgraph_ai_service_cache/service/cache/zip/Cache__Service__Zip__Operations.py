from typing                                                                              import Optional
from osbot_utils.decorators.methods.cache_on_self                                        import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.type_safe_core.decorators.type_safe                           import type_safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.utils.Zip                                                               import (zip_bytes__file_list, zip_bytes__file,
                                                                                                 zip_bytes__add_file, zip_bytes__remove_file,
                                                                                                 zip_bytes__replace_file)
from mgraph_ai_service_cache_client.schemas.cache.zip.enums.Enum__Cache__Zip__Operation         import Enum__Cache__Zip__Operation
from mgraph_ai_service_cache.service.cache.Cache__Service                                import Cache__Service
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Request    import Schema__Cache__Zip__Operation__Request
from mgraph_ai_service_cache_client.schemas.cache.zip.Schema__Cache__Zip__Operation__Response   import Schema__Cache__Zip__Operation__Response
from mgraph_ai_service_cache.utils.for_osbot_utils.Zip                                   import zip_bytes__content_hash


class Cache__Service__Zip__Operations(Type_Safe):                                        # Service layer for zip file operations
    cache_service : Cache__Service                                                       # Underlying cache service

    @cache_on_self
    def operation_handlers(self):
        return { Enum__Cache__Zip__Operation.LIST    : self.operation__list    ,
                 Enum__Cache__Zip__Operation.GET     : self.operation__get     ,
                 Enum__Cache__Zip__Operation.ADD     : self.operation__add     ,
                 Enum__Cache__Zip__Operation.REMOVE  : self.operation__remove  ,
                 Enum__Cache__Zip__Operation.REPLACE : self.operation__replace }

    @type_safe
    def perform_operation(self, request: Schema__Cache__Zip__Operation__Request
                           ) -> Schema__Cache__Zip__Operation__Response:                   # Route operation to appropriate handler
        try:
            handler = self.operation_handlers().get(request.operation)
            if not handler:
                return Schema__Cache__Zip__Operation__Response(success       = False                                    ,
                                                               operation     = request.operation                        ,
                                                               cache_id      = request.cache_id                         ,
                                                               error_details = f"Unknown operation: {request.operation}")

            response = handler(request)
            return response
        except Exception as e:
            return self.error_response(request, str(e))

    @type_safe
    def operation__list(self, request: Schema__Cache__Zip__Operation__Request
                         ) -> Schema__Cache__Zip__Operation__Response:                     # List all files in zip (read-only)
        zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
        if not zip_bytes:
            return self.error_response(request, "Zip file not found in cache")

        file_list = zip_bytes__file_list(zip_bytes)
        return Schema__Cache__Zip__Operation__Response(success    = True                                  ,
                                                       operation  = Enum__Cache__Zip__Operation.LIST      ,
                                                       cache_id   = request.cache_id                      ,
                                                       file_list  = file_list                             ,
                                                       message    = f"Found {len(file_list)} files in zip")

    @type_safe
    def operation__get(self, request: Schema__Cache__Zip__Operation__Request
                        ) -> Schema__Cache__Zip__Operation__Response:                      # Get single file from zip (read-only)
        if not request.file_path:
            return self.error_response(request, "file_path required for get operation")

        zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
        if not zip_bytes:
            return self.error_response(request, "Zip file not found in cache")

        file_content = zip_bytes__file(zip_bytes, str(request.file_path))
        if file_content is None:
            return self.error_response(request, f"File '{request.file_path}' not found in zip")

        return Schema__Cache__Zip__Operation__Response(success      = True                                ,
                                                       operation    = Enum__Cache__Zip__Operation.GET     ,
                                                       cache_id     = request.cache_id                    ,
                                                       file_content = file_content                        ,
                                                       file_size    = len(file_content)                   ,
                                                       message      = f"Retrieved '{request.file_path}'"  )

    @type_safe
    def operation__add(self, request: Schema__Cache__Zip__Operation__Request
                        ) -> Schema__Cache__Zip__Operation__Response:                           # Add new file to zip (creates new cache entry)
        if not request.file_path: #or not request.file_content:
            return self.error_response(request, "file_path required for get operation")

        zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
        if not zip_bytes:
            return self.error_response(request, "Zip file not found in cache")

        new_zip = zip_bytes__add_file(zip_bytes, str(request.file_path), request.file_content)

        new_cache_id = self.create_modified_zip(original_id = request.cache_id                      ,           # Create new immutable cache entry
                                                namespace   = request.namespace                     ,
                                                zip_bytes   = new_zip                               ,
                                                operation   = "add"                                 ,
                                                details     = {"added_file": str(request.file_path)})

        if not new_cache_id:
            return self.error_response(request, "Failed to create new cache entry")

        return Schema__Cache__Zip__Operation__Response(success           = True                                               ,
                                                       operation         = Enum__Cache__Zip__Operation.ADD                    ,
                                                       cache_id          = new_cache_id                                       ,  # NEW cache ID
                                                       original_cache_id = request.cache_id                                   ,  # Original for provenance
                                                       files_affected    = [request.file_path]                                ,
                                                       message           = f"Added '{request.file_path}' to zip (new cache_id: {new_cache_id})")

    @type_safe
    def operation__remove(self, request: Schema__Cache__Zip__Operation__Request
                         ) -> Schema__Cache__Zip__Operation__Response:                   # Remove file from zip (creates new cache entry)
        if not request.file_path:
            return self.error_response(request, "file_path required for remove operation")

        zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
        if not zip_bytes:
            return self.error_response(request, "Zip file not found in cache")

        new_zip = zip_bytes__remove_file(zip_bytes, str(request.file_path))

        # Create new immutable cache entry
        new_cache_id = self.create_modified_zip(original_id = request.cache_id                                 ,
                                                namespace   = request.namespace                                ,
                                                zip_bytes   = new_zip                                          ,
                                                operation   = "remove"                                         ,
                                                details     = {"removed_file": str(request.file_path)})

        if not new_cache_id:
            return self.error_response(request, "Failed to create new cache entry")

        return Schema__Cache__Zip__Operation__Response(
            success           = True                                                 ,
            operation         = Enum__Cache__Zip__Operation.REMOVE                   ,
            cache_id          = new_cache_id                                         ,  # NEW cache ID
            original_cache_id = request.cache_id                                     ,  # Original for provenance
            files_affected    = [request.file_path]                                  ,
            message           = f"Removed '{request.file_path}' from zip (new cache_id: {new_cache_id})")

    @type_safe
    def operation__replace(self, request: Schema__Cache__Zip__Operation__Request
                          ) -> Schema__Cache__Zip__Operation__Response:                  # Replace existing file in zip (creates new cache entry)
        if not request.file_path:
            return self.error_response(request, "file_path required for replace operation")

        zip_bytes = self.retrieve_zip_bytes(request.cache_id, request.namespace)
        if not zip_bytes:
            return self.error_response(request, "Zip file not found in cache")

        new_zip = zip_bytes__replace_file(zip_bytes, str(request.file_path), request.file_content)

        # Create new immutable cache entry
        new_cache_id = self.create_modified_zip(original_id = request.cache_id                                  ,
                                                namespace   = request.namespace                                 ,
                                                zip_bytes   = new_zip                                           ,
                                                operation   = "replace"                                         ,
                                                details     = {"replaced_file": str(request.file_path)}
)

        if not new_cache_id:
            return self.error_response(request, "Failed to create new cache entry")

        return Schema__Cache__Zip__Operation__Response(
            success           = True                                                   ,
            operation         = Enum__Cache__Zip__Operation.REPLACE                    ,
            cache_id          = new_cache_id                                           ,  # NEW cache ID
            original_cache_id = request.cache_id                                       ,  # Original for provenance
            files_affected    = [request.file_path]                                    ,
            message           = f"Replaced '{request.file_path}' in zip (new cache_id: {new_cache_id})")

    def create_modified_zip(self, original_id : Random_Guid  ,
                                  namespace   : str          ,
                                  zip_bytes   : bytes        ,
                                  operation   : str          ,
                                  details     : dict = None
                             ) -> Optional[Random_Guid]:                                                            # Create new immutable cache entry for modified zip

        hash_length = self.cache_service.hash_config.length
        cache_hash  = zip_bytes__content_hash(zip_bytes   = zip_bytes,
                                              hash_length = hash_length)                                            # use the bytes content and files path to calculate the zip's hash (which is different mode than when we just store bytes)

        original_entry__refs      = self.cache_service.retrieve_by_id__refs    (original_id, namespace)             # Get original entry to preserve strategy
        #original_entry__metadata  = self.cache_service.retrieve_by_id__metadata(original_id, namespace)           # Get original entry to preserve strategy
        original_strategy = original_entry__refs.strategy

        # todo: wire up the update of the metadata  (note: need to figure out the best place to put it
        # metadata__original = original_entry__metadata.data
        # metadata = { **metadata__original                  ,                            # Build provenance metadata
        #              "parent_id"      : str(original_id)   ,
        #              "operation"      : operation          ,
        #              "transformation" : f"zip_{operation}" ,
        #              "derived_from"   : str(original_id)   }
        # if details:
        #     metadata["operation_details"] = details

        result = self.cache_service.store_with_strategy(storage_data = zip_bytes         ,  # Store as new immutable entry with same strategy as original
                                                        cache_hash   = cache_hash       ,
                                                        cache_id     = Random_Guid()    ,   # Always new ID for immutability
                                                        namespace    = namespace        ,
                                                        strategy     = original_strategy)   # Preserve original's strategy
        return result.cache_id if result else None

    def retrieve_zip_bytes(self, cache_id  : Random_Guid    ,
                                 namespace : str
                          ) -> Optional[bytes]:                                          # Get zip from cache
        result = self.cache_service.retrieve_by_id(cache_id, namespace)
        if result and result.get('data_type') == 'binary':
            return result.get('data')
        return None

    def error_response(self, request       : Schema__Cache__Zip__Operation__Request,
                             error_details : str
                       ) -> Schema__Cache__Zip__Operation__Response:                    # Create error response
        return Schema__Cache__Zip__Operation__Response(
            success       = False                 ,
            operation     = request.operation     ,
            cache_id      = request.cache_id      ,
            error_details = error_details         )