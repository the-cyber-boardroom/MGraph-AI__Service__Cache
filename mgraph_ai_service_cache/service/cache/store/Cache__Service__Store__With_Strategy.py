from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.utils.Misc                                                              import timestamp_now
from mgraph_ai_service_cache_client.schemas.cache.Schema__Cache__Store__Response         import Schema__Cache__Store__Response
from mgraph_ai_service_cache_client.schemas.cache.file.Schema__Cache__File__Refs         import Schema__Cache__File__Refs
from mgraph_ai_service_cache_client.schemas.cache.store.Schema__Cache__Store__Metadata   import Schema__Cache__Store__Metadata
from mgraph_ai_service_cache_client.schemas.cache.store.Schema__Cache__Store__Paths      import Schema__Cache__Store__Paths
from mgraph_ai_service_cache_client.schemas.cache.store.Schema__Cache__Hash__Reference   import Schema__Cache__Hash__Reference, Schema__Cache__Hash__Entry
from mgraph_ai_service_cache_client.schemas.cache.store.Schema__Store__Context           import Schema__Store__Context

class Cache__Service__Store__With_Strategy(Type_Safe):                                       # Orchestrates the storage of cache entries with different strategies

    def execute(self, context: Schema__Store__Context                                        # Main orchestration method that coordinates all storage operations
                 ) -> Schema__Cache__Store__Response:
        self.initialize_context   (context)                                                 # Set defaults and initialize tracking
        self.store_data           (context)                                                 # Store the actual data using selected strategy
        self.update_hash_reference(context)                                                 # Update or create hash-to-ID reference
        self.create_file_refs  (context)                                                 # Create ID-to-hash reference with metadata
        return self.build_response(context)                                                 # Build and return the response

    def initialize_context(self, context: Schema__Store__Context):                          # Initialize context with defaults and tracking structures
        if not context.file_id:
            context.file_id = Safe_Str__Id(str(context.cache_id))                            # Use cache_id as file_id if not provided

        if not context.cache_key:
            context.cache_key = Safe_Str__File__Path("")                                     # Empty path if no cache key

        context.all_paths = Schema__Cache__Store__Paths()                                    # Initialize path tracking with Type_Safe
        context.timestamp = timestamp_now()                                                  # Capture storage timestamp

    def store_data(self, context: Schema__Store__Context):                                  # Store the actual data using the appropriate strategy and format
        fs_data = context.handler.get_fs_for_strategy(context.strategy)                      # Get filesystem for the selected strategy

        if isinstance(context.storage_data, bytes):                                          # Determine file type and get appropriate file handler
            file_fs           = fs_data.file__binary(file_id  = context.file_id  ,
                                                     file_key = context.cache_key)
            context.file_type = "binary"
        else:
            file_fs           = fs_data.file__json(file_id  = context.file_id  ,
                                                   file_key = context.cache_key)
            context.file_type = "json"

        with file_fs:                                                                         # Store data with metadata
            context.all_paths.data            = file_fs.create(context.storage_data)                     # Store directly to Type_Safe list
            context.file_paths.content_files  = file_fs.file_fs__paths().paths__content     ()
            context.file_paths.data_folders   = file_fs.file_fs__paths().paths__data_folders()
            context.metadata                  = self.build_metadata(context)                             # Build metadata as Type_Safe object

            file_fs.metadata__update(context.metadata.json())                                 # Convert to dict for storage

            context.file_size = file_fs.metadata().content__size

    def build_metadata(self, context: Schema__Store__Context  # Build metadata Type_Safe object for the stored file
                        ) -> Schema__Cache__Store__Metadata:
        return Schema__Cache__Store__Metadata(cache_hash       = context.cache_hash       ,     # todo: refactor this assigment to make better use of the fact that Schema__Store__Context and Schema__Cache__Store__Metadata share a lot of the same variables
                                              cache_key        = str(context.cache_key)   ,     #       for example could they have a shared base class
                                              cache_id         = str(context.cache_id)    ,
                                              content_encoding = context.content_encoding ,
                                              file_id          = str(context.file_id)     ,
                                              json_field_path  = context.json_field_path  ,
                                              stored_at        = context.timestamp        ,
                                              strategy         = context.strategy         ,
                                              namespace        = str(context.namespace)   ,
                                              file_type        = context.file_type        )

    def update_hash_reference(self, context: Schema__Store__Context):                       # Update or create the hash-to-ID reference
        file_id = Safe_Str__Id(str(context.cache_hash))                                      # Use hash as file ID for reference

        with context.handler.fs__refs_hash.file__json(file_id) as ref_fs:
            if ref_fs.exists():
                self.update_existing_hash_reference(ref_fs, context)                       # Update existing reference
            else:
                self.create_new_hash_reference(ref_fs, context)                             # Create new reference

    def update_existing_hash_reference(self, ref_fs,  # Update existing hash reference with new cache ID
                                             context: Schema__Store__Context):
        existing_data = ref_fs.content()                                                     # Get existing data as dict

        # Convert to Type_Safe object
        hash_ref = Schema__Cache__Hash__Reference.from_json(existing_data)

        # Add new entry
        new_entry = Schema__Cache__Hash__Entry(cache_id  = str(context.cache_id),
                                               timestamp = context.timestamp     )
        hash_ref.cache_ids.append(new_entry)
        hash_ref.latest_id       = str(context.cache_id)
        hash_ref.total_versions += 1

        # Update and track paths
        paths_hash_to_id          = ref_fs.update(file_data=hash_ref.json())
        context.all_paths.by_hash = paths_hash_to_id

    def create_new_hash_reference(self, ref_fs,  # Create a new hash reference structure
                                        context: Schema__Store__Context):
        # Build new reference with Type_Safe
        hash_entry = Schema__Cache__Hash__Entry(cache_id  = str(context.cache_id),
                                                timestamp = context.timestamp     )

        hash_ref = Schema__Cache__Hash__Reference(cache_hash     = str(context.cache_hash),
                                                  cache_ids      = [hash_entry]          ,
                                                  latest_id      = str(context.cache_id) ,
                                                  total_versions = 1                      )

        # Create and track paths
        paths_hash_to_id          = ref_fs.create(file_data=hash_ref.json())
        context.all_paths.by_hash = paths_hash_to_id

    def create_file_refs(self, context: Schema__Store__Context):                         # Create the ID-to-hash reference with content paths
        file_id = Safe_Str__Id(str(context.cache_id))                                        # Use cache ID as file ID

        with context.handler.fs__refs_id.file__json(file_id) as ref_fs:
            context.all_paths.by_id = ref_fs.paths()                                         # Track paths

            # Build complete reference with Type_Safe
            id_reference = Schema__Cache__File__Refs(all_paths         = context.all_paths      ,
                                                     cache_id          = str(context.cache_id)  ,
                                                     cache_hash        = str(context.cache_hash),
                                                     file_paths        = context.file_paths     ,
                                                     namespace         = str(context.namespace) ,
                                                     strategy          = context.strategy       ,
                                                     file_type         = context.file_type      ,
                                                     timestamp         = context.timestamp      )

            ref_fs.create(id_reference.json())                                               # Store as JSON

    def build_response(self, context: Schema__Store__Context  # Build the final response object
                        ) -> Schema__Cache__Store__Response:
        return Schema__Cache__Store__Response(cache_id   = context.cache_id       ,
                                              cache_hash = context.cache_hash     ,
                                              namespace  = context.namespace      ,
                                              paths      = context.all_paths.json(),           # Convert to dict for response
                                              size       = context.file_size       )