import gzip
import json
from typing                                                                              import Dict, Optional, Any, List, Literal
from osbot_utils.decorators.methods.cache_on_self                                        import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path        import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id          import Safe_Str__Id
from osbot_utils.utils.Files                                                             import file_extension, file_name_without_extension
from osbot_utils.utils.Http                                                              import url_join_safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.utils.Misc                                                              import timestamp_now
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service                         import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Store__Strategy            import Enum__Cache__Store__Strategy
from mgraph_ai_service_cache.service.cache.Cache__Handler                                import Cache__Handler
from mgraph_ai_service_cache.service.cache.Cache__Hash__Config                           import Cache__Hash__Config
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator                        import Cache__Hash__Generator
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response                import Schema__Cache__Store__Response
from mgraph_ai_service_cache.service.storage.Storage_FS__S3                              import Storage_FS__S3

DEFAULT__CACHE__SERVICE__BUCKET_NAME        = "mgraph-ai-cache"
DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS  = 24

class Cache__Service(Type_Safe):                                                   # Main cache service orchestrator
    cache_handlers    : Dict[Safe_Str__Id, Cache__Handler]                              # Multiple cache handlers by namespace
    default_bucket    : str                           = DEFAULT__CACHE__SERVICE__BUCKET_NAME
    default_ttl_hours : int                           = DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
    hash_config       : Cache__Hash__Config                                        # Hash generation config
    hash_generator    : Cache__Hash__Generator                                     # Hash generator instance

    def delete_by_id(self, cache_id: Random_Guid, namespace: Safe_Str__Id = None) -> Dict[str, Any]:
        namespace = namespace or Safe_Str__Id("default")
        handler   = self.get_or_create_handler(namespace)

        with handler.fs__refs_id.file__json(Safe_Str__Id(str(cache_id))) as ref_fs:
            if not ref_fs.exists():
                return {"status": "not_found", "message": f"Cache ID {cache_id} not found"}

            id_ref_data  = ref_fs.content()
            all_paths    = id_ref_data.get("all_paths", {})
            cache_hash   = id_ref_data.get("hash")
            strategy     = id_ref_data.get("strategy")

        # Track deletion results
        deleted_paths = []
        failed_paths  = []

        # Delete data files first (use the appropriate fs based on strategy)
        fs_data = handler.get_fs_for_strategy(strategy)
        for path in all_paths.get("data", []):
            try:
                if fs_data.storage_fs.file__delete(path):
                    deleted_paths.append(path)
                else:
                    failed_paths.append(path)
            except Exception as e:
                failed_paths.append(f"{path}: {str(e)}")

        # Update hash reference (remove this cache_id from the list)
        if cache_hash:
            with handler.fs__refs_hash.file__json(Safe_Str__Id(cache_hash)) as ref_fs:
                if ref_fs.exists():
                    refs = ref_fs.content()
                    # Remove this cache_id from the list
                    refs["cache_ids"] = [entry for entry in refs["cache_ids"]
                                        if entry["id"] != str(cache_id)]
                    refs["total_versions"] -= 1

                    if refs["total_versions"] > 0:
                        # Update the latest_id if needed
                        if refs["latest_id"] == str(cache_id) and refs["cache_ids"]:
                            refs["latest_id"] = refs["cache_ids"][-1]["id"]
                        ref_fs.update(file_data=refs)
                    else:
                        # No more versions, delete the hash reference files
                        # FIX: Changed from all_paths.get("refs", {}).get("hash", [])
                        for path in all_paths.get("by_hash", []):
                            try:
                                if handler.fs__refs_hash.storage_fs.file__delete(path):
                                    deleted_paths.append(path)
                                else:
                                    failed_paths.append(path)
                            except Exception as e:
                                failed_paths.append(f"{path}: {str(e)}")

        # Finally, delete the ID reference files
        # FIX: Changed from all_paths.get("refs", {}).get("id", [])
        for path in all_paths.get("by_id", []):
            try:
                if handler.fs__refs_id.storage_fs.file__delete(path):
                    deleted_paths.append(path)
                else:
                    failed_paths.append(path)
            except Exception as e:
                failed_paths.append(f"{path}: {str(e)}")

        return {
            "status"        : "success" if not failed_paths else "partial",
            "cache_id"      : str(cache_id),
            "deleted_count" : len(deleted_paths),
            "failed_count"  : len(failed_paths),
            "deleted_paths" : deleted_paths,
            "failed_paths"  : failed_paths
        }

    def get_all_namespaces_stats(self) -> Dict[str, Any]:       # Get file counts for all active namespaces
        all_stats = {}

        for namespace in self.cache_handlers.keys():
            counts_data = self.get_namespace__file_counts(namespace)
            all_stats[str(namespace)] = {
                'total_files': counts_data['total_files'],
                'file_counts': counts_data['file_counts']
            }

        return {
            'namespaces': all_stats,
            'total_namespaces': len(self.cache_handlers),
            'grand_total_files': sum(ns['total_files'] for ns in all_stats.values())
        }

    def get_namespace__file_hashes(self, namespace: Safe_Str__Id ) -> Dict[str, Any]:
        file_hashes = []
        parent_folder = url_join_safe(str(namespace), "refs/by-hash")

        for file_path in self.storage_fs().folder__files__all(parent_folder=parent_folder):
            if file_extension(file_path) == '.json':
                file_id = file_name_without_extension(file_path)
                file_hashes.append(file_id)
        return file_hashes

    def get_namespace__file_ids(self, namespace: Safe_Str__Id ) -> Dict[str, Any]:
        file_ids = []
        parent_folder = url_join_safe(str(namespace), "refs/by-id")

        for file_path in self.storage_fs().folder__files__all(parent_folder=parent_folder):
            if file_extension(file_path) == '.json':
                file_id = file_name_without_extension(file_path)
                file_ids.append(file_id)
        return file_ids

    def get_namespace__file_counts(self, namespace: Safe_Str__Id = None) -> Dict[str, Any]:       # Get file counts for all strategies in a namespace
        namespace = namespace or Safe_Str__Id("default")
        handler = self.get_or_create_handler(namespace)

        file_counts = {}
        total_files = 0

        # todo: review the performance implications of this on large namespaces
        for strategy in ["direct", "temporal", "temporal_latest", "temporal_versioned"]:    # Count files in each data strategy
            try:
                fs = handler.get_fs_for_strategy(strategy)
                if fs and fs.storage_fs:
                    count = len(fs.storage_fs.files__paths())
                    file_counts[f"{strategy}_files"] = count
                    total_files += count
                else:
                    file_counts[f"{strategy}_files"] = 0
            except Exception:
                file_counts[f"{strategy}_files"] = 0

        # Count reference store files
        try:
            refs_hash_count = len(handler.fs__refs_hash.storage_fs.files__paths())
            file_counts['refs_hash_files'] = refs_hash_count
            total_files += refs_hash_count
        except Exception:
            file_counts['refs_hash_files'] = 0

        try:
            refs_id_count = len(handler.fs__refs_id.storage_fs.files__paths())
            file_counts['refs_id_files'] = refs_id_count
            total_files += refs_id_count
        except Exception:
            file_counts['refs_id_files'] = 0

        file_counts['total_files'] = total_files

        return {
            'namespace': str(namespace),
            'handler': handler,
            'file_counts': file_counts,
            'total_files': total_files
        }

    def get_or_create_handler(self, namespace: Safe_Str__Id = DEFAULT_CACHE__NAMESPACE  # Get existing or create new cache handler
                              ) -> Cache__Handler:
        if namespace not in self.cache_handlers:
            handler = Cache__Handler(s3__bucket      = self.default_bucket,
                                     s3__prefix      = str(namespace),
                                     cache_ttl_hours = self.default_ttl_hours).setup()
            self.cache_handlers[namespace] = handler
        return self.cache_handlers[namespace]

    @cache_on_self
    def storage_fs(self) -> Storage_FS__S3:
        return Storage_FS__S3(s3_bucket=self.default_bucket).setup()

    def store_with_strategy(self, storage_data     : Any                                   ,
                                  cache_hash       : Safe_Str__Cache_Hash                  ,
                                  strategy         : Enum__Cache__Store__Strategy          ,
                                  cache_id         : Random_Guid                  = None                       ,
                                  cache_key        : Safe_Str__File__Path         = None   ,  # Allow extra key/path to be provided (used by some path_handlers)
                                  file_id          : Safe_Str__Id                      = None   ,
                                  namespace        : Safe_Str__Id                      = None   ,
                                  content_encoding : Safe_Str__Id                      = None
                            ) -> Schema__Cache__Store__Response:
        if not cache_hash:
            raise ValueError("in Cache__Service.store_with_strategy, the cache_hash must be provided")                      # todo: see if it makes sense for use to calculate the hash here

        cache_id  = cache_id or Random_Guid()
        namespace = namespace or Safe_Str__Id("default")
        handler   = self.get_or_create_handler(namespace)
        fs_data   = handler.get_fs_for_strategy(strategy)
        all_paths = { "data": [], "by_hash": [], "by_id" : []     }

        file_id   =  Safe_Str__Id  (file_id or cache_id )               # if not provided use the cache_id as file_id (needs casting to Safe_Str__Id)
        file_key  = Safe_Str__File__Path(cache_key)                     # use cache_key as file_key
        if isinstance(storage_data, bytes):                             # Determine file type based on storage data
            file_fs = fs_data.file__binary(file_id=file_id, file_key=file_key)
            file_type = "binary"
        else:
            file_fs = fs_data.file__json(file_id=file_id, file_key=file_key)
            file_type = "json"

        # Store actual data
        with file_fs:
            all_paths['data']  = file_fs.create(storage_data)
            content_file_paths = file_fs.file_fs__paths().paths__content()          # get the file paths for the content files

            # Add metadata
            metadata = { "cache_hash"       : str(cache_hash)    ,                  # todo: convert to Type_Safe class
                         "cache_key"        : str(cache_key)     ,
                         "cache_id"         : str(cache_id)      ,
                         "content_encoding" : content_encoding   ,
                         "file_id"          : str(file_id)       ,
                         "stored_at"        : timestamp_now()    ,
                         "strategy"         : strategy           ,
                         "namespace"        : str(namespace)     ,
                         "file_type"        : file_type          }
            file_fs.metadata__update(metadata)
            file_size = file_fs.metadata().content__size

        # Update hash->ID reference
        with handler.fs__refs_hash.file__json(Safe_Str__Id(cache_hash)) as ref_fs:
            if ref_fs.exists():
                refs = ref_fs.content()
                refs["cache_ids"     ].append({"id": str(cache_id), "timestamp": timestamp_now()})
                refs["latest_id"     ] = str(cache_id)
                refs["total_versions"] += 1
                paths__hash_to_id =  ref_fs.update(file_data=refs)
            else:
                refs = {"hash"           : str(cache_hash)                                     ,
                        "cache_ids"      : [{"id": str(cache_id), "timestamp": timestamp_now()}],
                        "latest_id"      : str(cache_id)                                        ,
                        "total_versions" : 1                                                    }
                paths__hash_to_id = ref_fs.create(file_data=refs)
            all_paths["by_hash"] = paths__hash_to_id

        # Update ID->hash reference WITH content path and file type
        with handler.fs__refs_id.file__json(Safe_Str__Id(str(cache_id))) as ref_fs:
            all_paths["by_id"] = ref_fs.paths()
            paths__id_to_hash  =  ref_fs.create({ "all_paths"        : all_paths          ,
                                                   "cache_id"         : str(cache_id)     ,
                                                   "hash"             : str(cache_hash)   ,
                                                   "namespace"        : str(namespace)    ,
                                                   "strategy"         : strategy          ,
                                                   "content_paths"    : content_file_paths,
                                                   "file_type"        : file_type         ,
                                                   "timestamp"        : timestamp_now()   })

        return Schema__Cache__Store__Response(cache_id   = cache_id     ,
                                              hash       = cache_hash   ,
                                              namespace  = namespace,
                                              paths      = all_paths    ,
                                              size       = file_size    )


    def retrieve_by_hash(self, cache_hash : Safe_Str__Cache_Hash,
                               namespace  : Safe_Str__Id = None
                          ) -> Optional[Dict[str, Any]]:                        # Retrieve latest by hash"""
        namespace = namespace or Safe_Str__Id("default")
        handler   = self.get_or_create_handler(namespace)

        with handler.fs__refs_hash.file__json(Safe_Str__Id(cache_hash)) as ref_fs:   # Get hash->ID mapping
            if not ref_fs.exists():
                return None
            refs = ref_fs.content()
            latest_id = refs.get("latest_id")

        if not latest_id:
            return None

        return self.retrieve_by_id(Random_Guid(latest_id), namespace)           # Delegate to retrieve_by_id which handles the path lookup

    def retrieve_by_id(self, cache_id : Random_Guid,
                             namespace : Safe_Str__Id = None
                        ) -> Optional[Dict[str, Any]]:                  #  Retrieve by cache ID using direct path from reference
        namespace = namespace or Safe_Str__Id("default")
        handler   = self.get_or_create_handler(namespace)

        # Get ID reference with content path
        with handler.fs__refs_id.file__json(Safe_Str__Id(cache_id)) as ref_fs:
            if not ref_fs.exists():
                return None
            ref_data = ref_fs.content()

        content_paths = ref_data.get("content_paths", [])
        file_type     = ref_data.get("file_type", "json")

        if not content_paths:
            return None

        # Get the storage backend
        storage = handler.fs__refs_id.storage_fs

        # Read the content file directly (first path is the main content)
        content_path = content_paths[0] if content_paths else None

        if content_path and storage.file__exists(content_path):
            if file_type == "binary":
                data = storage.file__bytes(content_path)
            else:
                data = storage.file__json(content_path)

            # Read metadata
            metadata_path = content_path + '.metadata'              # todo: review this usage since we should have a much better way to do this using Memory_FS
            metadata_data = {}

            if storage.file__exists(metadata_path):
                metadata_raw = storage.file__json(metadata_path)
                # Convert metadata keys to Safe_Str__Id for consistency
                metadata_data = metadata_raw.get('data')            # todo: use native Memory_FS methods here (and we should be using a Type_Safe class here)

            # Get content encoding from metadata
            content_encoding = metadata_data.get('content_encoding')

            # Handle decompression if needed
            if content_encoding == 'gzip' and isinstance(data, bytes):
                data = gzip.decompress(data)
                # After decompression, determine if it's JSON or remains binary
                try:                                            # todo: we should know this from the metadata/config
                    data = json.loads(data.decode('utf-8'))
                    data_type = "json"
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data_type = "binary"
            else:
                data_type = self._determine_data_type(data)

            return { "data"            : data             ,                 # convert to Type_Safe class
                     "metadata"        : metadata_data    ,
                     "data_type"       : data_type        ,
                     "content_encoding": content_encoding }

        return None

    #todo: this method should return Schema__Cache__Entry__Details (which should be the class used to save the file)
    def retrieve_by_id__config(self, cache_id  : Random_Guid,
                                     namespace : Safe_Str__Id    = None
                                ) -> Optional[Dict[str, Any]]:                      #   Retrieve by cache ID using direct path from reference
        namespace = namespace or Safe_Str__Id("default")
        handler   = self.get_or_create_handler(namespace)

        with handler.fs__refs_id.file__json(Safe_Str__Id(cache_id)) as ref_fs:           # get the main by-id file, which contains pointers to the other files
            if not ref_fs.exists():
                return None
            return ref_fs.content()

    # todo: check who is using this
    def _is_binary_data(self, metadata) -> bool:                                    # Check if stored data is binary based on metadata
        if not metadata:
            return False

        content_encoding = metadata.data.get(Safe_Str__Id('content_encoding'))      # Check for content encoding or binary indicators
        if content_encoding:
            return True

        # Could add more checks here based on content_type if we store it
        return False

    # todo: we should be using the Enum for this
    def _determine_data_type(self, data) -> str:                                    # Determine the type of data (string, json, binary)
        if isinstance(data, bytes):
            return "binary"
        elif isinstance(data, dict) or isinstance(data, list):
            return "json"
        else:
            return "string"

    def hash_from_string(self, data: str) -> Safe_Str__Cache_Hash:                       # Calculate hash from string
        return self.hash_generator.from_string(data)

    def hash_from_bytes(self, data: bytes) -> Safe_Str__Cache_Hash:                      # Calculate hash from bytes
        return self.hash_generator.from_bytes(data)

    def hash_from_json(self, data          : dict        ,                               # Calculate hash from JSON
                             exclude_fields : List[str] = None
                        ) -> Safe_Str__Cache_Hash:
        return self.hash_generator.from_json(data, exclude_fields)