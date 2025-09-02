from typing                                                                        import Dict, Optional, Any, List, Literal
from osbot_utils.type_safe.Type_Safe                                               import Type_Safe
from osbot_utils.utils.Dev import pprint
from osbot_utils.utils.Json                                                        import json_to_str
from osbot_utils.type_safe.primitives.safe_str.identifiers.Random_Guid             import Random_Guid
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id                 import Safe_Id
from osbot_utils.utils.Misc import timestamp_now, list_set
from mgraph_ai_service_cache.schemas.hashes.Safe_Str__Cache_Hash                   import Safe_Str__Cache_Hash
from mgraph_ai_service_cache.service.cache.Cache__Handler                          import Cache__Handler
from mgraph_ai_service_cache.service.cache.Cache__Hash__Config                     import Cache__Hash__Config
from mgraph_ai_service_cache.service.cache.Cache__Hash__Generator                  import Cache__Hash__Generator
from mgraph_ai_service_cache.schemas.cache.Schema__Cache__Store__Response          import Schema__Cache__Store__Response

DEFAULT__CACHE__SERVICE__BUCKET_NAME        = "mgraph-ai-cache"
DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS  = 24

class Cache__Service(Type_Safe):                                                   # Main cache service orchestrator
    cache_handlers    : Dict[Safe_Id, Cache__Handler]                              # Multiple cache handlers by namespace
    default_bucket    : str                           = DEFAULT__CACHE__SERVICE__BUCKET_NAME
    default_ttl_hours : int                           = DEFAULT__CACHE__SERVICE__DEFAULT_TTL_HOURS
    hash_config       : Cache__Hash__Config                                        # Hash generation config
    hash_generator    : Cache__Hash__Generator                                     # Hash generator instance

    def delete_by_id(self, cache_id: Random_Guid, namespace: Safe_Id = None) -> Dict[str, Any]:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        with handler.fs__refs_id.file__json(Safe_Id(str(cache_id))) as ref_fs:
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
            with handler.fs__refs_hash.file__json(Safe_Id(cache_hash)) as ref_fs:
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
            counts_data = self.get_namespace_file_counts(namespace)
            all_stats[str(namespace)] = {
                'total_files': counts_data['total_files'],
                'file_counts': counts_data['file_counts']
            }

        return {
            'namespaces': all_stats,
            'total_namespaces': len(self.cache_handlers),
            'grand_total_files': sum(ns['total_files'] for ns in all_stats.values())
        }
    def get_namespace_file_counts(self, namespace: Safe_Id = None) -> Dict[str, Any]:       # Get file counts for all strategies in a namespace
        namespace = namespace or Safe_Id("default")
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

    def get_or_create_handler(self, namespace: Safe_Id = None                      # Get existing or create new cache handler
                              ) -> Cache__Handler:
        namespace = namespace or Safe_Id("default")
        if namespace not in self.cache_handlers:
            handler = Cache__Handler(s3__bucket      = self.default_bucket,
                                     s3__prefix      = str(namespace),
                                     cache_ttl_hours = self.default_ttl_hours).setup()
            self.cache_handlers[namespace] = handler
        return self.cache_handlers[namespace]

    def store_with_strategy(self, cache_key_data   : Any                          ,  # Store data with clear separation
                                  storage_data     : Any                          ,
                                  cache_hash       : Safe_Str__Cache_Hash         ,
                                  cache_id         : Random_Guid                  ,
                                  strategy         : Literal["direct", "temporal", "temporal_latest", "temporal_versioned"],
                                  namespace        : Safe_Id = None               ,
                                  content_encoding : str = None
                            ) -> Schema__Cache__Store__Response:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)
        fs_data   = handler.get_fs_for_strategy(strategy)
        all_paths = { "data": {}, "by_hash": {}, "by_id" : {}     }                                # todo: this should be an Type_Safe class

        if cache_key_data is dict:                                                  # if the cache_key_data is a dict
            cache_key_data = json_to_str(cache_key_data)                            #    convert it to str

        # Store actual data
        with fs_data.file__json(Safe_Id(str(cache_id))) as file_fs:
            all_paths['data'] = file_fs.create(storage_data)

            # Add metadata
            metadata = { "cache_hash"       : str(cache_hash)    ,
                         "cache_key_data"   : str(cache_key_data),
                         "cache_id"         : str(cache_id)      ,
                         "content_encoding" : content_encoding   ,
                         "stored_at"        : timestamp_now()    ,
                         "strategy"         : strategy           ,
                         "namespace"        : str(namespace)     }
            file_fs.metadata__update(metadata)
            file_size = file_fs.metadata().content__size

        # tood: move this update xrefs into different folder
        # Update hash->ID reference
        with handler.fs__refs_hash.file__json(Safe_Id(cache_hash)) as ref_fs:
            if ref_fs.exists():
                refs = ref_fs.content() # todo: we should be using a Type_Safe class here
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


        # Update ID->hash reference
        with handler.fs__refs_id.file__json(Safe_Id(str(cache_id))) as ref_fs:
            all_paths["by_id"] = ref_fs.paths()                                         # we need to calculate these or we would need todo an extra update call to capture these by_id files
            paths__id_to_hash  =  ref_fs.create({ "all_paths"  : all_paths        ,
                                                   "cache_id"   : str(cache_id)    ,
                                                   "hash"       : str(cache_hash)  ,
                                                   "namespace"  : str(namespace)   ,
                                                   "strategy"   : strategy         ,
                                                   "timestamp"  : timestamp_now()  })

        return Schema__Cache__Store__Response(cache_id   = cache_id     ,
                                              hash       = cache_hash   ,
                                              paths      = all_paths    ,
                                              size       = file_size    )

    def retrieve_by_hash(self, cache_hash : Safe_Str__Cache_Hash       ,                 # Retrieve latest by hash
                              namespace   : Safe_Id = None
                         ) -> Optional[Dict[str, Any]]:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        # Get hash->ID mapping
        with handler.fs__refs_hash.file__json(Safe_Id(cache_hash)) as ref_fs:
            if not ref_fs.exists():
                return None
            refs = ref_fs.content()
            latest_id = refs.get("latest_id")

        if not latest_id:
            return None

        # Get data using cache ID
        return self.retrieve_by_id(Random_Guid(latest_id), namespace)

    def retrieve_by_id(self, cache_id : Random_Guid              ,                 # Retrieve by cache ID
                            namespace : Safe_Id = None
                       ) -> Optional[Dict[str, Any]]:
        namespace = namespace or Safe_Id("default")
        handler   = self.get_or_create_handler(namespace)

        # Get ID->hash mapping to find strategy
        with handler.fs__refs_id.file__json(Safe_Id(str(cache_id))) as ref_fs:
            if not ref_fs.exists():
                return None
            ref_data = ref_fs.content()                         # todo: review the use of this variable since it not currently being used at the moment

        # Try each strategy to find the data
        for strategy in ["direct", "temporal", "temporal_latest", "temporal_versioned"]:
            fs_data = handler.get_fs_for_strategy(strategy)
            with fs_data.file__json(Safe_Id(str(cache_id))) as file_fs:
                if file_fs.exists():
                    data     = file_fs.content()
                    metadata = file_fs.metadata()
                    return {"data": data, "metadata": metadata.data if metadata else {}}

        return None

    def hash_from_string(self, data: str) -> Safe_Str__Cache_Hash:                       # Calculate hash from string
        return self.hash_generator.from_string(data)

    def hash_from_bytes(self, data: bytes) -> Safe_Str__Cache_Hash:                      # Calculate hash from bytes
        return self.hash_generator.from_bytes(data)

    def hash_from_json(self, data          : dict        ,                         # Calculate hash from JSON
                            exclude_fields : List[str] = None
                       ) -> Safe_Str__Cache_Hash:
        return self.hash_generator.from_json(data, exclude_fields)

    def list_namespaces(self) -> List[Safe_Id]:                                    # List all active namespaces
        return list_set(self.cache_handlers)