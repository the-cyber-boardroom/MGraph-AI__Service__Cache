from typing                                                                                  import Any, List, Dict, Optional
from osbot_utils.decorators.methods.cache_on_self                                            import cache_on_self
from osbot_utils.type_safe.Type_Safe                                                         import Type_Safe
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path            import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                        import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id              import Safe_Str__Id
from osbot_utils.type_safe.primitives.domains.common.safe_str.Safe_Str__Text   import Safe_Str__Text
from osbot_utils.type_safe.primitives.core.Safe_UInt                           import Safe_UInt
from osbot_utils.utils.Http                                                    import url_join_safe
from mgraph_ai_service_cache.schemas.cache.consts__Cache_Service               import DEFAULT_CACHE__NAMESPACE
from mgraph_ai_service_cache.schemas.cache.enums.Enum__Cache__Data_Type        import Enum__Cache__Data_Type
from mgraph_ai_service_cache.service.cache.Cache__Service                      import Cache__Service
from mgraph_ai_service_cache.service.cache.retrieve.Cache__Service__Retrieve   import Cache__Service__Retrieve


class Schema__Data__File__Info(Type_Safe):                                                      # Information about a data file
    data_file_id : Safe_Str__Id                                                                 # Data file identifier
    data_key     : Safe_Str__File__Path                                                         # Path within data folder
    full_path    : Safe_Str__File__Path                                                         # Full file path
    data_type    : Enum__Cache__Data_Type                                                       # Data type: json, string, binary
    extension    : Safe_Str__Text                                                               # File extension
    size         : Safe_UInt                                                                    # File size in bytes


class Schema__Data__File__Content(Type_Safe):                                                   # Data file with its content
    data         : Any                                                                          # Actual file content
    data_type    : Enum__Cache__Data_Type                                                       # Data type: json, string, binary
    data_file_id : Safe_Str__Id                                                                 # Data file identifier
    data_key     : Safe_Str__File__Path                                                         # Path within data folder
    full_path    : Safe_Str__File__Path                                                         # Full file path
    size         : Safe_UInt                                                                    # File size in bytes


class Cache__Service__Retrieve__Data(Type_Safe):                                                # Service for retrieving data files
    cache_service : Cache__Service                                                              # Underlying cache service instance

    @cache_on_self
    def retrieve_service(self):
        return Cache__Service__Retrieve(cache_service = self.cache_service)

    @type_safe
    def retrieve_data(self, cache_id     : Random_Guid                                        ,  # Cache entry ID
                            data_key     : Safe_Str__File__Path         = None               ,  # Path within data folder
                            data_file_id : Safe_Str__Id                 = None               ,  # Specific file ID
                            namespace    : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE
                      ) -> Optional[Schema__Data__File__Content]:                               # Returns data content or None

        if not cache_id:
            raise ValueError("cache_id is required to retrieve data")

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id=cache_id, namespace=namespace)
        if not file_refs:
            return None

        data_folders = file_refs.file_paths.data_folders
        if not data_folders:
            return None

        # Build target path
        for data_folder in data_folders:
            target_path = data_folder
            if data_key:
                target_path = url_join_safe(target_path, data_key)
            if data_file_id:
                target_path = url_join_safe(target_path, data_file_id)

            handler = self.cache_service.get_or_create_handler(namespace)

            # Try different extensions to find the file
            for ext, data_type in [('.json', Enum__Cache__Data_Type.JSON  ),
                                   ('.txt' , Enum__Cache__Data_Type.STRING),
                                   ('.bin' , Enum__Cache__Data_Type.BINARY)]:
                full_path = Safe_Str__File__Path(str(target_path) + ext)

                if handler.storage_backend.file__exists(full_path):
                    # Read file based on type
                    if data_type == Enum__Cache__Data_Type.JSON:
                        data = handler.storage_backend.file__json(full_path)
                    elif data_type == Enum__Cache__Data_Type.STRING:
                        data = handler.storage_backend.file__str(full_path)
                    else:  # BINARY
                        data = handler.storage_backend.file__bytes(full_path)

                    file_size = handler.storage_backend.file__size(full_path) if hasattr(handler.storage_backend, 'file__size') else len(str(data))

                    return Schema__Data__File__Content(data         = data                          ,
                                                       data_type    = data_type                      ,
                                                       data_file_id = data_file_id or Safe_Str__Id(""),
                                                       data_key     = data_key or Safe_Str__File__Path(""),
                                                       full_path    = full_path                      ,
                                                       size         = Safe_UInt(file_size)           )
        return None

    @type_safe
    def list_data_files(self, cache_id  : Random_Guid                                         ,  # Cache entry ID
                              data_key  : Safe_Str__File__Path          = None               ,  # Filter by path
                              namespace : Safe_Str__Id                  = DEFAULT_CACHE__NAMESPACE
                        ) -> List[Schema__Data__File__Info]:                                    # List all data files

        if not cache_id:
            raise ValueError("cache_id is required to list data files")

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id=cache_id, namespace=namespace)
        if not file_refs:
            return []

        data_folders = file_refs.file_paths.data_folders
        if not data_folders:
            return []

        handler = self.cache_service.get_or_create_handler(namespace)
        data_files = []

        for data_folder in data_folders:
            search_path = data_folder
            if data_key:
                search_path = url_join_safe(search_path, data_key)

            # List files in the folder
            if hasattr(handler.storage_backend, 'folder__files'):
                files = handler.storage_backend.folder__files(search_path, return_full_path=True)
            else:
                # Fallback to filtering all files
                all_files = handler.storage_backend.files__paths()
                search_str = str(search_path) + '/'
                files = [f for f in all_files if str(f).startswith(search_str)]

            # Parse file information
            for file_path in files:
                file_str = str(file_path)

                # Extract relative path from data folder
                relative_path = file_str.replace(str(data_folder) + '/', '')

                # Extract file name and extension
                if '/' in relative_path:
                    path_parts = relative_path.rsplit('/', 1)
                    relative_key = Safe_Str__File__Path(path_parts[0])
                    filename = path_parts[1]
                else:
                    relative_key = Safe_Str__File__Path("")
                    filename = relative_path

                if '.' in filename:
                    file_id_part = filename.rsplit('.', 1)[0]
                    extension = filename.rsplit('.', 1)[1]

                    # Determine data type from extension
                    if extension == 'txt':
                        data_type = Enum__Cache__Data_Type.STRING
                    elif extension == 'json':
                        data_type = Enum__Cache__Data_Type.JSON
                    else:
                        data_type = Enum__Cache__Data_Type.BINARY

                    # Get file size
                    file_size = handler.storage_backend.file__size(file_path) if hasattr(handler.storage_backend, 'file__size') else 0

                    data_files.append(Schema__Data__File__Info(
                        data_file_id = Safe_Str__Id(file_id_part)           ,
                        data_key     = relative_key                         ,
                        full_path    = Safe_Str__File__Path(file_str)       ,
                        data_type    = data_type                            ,
                        extension    = Safe_Str__Text(extension)            ,
                        size         = Safe_UInt(file_size)                 ))

        return data_files

    @type_safe
    def count_data_files(self, cache_id  : Random_Guid                                        ,
                               data_key  : Safe_Str__File__Path         = None               ,
                               namespace : Safe_Str__Id                 = DEFAULT_CACHE__NAMESPACE
                         ) -> Safe_UInt:                                                        # Count data files
        data_files = self.list_data_files(cache_id, data_key, namespace)
        return Safe_UInt(len(data_files))

    @type_safe
    def delete_data_file(self, cache_id     : Random_Guid                                     ,  # Delete specific data file
                               data_key     : Safe_Str__File__Path      = None               ,
                               data_file_id : Safe_Str__Id              = None               ,
                               namespace    : Safe_Str__Id              = DEFAULT_CACHE__NAMESPACE
                         ) -> bool:                                                              # Returns True if deleted

        if not cache_id:
            raise ValueError("cache_id is required to delete data")
        if not data_file_id:
            raise ValueError("data_file_id is required to delete specific data file")

        file_refs = self.retrieve_service().retrieve_by_id__refs(cache_id=cache_id, namespace=namespace)
        if not file_refs:
            return False

        data_folders = file_refs.file_paths.data_folders
        if not data_folders:
            return False

        handler = self.cache_service.get_or_create_handler(namespace)
        deleted = False

        for data_folder in data_folders:
            target_path = data_folder
            if data_key:
                target_path = url_join_safe(target_path, data_key)
            target_path = url_join_safe(target_path, data_file_id)

            # Try to delete all possible extensions
            for ext in ['.json', '.txt', '.bin']:
                full_path = Safe_Str__File__Path(str(target_path) + ext)
                if handler.storage_backend.file__exists(full_path):
                    deleted = handler.storage_backend.file__delete(full_path) or deleted

        return deleted

    @type_safe
    def delete_all_data_files(self, cache_id  : Random_Guid                                   ,
                                    data_key  : Safe_Str__File__Path     = None              ,
                                    namespace : Safe_Str__Id             = DEFAULT_CACHE__NAMESPACE
                              ) -> Safe_UInt:                                                   # Delete all data files, return count

        data_files = self.list_data_files(cache_id, data_key, namespace)
        deleted_count = Safe_UInt(0)

        for file_info in data_files:
            if self.delete_data_file(cache_id     = cache_id                                  ,
                                     data_key     = file_info.data_key                        ,
                                     data_file_id = file_info.data_file_id                    ,
                                     namespace    = namespace                                 ):
                deleted_count = Safe_UInt(deleted_count + 1)

        return deleted_count

    @type_safe
    def get_data_folder_size(self, cache_id  : Random_Guid                                    ,  # Get total size of data folder
                                   namespace : Safe_Str__Id              = DEFAULT_CACHE__NAMESPACE
                             ) -> Safe_UInt:

        data_files = self.list_data_files(cache_id, namespace=namespace)
        total_size = Safe_UInt(0)

        for file_info in data_files:
            total_size = Safe_UInt(total_size + file_info.size)

        return total_size