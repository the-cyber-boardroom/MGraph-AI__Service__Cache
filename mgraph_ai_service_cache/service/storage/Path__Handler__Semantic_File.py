from memory_fs.path_handlers.Path__Handler                                        import Path__Handler
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.domains.identifiers.Safe_Id                 import Safe_Id

class Path__Handler__Semantic_File(Path__Handler):                                  # Hash-based sharding path handler for references

    def generate_path(self, file_id : Safe_Id              = None,
                            file_key: Safe_Str__File__Path = None
                       ) -> Safe_Str__File__Path:                                   # Generate sharded path based on hash
        if file_id is None:
            raise ValueError('In Path__Handler__Semantic_File, file_id cannot be None')

        if file_key is None:
            raise ValueError('In Path__Handler__Semantic_File, file_key cannot be None')
        #hash_str = str(file_id)

        return self.combine_paths(file_key)