from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id     import Safe_Str__Id
from memory_fs.path_handlers.Path__Handler                                          import Path__Handler
from osbot_utils.type_safe.primitives.domains.files.safe_str.Safe_Str__File__Path   import Safe_Str__File__Path

class Path__Handler__Hash_Sharded(Path__Handler):                                  # Hash-based sharding path handler for references
    shard_depth : int = 2                                                          # Two levels: aa/bb/
    shard_size  : int = 2                                                          # 2 chars per level

    def generate_path(self, file_id : Safe_Str__Id         = None                   # allow the file_id to be used by overwritten methods
                          , file_key: Safe_Str__File__Path = None                   # allow the file_key to be used by overwritten methods
                       ) -> Safe_Str__File__Path:                                   # Generate sharded path based on hash
        if file_id is None:
            raise ValueError('In Path__Handler__Hash_Sharded, file_id cannot be None')
        hash_str = str(file_id)

        shards = []                                                                 # Generate sharded path: ab/cd/abcdef123456.json
        for i in range(self.shard_depth):
            start = i * self.shard_size
            end   = start + self.shard_size
            if start < len(hash_str):
                shards.append(hash_str[start:end])

        sharded_path = '/'.join(shards) if shards else ''
        return self.combine_paths(Safe_Str__File__Path(sharded_path))