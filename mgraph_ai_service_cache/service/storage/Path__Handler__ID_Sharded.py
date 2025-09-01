from memory_fs.path_handlers.Path__Handler                                     import Path__Handler
from osbot_utils.type_safe.primitives.safe_str.filesystem.Safe_Str__File__Path import Safe_Str__File__Path
from osbot_utils.type_safe.primitives.safe_str.identifiers.Safe_Id             import Safe_Id

class Path__Handler__ID_Sharded(Path__Handler):                                    # ID-based sharding path handler for references
    shard_depth : int = 2                                                          # Two levels: aa/bb/
    shard_size  : int = 2                                                          # 2 chars per level

    def generate_path(self, file_id: Safe_Id) -> Safe_Str__File__Path:            # Generate sharded path based on cache ID
        id_str = str(file_id)

        shards = []
        for i in range(self.shard_depth):
            start = i * self.shard_size
            end   = start + self.shard_size
            if start < len(id_str):
                shards.append(id_str[start:end])

        sharded_path = '/'.join(shards) if shards else ''
        return self.combine_paths(Safe_Str__File__Path(sharded_path))