from enum import Enum

class Enum__Cache__Storage_Mode(str, Enum):
    MEMORY     = "memory"      # Default - no dependencies, fully in-memory
    S3         = "s3"          # AWS S3 or S3-compatible storage
    LOCAL_DISK = "local_disk"  # Local file system storage
    SQLITE     = "sqlite"      # SQLite database storage
    ZIP        = "zip"         # ZIP archive storage