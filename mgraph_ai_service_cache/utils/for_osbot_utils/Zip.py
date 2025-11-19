import hashlib
from osbot_utils.type_safe.primitives.domains.cryptography.safe_str.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash
from osbot_utils.utils.Zip                                                               import zip_bytes__files


def zip_bytes__content_hash(zip_bytes: bytes, hash_length: int) -> Safe_Str__Cache_Hash:     # Calculate hash based on ZIP content, not raw bytes. This ensures identical content produces the same hash regardless of  creation time or compression settings.

    files_dict  = zip_bytes__files(zip_bytes)                                       # Use existing zip_bytes__files to get all files and their content
    sorted_files = sorted(files_dict.items())                                       # Sort files by name for deterministic ordering
    hasher = hashlib.sha256()                                                       # Create hash from file paths and contents

    for file_path, file_content in sorted_files:                                    # Hash the file path
        hasher.update(file_path.encode('utf-8'))
        hasher.update(b'\x00'                  )                                    # Null separator between path and content
        hasher.update(file_content             )                                    # Hash the file content
        hasher.update(b'\x00'                  )                                    # Null separator between entries

    full_hash   = hasher.hexdigest()                                                # Get the full hash and truncate to configured length

    return Safe_Str__Cache_Hash(full_hash[:hash_length])