from osbot_utils.utils.Misc                 import random_bytes, bytes_sha256
from memory_fs.schemas.Safe_Str__Cache_Hash import Safe_Str__Cache_Hash


class Random_Hash(Safe_Str__Cache_Hash, str):
    def __new__(cls, value=None):
        if value is None:
            hash_obj = bytes_sha256(random_bytes())     # hash from 32 bytes of randomness
            value = hash_obj[:16]                       # Take first 16 chars to match Safe_Str__Cache_Hash

        return str.__new__(cls, value)                  # let Safe_Str__Cache_Hash handle the validation