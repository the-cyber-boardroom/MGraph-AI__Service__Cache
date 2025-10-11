from enum                            import Enum
from osbot_utils.type_safe.Type_Safe import Type_Safe

# todo: refactor to schemas folder

class Enum__Hash__Algorithm(Enum):
    MD5    = "md5"
    SHA256 = "sha256"
    SHA384 = "sha384"

class Cache__Hash__Config(Type_Safe):                                              # Configuration for hash generation
    algorithm : Enum__Hash__Algorithm = Enum__Hash__Algorithm.SHA256               # Hash algorithm to use
    length    : int                   = 16                                         # Hash length: 10, 16, 32, 64, 96