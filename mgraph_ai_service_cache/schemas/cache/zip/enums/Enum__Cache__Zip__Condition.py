from enum import Enum

class Enum__Cache__Zip__Condition(str, Enum):
    ALWAYS        = "always"
    IF_EXISTS     = "if_exists"
    IF_NOT_EXISTS = "if_not_exists"