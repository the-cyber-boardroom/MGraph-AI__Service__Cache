from enum import Enum

class Enum__Cache__Zip__Operation(str, Enum):
    LIST    = "list"
    GET     = "get"
    ADD     = "add"
    REMOVE  = "remove"
    REPLACE = "replace"
    RENAME  = "rename"

    def __str__(self):
        return self.value