from enum import Enum

class Enum__Cache__Store__Strategy(str, Enum):
    DIRECT              = "direct"
    TEMPORAL            = "temporal"
    TEMPORAL_LATEST     = "temporal_latest"
    TEMPORAL_VERSIONED  = "temporal_versioned"
    KEY_BASED           = "key_based"
