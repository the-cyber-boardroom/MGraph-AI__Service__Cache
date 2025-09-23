from osbot_utils.type_safe.Type_Safe                                                     import Type_Safe
from osbot_utils.type_safe.primitives.domains.identifiers.Random_Guid                    import Random_Guid
from osbot_utils.type_safe.primitives.domains.identifiers.safe_int.Timestamp_Now         import Timestamp_Now


class Schema__Cache__Hash__Entry(Type_Safe):       # Individual cache ID entry in hash reference
    cache_id   : Random_Guid                       # Cache ID
    timestamp : Timestamp_Now