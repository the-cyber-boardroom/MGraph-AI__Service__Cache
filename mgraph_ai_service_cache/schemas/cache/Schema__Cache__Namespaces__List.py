from osbot_utils.type_safe.Type_Safe                                              import Type_Safe
from osbot_utils.type_safe.primitives.core.Safe_UInt                              import Safe_UInt


class Schema__Cache__Namespaces__List(Type_Safe):                                     # List of namespaces
    namespaces : list                                                                 # List of namespace names
    count      : Safe_UInt                                                            # Total count
