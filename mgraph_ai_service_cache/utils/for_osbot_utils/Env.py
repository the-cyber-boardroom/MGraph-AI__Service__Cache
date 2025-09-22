from typing import Type, TypeVar, Optional
from enum import Enum
from osbot_utils.type_safe.Type_Safe__Primitive import Type_Safe__Primitive
from osbot_utils.utils.Env import get_env

T = TypeVar('T', bound=Enum)
P = TypeVar('P', bound=Type_Safe__Primitive)

def get_env_enum(env_var_name : str                ,                            # Environment variable name to look up
                 enum_class   : Type[T]            ,                            # Enum class to convert to
                 default      : Optional[T] = None                              # Optional default if env var not set or invalid
            ) -> Optional[T]:                                                   # Returns enum value or default
    env_value = get_env(env_var_name)
    if not env_value:
        return default

    try:                                                                        # Convert string to appropriate type based on enum's base class
        if issubclass(enum_class, int):                                         # Integer-based enums
            env_value = int(env_value)
        elif issubclass(enum_class, float) and not issubclass(enum_class, int): # Float-based enums (for numeric thresholds, etc.)
            env_value = float(env_value)
        elif issubclass(enum_class, bytes):                                     # Bytes-based enums (rare, for binary protocols)
            env_value = env_value.encode('utf-8')
        else:
            pass                                                                # str enums pass through as-is (most common case)

        return enum_class(env_value)
    except (ValueError, KeyError, UnicodeEncodeError):
        return default

def get_env_primitive(env_var_name  : str                     ,                   # Environment variable name to look up
                      primitive_class: Type[P]                ,                   # Type_Safe__Primitive class to convert to
                      default        : Optional[P] = None                         # Optional default if env var not set or invalid
                      ) -> Optional[P]:                                           # Returns primitive value or default
    env_value = get_env(env_var_name)
    if not env_value:
        return default

    try:
        return primitive_class(env_value)
    except (ValueError, TypeError):
        return default