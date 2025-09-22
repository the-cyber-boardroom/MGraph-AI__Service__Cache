from unittest                                                                     import TestCase
from enum                                                                         import Enum
from mgraph_ai_service_cache.utils.for_osbot_utils.Env                            import get_env_enum, get_env_primitive
from osbot_utils.utils.Env                                                        import set_env, del_env
from osbot_utils.type_safe.primitives.core.Safe_Int                               import Safe_Int
from osbot_utils.type_safe.primitives.domains.network.safe_uint.Safe_UInt__Port   import Safe_UInt__Port
from osbot_utils.type_safe.primitives.domains.web.safe_str.Safe_Str__Url          import Safe_Str__Url
from osbot_utils.type_safe.primitives.domains.identifiers.safe_str.Safe_Str__Id   import Safe_Str__Id

# Test enums
class Enum__Cache__Storage_Mode(str, Enum):
    MEMORY     = "memory"
    S3         = "s3"
    LOCAL_DISK = "local_disk"
    SQLITE     = "sqlite"
    ZIP        = "zip"

class Enum__Log_Level(str, Enum):
    DEBUG   = "DEBUG"                                                            # Mixed case enum values
    INFO    = "INFO"
    WARNING = "WARNING"
    ERROR   = "ERROR"

class Enum__Priority(int, Enum):                                                 # Integer-based enum
    LOW    = 1
    MEDIUM = 2
    HIGH   = 3

# Additional test enums
class Enum__Threshold(float, Enum):                                              # Float-based enum
    LOW    = 0.25
    MEDIUM = 0.5
    HIGH   = 0.75
    MAX    = 1.0

class Enum__Binary_Protocol(bytes, Enum):                                        # Bytes-based enum
    START = b'\x02'
    END   = b'\x03'
    ACK   = b'\x06'
    NAK   = b'\x15'

class test_Env__get_env_enum_and_primitive(TestCase):

    @classmethod
    def setUpClass(cls):                                                         # One-time setup for test data
        cls.test_env_vars = [
            "TEST_STORAGE_MODE",
            "TEST_LOG_LEVEL",
            "TEST_PRIORITY",
            "TEST_PORT",
            "TEST_URL",
            "TEST_SERVICE_ID"
        ]

    def tearDown(self):                                                          # Clean up env vars after each test
        for var in self.test_env_vars:
            del_env(var)

    # get_env_enum tests

    def test_get_env_enum(self):                                                 # Test basic enum conversion
        set_env("TEST_STORAGE_MODE", "s3")

        result = get_env_enum("TEST_STORAGE_MODE", Enum__Cache__Storage_Mode)
        assert type(result) is Enum__Cache__Storage_Mode
        assert result       == Enum__Cache__Storage_Mode.S3
        assert result.value == "s3"

    def test_get_env_enum__with_default(self):                                   # Test default value handling
        # No env var set - should return default
        result = get_env_enum("TEST_STORAGE_MODE",
                             Enum__Cache__Storage_Mode,
                             Enum__Cache__Storage_Mode.MEMORY)
        assert result == Enum__Cache__Storage_Mode.MEMORY

        # Invalid value - should return default
        set_env("TEST_STORAGE_MODE", "invalid_mode")
        result = get_env_enum("TEST_STORAGE_MODE",
                             Enum__Cache__Storage_Mode,
                             Enum__Cache__Storage_Mode.SQLITE)
        assert result == Enum__Cache__Storage_Mode.SQLITE

    def test_get_env_enum__no_default(self):                                     # Test None return when no default
        # No env var set - should return None
        result = get_env_enum("TEST_STORAGE_MODE", Enum__Cache__Storage_Mode)
        assert result is None

        # Invalid value - should return None
        set_env("TEST_STORAGE_MODE", "not_a_valid_mode")
        result = get_env_enum("TEST_STORAGE_MODE", Enum__Cache__Storage_Mode)
        assert result is None

    def test_get_env_enum__case_sensitive(self):                                 # Test case sensitivity is preserved
        # Lowercase value for lowercase enum
        set_env("TEST_STORAGE_MODE", "memory")
        result = get_env_enum("TEST_STORAGE_MODE", Enum__Cache__Storage_Mode)
        assert result == Enum__Cache__Storage_Mode.MEMORY

        # UPPERCASE value should fail for lowercase enum
        set_env("TEST_STORAGE_MODE", "MEMORY")
        result = get_env_enum("TEST_STORAGE_MODE", Enum__Cache__Storage_Mode)
        assert result is None                                                    # Case mismatch - no conversion

        # Mixed case enum requires exact match
        set_env("TEST_LOG_LEVEL", "DEBUG")
        result = get_env_enum("TEST_LOG_LEVEL", Enum__Log_Level)
        assert result == Enum__Log_Level.DEBUG

        set_env("TEST_LOG_LEVEL", "debug")                                       # Wrong case
        result = get_env_enum("TEST_LOG_LEVEL", Enum__Log_Level)
        assert result is None

    def test_get_env_enum__integer_enum(self):                                   # Test integer-based enums
        set_env("TEST_PRIORITY", "2")
        result = get_env_enum("TEST_PRIORITY", Enum__Priority)
        assert result == Enum__Priority.MEDIUM
        assert result.value == 2

        set_env("TEST_PRIORITY", "99")                                          # Invalid integer value
        result = get_env_enum("TEST_PRIORITY", Enum__Priority)
        assert result is None

        set_env("TEST_PRIORITY", "high")                                        # Non-integer value
        result = get_env_enum("TEST_PRIORITY", Enum__Priority)
        assert result is None

    def test_get_env_enum__float_enum(self):                                     # Test float-based enums
        set_env("TEST_THRESHOLD", "0.5")
        result = get_env_enum("TEST_THRESHOLD", Enum__Threshold)
        assert result == Enum__Threshold.MEDIUM
        assert result.value == 0.5

        # Scientific notation
        set_env("TEST_THRESHOLD", "2.5e-1")                                      # 0.25
        result = get_env_enum("TEST_THRESHOLD", Enum__Threshold)
        assert result == Enum__Threshold.LOW

        # Integer string converts to float
        set_env("TEST_THRESHOLD", "1")
        result = get_env_enum("TEST_THRESHOLD", Enum__Threshold)
        assert result == Enum__Threshold.MAX
        assert result.value == 1.0

        # Invalid float value
        set_env("TEST_THRESHOLD", "0.333")                                       # Not in enum
        result = get_env_enum("TEST_THRESHOLD", Enum__Threshold)
        assert result is None

        # Non-numeric string
        set_env("TEST_THRESHOLD", "high")
        result = get_env_enum("TEST_THRESHOLD", Enum__Threshold)
        assert result is None

    def test_get_env_enum__bytes_enum(self):                                     # Test bytes-based enums
        # Bytes enums probably expect UTF-8 encoded strings
        set_env("TEST_PROTOCOL", "\x02")                                         # START character
        result = get_env_enum("TEST_PROTOCOL", Enum__Binary_Protocol)
        assert result == Enum__Binary_Protocol.START
        assert result.value == b'\x02'

        # Regular string that doesn't match
        set_env("TEST_PROTOCOL", "START")
        result = get_env_enum("TEST_PROTOCOL", Enum__Binary_Protocol)
        assert result is None                                                    # b'START' not in enum

    # get_env_primitive tests

    def test_get_env_primitive__safe_uint_port(self):                            # Test Safe_UInt__Port conversion
        set_env("TEST_PORT", "8080")

        with get_env_primitive("TEST_PORT", Safe_UInt__Port) as result:
            assert type(result) is Safe_UInt__Port
            assert result       == 8080

        # Out of range port
        set_env("TEST_PORT", "70000")                                           # > 65535
        result = get_env_primitive("TEST_PORT", Safe_UInt__Port)
        assert result is None                                                    # Validation failed

        # Negative port
        set_env("TEST_PORT", "-1")
        result = get_env_primitive("TEST_PORT", Safe_UInt__Port)
        assert result is None

    def test_get_env_primitive__safe_str_url(self):                              # Test Safe_Str__Url conversion
        test_url = "https://example.com/api/v1"
        set_env("TEST_URL", test_url)

        result = get_env_primitive("TEST_URL", Safe_Str__Url)
        assert type(result) is Safe_Str__Url
        assert result       == test_url

        # Invalid URL (too long)
        long_url = "https://example.com/" + "a" * 3000                          # > 2048 char limit
        set_env("TEST_URL", long_url)
        result = get_env_primitive("TEST_URL", Safe_Str__Url)
        assert result is None

    def test_get_env_primitive__safe_str_id(self):                               # Test Safe_Str__Id conversion
        set_env("TEST_SERVICE_ID", "service-123_abc")

        result = get_env_primitive("TEST_SERVICE_ID", Safe_Str__Id)
        assert type(result) is Safe_Str__Id
        assert result       == "service-123_abc"

        # Special characters get sanitized
        set_env("TEST_SERVICE_ID", "service@123#abc")
        result = get_env_primitive("TEST_SERVICE_ID", Safe_Str__Id)
        assert type(result) is Safe_Str__Id
        assert result       == "service_123_abc"                                 # @ and # replaced with _

    def test_get_env_primitive__with_default(self):                              # Test default handling for primitives
        default_port = Safe_UInt__Port(3000)

        # No env var - use default
        result = get_env_primitive("TEST_PORT", Safe_UInt__Port, default_port)
        assert result is default_port                                            # Same instance
        assert result == 3000

        # Invalid value - use default
        set_env("TEST_PORT", "not_a_number")
        result = get_env_primitive("TEST_PORT", Safe_UInt__Port, default_port)
        assert result is default_port

    def test_get_env_primitive__safe_int(self):                                  # Test signed integer primitives
        class Safe_Int__Temperature(Safe_Int):
            min_value = -273                                                     # Absolute zero
            max_value = 5000

        set_env("TEST_TEMP", "-40")
        result = get_env_primitive("TEST_TEMP", Safe_Int__Temperature)
        assert type(result) is Safe_Int__Temperature
        assert result       == -40

        # Below minimum
        set_env("TEST_TEMP", "-300")
        result = get_env_primitive("TEST_TEMP", Safe_Int__Temperature)
        assert result is None                                                    # Out of bounds

    def test_get_env_primitive__empty_string(self):                              # Test empty string handling
        set_env("TEST_SERVICE_ID", "")

        # Empty string returns None (no value)
        result = get_env_primitive("TEST_SERVICE_ID", Safe_Str__Id)
        assert result is None

        # Same with default
        default_id = Safe_Str__Id("default-id")
        result = get_env_primitive("TEST_SERVICE_ID", Safe_Str__Id, default_id)
        assert result is default_id

    def test_get_env_primitive__type_conversion_chain(self):                     # Test the conversion process
        # String "123" -> Safe_UInt -> value 123
        set_env("TEST_PORT", "123")
        result = get_env_primitive("TEST_PORT", Safe_UInt__Port)
        assert result == 123
        assert type(result) is Safe_UInt__Port

        # Float string is not value
        set_env("TEST_PORT", "123.456")
        result = get_env_primitive("TEST_PORT", Safe_UInt__Port)
        assert result is None

    def test_get_env_primitive__error_scenarios(self):                           # Test various error conditions
        # Non-existent env var
        assert get_env_primitive("DOES_NOT_EXIST", Safe_Str__Id) is None

        # Wrong type that can't convert
        set_env("TEST_PORT", "hello world")
        assert get_env_primitive("TEST_PORT", Safe_UInt__Port) is None

        # Dict/list values (from JSON perhaps)
        set_env("TEST_PORT", "{'port': 8080}")
        assert get_env_primitive("TEST_PORT", Safe_UInt__Port) is None