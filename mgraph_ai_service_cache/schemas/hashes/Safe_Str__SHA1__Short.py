import re
from osbot_utils.helpers.safe_str.Safe_Str                           import Safe_Str
from osbot_utils.helpers.safe_str.schemas.Enum__Safe_Str__Regex_Mode import Enum__Safe_Str__Regex_Mode

TYPE_SAFE_STR__GITHUB__SHA_SHORT__REGEX  = re.compile(r'^[a-fA-F0-9]{7}$')
TYPE_SAFE_STR__GITHUB__SHA_SHORT__LENGTH = 7

# todo: move to OSBot_Utils
class Safe_Str__SHA1__Short(Safe_Str):
    """
    Safe string class for short 7-character Git commit SHAs.

    Examples:
    - "7fd1a60"
    - "abc1234"
    """
    regex             = TYPE_SAFE_STR__GITHUB__SHA_SHORT__REGEX
    regex_mode        = Enum__Safe_Str__Regex_Mode.MATCH
    max_length        = TYPE_SAFE_STR__GITHUB__SHA_SHORT__LENGTH
    exact_length      = True
    allow_empty       = False
    trim_whitespace   = True
    strict_validation = True
