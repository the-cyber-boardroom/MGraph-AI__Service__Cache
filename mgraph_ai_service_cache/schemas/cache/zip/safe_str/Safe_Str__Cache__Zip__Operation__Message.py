import re
from osbot_utils.type_safe.primitives.core.Safe_Str import Safe_Str


class Safe_Str__Cache__Zip__Operation__Message(Safe_Str):
    regex = re.compile(r'[^a-zA-Z0-9:.\'"() ]')
