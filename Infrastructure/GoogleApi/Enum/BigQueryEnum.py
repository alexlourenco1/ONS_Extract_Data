from enum import Enum


class WriteOption(Enum):
    WriteEmpty = 1,
    WriteAppend = 2,
    WriteTruncate = 3