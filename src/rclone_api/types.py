from enum import Enum


class ModTimeStrategy(Enum):
    USE_SERVER_MODTIME = "use-server-modtime"
    NO_MODTIME = "no-modtime"


class ListingOption(Enum):
    DIRS_ONLY = "dirs-only"
    FILES_ONLY = "files-only"
    ALL = "all"


class Order(Enum):
    NORMAL = "normal"
    REVERSE = "reverse"
    RANDOM = "random"


class GroupingOption(Enum):
    BUCKET = "bucket"
    REMOTE = "remote"
