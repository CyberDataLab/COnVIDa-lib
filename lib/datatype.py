from enum import Enum

class DataType(Enum):
    """
    Perspective of the data

    TEMPORAL : Data has associated daily values for each region
    GEOGRAPHICAL : Data has absoluted values for each region
    """
    TEMPORAL = 0
    GEOGRAPHICAL = 1