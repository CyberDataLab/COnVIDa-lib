from enum import Enum

class TemporalGranularity(Enum):
    """
    Temporal granularities supported by the platform

    DAILY : Time series are reported by day
    """
    DAILY = 0