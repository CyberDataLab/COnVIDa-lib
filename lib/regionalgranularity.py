from enum import Enum

class RegionalGranularity(Enum):
    """
    Regional granularities supported by the platform

    COMMUNITY : Data series are reported by Spanish communities
    PROVINCE : Data series are reported by Spanish provinces
    """
    COMMUNITY = 0
    PROVINCE = 1