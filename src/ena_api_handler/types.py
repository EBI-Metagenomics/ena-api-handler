from enum import Enum


class ENAPortalDataPortal(str, Enum):
    ENA = "ena"
    FAANG = "faang"
    METAGENOME = "metagenome"
    PATHOGEN = "pathogen"


class ENAAvailability(str, Enum):
    PUBLIC = "public"
    PRIVATE = "private"
    SUPPRESSED = "suppressed"
