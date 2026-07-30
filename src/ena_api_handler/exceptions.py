class ENAClientError(Exception):
    """Raised when the ENA Portal API returns an error response."""


class ENAAvailabilityError(ENAClientError):
    """Raised when all portals returned empty results and raise_on_empty=True."""


class ENAQueryValidationError(ENAClientError):
    """Raised when fields/query don't match result type for any queried portal."""
