"""The exceptions the client and its typed models can raise: ENAClientError,
ENAQueryValidationError, ENAAvailabilityError, and a plain Pydantic
ValidationError from constructing a typed model with a bad value.

Run with: uv run python examples/07_error_handling.py
"""

from pydantic import ValidationError

from ena_api_handler import (
    ENAAvailabilityError,
    ENAClient,
    ENAClientError,
    ENAQueryValidationError,
)
from ena_api_handler.models import ENAPortalResultType
from ena_api_handler.models.ena import ENAReadRunQuery, ENAStudyQuery

# Constructing a typed query/result model with the wrong type for a field
# raises Pydantic's own ValidationError, before any network call is made.
try:
    ENAReadRunQuery(study_accession=["not", "a", "string"])  # type: ignore[arg-type]
except ValidationError as exc:
    print(f"pydantic validation error as expected: {exc.error_count()} error(s)")

with ENAClient() as client:
    # ENAQueryValidationError: typed query/fields don't match the result type
    # for any portal in `portals=`.
    try:
        client.search(
            result=ENAPortalResultType.STUDY,
            query=ENAReadRunQuery(study_accession="PRJEB1787"),
        )
    except ENAQueryValidationError as exc:
        print(f"query validation error: {exc}")

    # ENAAvailabilityError: raise_on_empty=True and no portal returned rows.
    try:
        client.search(
            result=ENAPortalResultType.STUDY,
            query=ENAStudyQuery(study_accession="NOT_A_REAL_ACCESSION"),
            raise_on_empty=True,
        )
    except ENAAvailabilityError:
        print("no results found in any requested portal")

    # ENAClientError: base class for API/HTTP errors (also the base class for
    # both exceptions above).
    try:
        client.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession="PRJEB1787"),
        )
    except ENAClientError as exc:
        print(f"ENA request failed: {exc}")
