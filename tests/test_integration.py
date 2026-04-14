"""Integration tests for ena-api-handler.

Split into two sections:

1. Structural tests (no network) — import the generated models and verify
   they have the correct shape.  These run on every `pytest` invocation.

2. Live API tests (network) — call the real ENA Portal API.  These are
   marked `@pytest.mark.integration` and skipped by default.
   Run them with:  pytest -m integration
"""

from __future__ import annotations

import importlib

import pytest
from pydantic import BaseModel

from ena_api_handler import ENAClient
from ena_api_handler.models import RESULT_MODELS, ENAPortalResultType
from ena_api_handler.query import ENABaseQuery
from ena_api_handler.types import ENAPortalDataPortal

# ── Helpers ───────────────────────────────────────────────────────────────────

ALL_PORTALS = {
    ENAPortalDataPortal.ENA,
    ENAPortalDataPortal.FAANG,
    ENAPortalDataPortal.METAGENOME,
    ENAPortalDataPortal.PATHOGEN,
}


def _companion_classes(result_cls: type[BaseModel]) -> tuple[type, type]:
    """
    Derive the Fields and Query classes from a Result class.

    ENAReadRunResult  →  (ENAReadRunFields, ENAReadRunQuery)
    """
    base_name = result_cls.__name__.removesuffix("Result")
    module = importlib.import_module(result_cls.__module__)
    fields_cls = getattr(module, f"{base_name}Fields")
    query_cls = getattr(module, f"{base_name}Query")
    return fields_cls, query_cls


# ── Structural tests ──────────────────────────────────────────────────────────


def test_result_models_is_populated() -> None:
    assert len(RESULT_MODELS) > 0


def test_all_portals_represented() -> None:
    portals_in_registry = {portal for portal, _ in RESULT_MODELS}
    assert portals_in_registry == ALL_PORTALS


def test_all_result_types_represented() -> None:
    result_types_in_registry = {rt for _, rt in RESULT_MODELS}
    all_result_types = set(ENAPortalResultType)
    assert result_types_in_registry == all_result_types


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_result_model_is_basemodel_subclass(key: tuple) -> None:
    result_cls = RESULT_MODELS[key]
    assert issubclass(result_cls, BaseModel)


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_result_model_allows_extra_fields(key: tuple) -> None:
    result_cls = RESULT_MODELS[key]
    assert result_cls.model_config.get("extra") == "allow"


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_query_model_is_enabasequery_subclass(key: tuple) -> None:
    result_cls = RESULT_MODELS[key]
    _, query_cls = _companion_classes(result_cls)
    assert issubclass(query_cls, ENABaseQuery)


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_fields_enum_is_str_and_enum(key: tuple) -> None:
    from enum import Enum  # noqa: PLC0415

    result_cls = RESULT_MODELS[key]
    fields_cls, _ = _companion_classes(result_cls)
    assert issubclass(fields_cls, str)
    assert issubclass(fields_cls, Enum)


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_fields_enum_is_nonempty(key: tuple) -> None:
    result_cls = RESULT_MODELS[key]
    fields_cls, _ = _companion_classes(result_cls)
    assert len(list(fields_cls)) > 0


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_result_model_instantiates_with_no_args(key: tuple) -> None:
    result_cls = RESULT_MODELS[key]
    instance = result_cls()
    assert isinstance(instance, BaseModel)


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_query_model_instantiates_with_no_args(key: tuple) -> None:
    result_cls = RESULT_MODELS[key]
    _, query_cls = _companion_classes(result_cls)
    instance = query_cls()
    assert isinstance(instance, ENABaseQuery)


@pytest.mark.parametrize("key", list(RESULT_MODELS.keys()))
def test_query_to_query_string_single_field(key: tuple) -> None:
    """Setting one field on a Query model produces the expected query string."""
    result_cls = RESULT_MODELS[key]
    _, query_cls = _companion_classes(result_cls)

    first_field = next(iter(query_cls.model_fields))
    instance = query_cls(**{first_field: "test_value"})
    qs = instance.to_query_string()

    assert f'{first_field}="test_value"' in qs


# ── Live API tests ────────────────────────────────────────────────────────────

# Study PRJEB1787 is a stable metagenomics study on ENA used as a test fixture.
_LIVE_STUDY = "PRJEB1787"


@pytest.mark.integration
def test_live_sync_search_returns_results() -> None:
    from ena_api_handler.models.ena.read_run import ENAReadRunFields, ENAReadRunQuery  # noqa: PLC0415

    client = ENAClient()
    results = client.search(
        result=ENAPortalResultType.READ_RUN,
        query=ENAReadRunQuery(study_accession=_LIVE_STUDY),
        fields=[ENAReadRunFields.RUN_ACCESSION],
        portals=(ENAPortalDataPortal.ENA,),
        limit=5,
    )
    client.close()

    assert len(results) > 0


@pytest.mark.integration
def test_live_sync_search_results_are_typed() -> None:
    from ena_api_handler.models.ena.read_run import ENAReadRunFields, ENAReadRunQuery  # noqa: PLC0415

    with ENAClient() as client:
        results = client.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession=_LIVE_STUDY),
            fields=[ENAReadRunFields.RUN_ACCESSION],
            portals=(ENAPortalDataPortal.ENA,),
            limit=5,
        )

    assert all(isinstance(r, BaseModel) for r in results)


@pytest.mark.integration
async def test_live_search_async_returns_results() -> None:
    from ena_api_handler.models.ena.read_run import ENAReadRunFields, ENAReadRunQuery  # noqa: PLC0415

    async with ENAClient() as client:
        results = await client.search_async(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession=_LIVE_STUDY),
            fields=[ENAReadRunFields.RUN_ACCESSION],
            portals=(ENAPortalDataPortal.ENA,),
            limit=5,
        )

    assert len(results) > 0
    assert all(isinstance(r, BaseModel) for r in results)


@pytest.mark.integration
def test_live_field_filtering_is_respected() -> None:
    """Requesting only run_accession should not return other fields like fastq_ftp."""
    from ena_api_handler.models.ena.read_run import ENAReadRunFields, ENAReadRunQuery  # noqa: PLC0415

    with ENAClient() as client:
        results = client.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession=_LIVE_STUDY),
            fields=[ENAReadRunFields.RUN_ACCESSION],
            portals=(ENAPortalDataPortal.ENA,),
            limit=3,
            field_coercions=None,
        )

    assert len(results) > 0
    row = results[0]
    assert row.run_accession is not None  # type: ignore[attr-defined]
    assert row.fastq_ftp is None, (  # type: ignore[attr-defined]
        "fastq_ftp was populated even though it was not requested — "
        "fields parameter may not be reaching the API correctly"
    )
