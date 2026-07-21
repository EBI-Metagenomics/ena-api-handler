"""Unit tests for ENAClient (sync and async search).

All HTTP calls are mocked — no network required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from pydantic import BaseModel

from ena_api_handler import ENAClient, ENAClientError
from ena_api_handler._processing import compute_raw_data_size
from ena_api_handler.client import ENAAvailabilityError
from ena_api_handler.query import ENABaseQuery
from ena_api_handler.types import ENAPortalDataPortal


# ── Helpers ───────────────────────────────────────────────────────────────────


class _Q(ENABaseQuery):
    """Minimal hand-written query for tests — avoids importing generated models."""

    study_accession: str | None = None


def _make_sync_mock(
    status_code: int,
    json_data: Any = None,
    text: str = "",
) -> MagicMock:
    """Return a mock httpx.Client whose .post() returns a canned response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.json.return_value = json_data if json_data is not None else []
    resp.text = text

    mock_client = MagicMock(spec=httpx.Client)
    mock_client.get.return_value = resp
    return mock_client


def _make_async_mock(
    status_code: int,
    json_data: Any = None,
    text: str = "",
) -> MagicMock:
    """Return a mock httpx.AsyncClient whose .post() is an AsyncMock."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.json.return_value = json_data if json_data is not None else []
    resp.text = text

    mock_client = MagicMock(spec=httpx.AsyncClient)
    mock_client.get = AsyncMock(return_value=resp)
    return mock_client


def _result_type():
    """Return ENAPortalResultType.READ_RUN without importing at module level."""
    from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

    return ENAPortalResultType.READ_RUN


# ── Sync tests ────────────────────────────────────────────────────────────────


def test_sync_search_success_returns_basemodel_instances() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(200, [{}])  # empty row; all fields Optional

    results = client.search(
        result=_result_type(),
        query=_Q(study_accession="PRJEB1234"),
        fields=[],
        portals=(ENAPortalDataPortal.ENA,),
    )

    assert len(results) == 1
    assert isinstance(results[0], BaseModel)


def test_sync_search_204_returns_empty_list() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(204)

    results = client.search(
        result=_result_type(),
        query=_Q(),
        fields=[],
    )

    assert results == []


def test_sync_search_http_error_raises_client_error() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(
        400, json_data={"message": "bad query"}, text="bad query"
    )

    with pytest.raises(ENAClientError, match="bad query"):
        client.search(result=_result_type(), query=_Q(), fields=[])


def test_sync_search_post_body_contains_required_fields() -> None:
    client = ENAClient()
    mock_http = _make_sync_mock(200, [{}])
    client._client = mock_http

    client.search(
        result=_result_type(),
        query=_Q(study_accession="PRJEB1234"),
        fields=[],
        portals=(ENAPortalDataPortal.ENA,),
        limit=10,
    )

    _, kwargs = mock_http.get.call_args
    params: dict = kwargs["params"]
    assert params["result"] == "read_run"
    assert params["format"] == "json"
    assert params["dataPortal"] == "ena"
    assert params["limit"] == 10
    assert 'study_accession="PRJEB1234"' in params["query"]


def test_sync_search_include_metagenomes_flag() -> None:
    client = ENAClient()
    mock_http = _make_sync_mock(200, [{}])
    client._client = mock_http

    # Without flag
    client.search(result=_result_type(), query=_Q(), fields=[])
    _, kwargs = mock_http.get.call_args
    assert "includeMetagenomes" not in kwargs["params"]

    # With flag
    client.search(
        result=_result_type(), query=_Q(), fields=[], include_metagenomes=True
    )
    _, kwargs = mock_http.get.call_args
    assert kwargs["params"]["includeMetagenomes"] == "true"


def test_sync_search_no_context_manager() -> None:
    """Client works fine without a `with` block; close() cleans up."""
    client = ENAClient()
    client._client = _make_sync_mock(200, [{}])

    results = client.search(result=_result_type(), query=_Q(), fields=[])
    client.close()

    assert len(results) == 1
    assert client._client is None  # cleaned up


def test_sync_context_manager_closes_on_exit() -> None:
    with ENAClient() as client:
        client._client = _make_sync_mock(200, [{}])
        results = client.search(result=_result_type(), query=_Q(), fields=[])

    assert len(results) == 1
    assert client._client is None


def test_get_study_supports_secondary_accession_only() -> None:
    client = ENAClient()
    calls: list[dict[str, Any]] = []

    def fake_search(**kwargs: Any) -> list[dict[str, str]]:
        calls.append(kwargs)
        return [{"study_accession": "PRJEB1234"}]

    client.search = fake_search  # type: ignore[assignment]

    result = client.get_study(secondary_accession="ERP1234")

    assert result == {"study_accession": "PRJEB1234"}
    assert len(calls) == 1
    assert calls[0]["query"].to_query_string() == 'secondary_study_accession="ERP1234"'


def test_get_study_prefers_study_level_results_over_runs() -> None:
    client = ENAClient()
    calls: list[Any] = []

    def fake_search(**kwargs: Any) -> list[dict[str, str]]:
        calls.append(kwargs["result"])
        if kwargs["result"].value == "read_study":
            return [{"study_accession": "PRJEB1234"}]
        return [{"run_accession": "ERR1234"}]

    client.search = fake_search  # type: ignore[assignment]

    result = client.get_study(primary_accession="PRJEB1234")

    assert result == {"study_accession": "PRJEB1234"}
    assert calls[0].value == "read_study"


def test_get_study_runs_adds_library_strategy_when_filtering() -> None:
    client = ENAClient()

    def fake_search(**kwargs: Any) -> list[dict[str, str]]:
        field_values = [
            field.value if hasattr(field, "value") else field
            for field in (kwargs["fields"] or [])
        ]
        assert "run_accession" in field_values
        assert "library_strategy" in field_values
        return [{"run_accession": "ERR1234"}]

    client.search = fake_search  # type: ignore[assignment]

    result = client.get_study_runs("PRJEB1234", fields=["run_accession"])

    assert result == [{"run_accession": "ERR1234", "raw_data_size": None}]


def test_get_sample_studies_queries_both_accession_fields_and_defaults_to_read_run() -> (
    None
):
    client = ENAClient()
    mock_http = _make_sync_mock(
        200,
        [
            {"secondary_study_accession": "ERP1234"},
            {"secondary_study_accession": "ERP5678"},
            {},  # rows missing the field are dropped
        ],
    )
    client._client = mock_http

    result = client.get_sample_studies("SAMN11835464")

    assert result == {"ERP1234", "ERP5678"}
    _, kwargs = mock_http.get.call_args
    params: dict = kwargs["params"]
    assert params["result"] == "read_run"
    assert params["fields"] == "secondary_study_accession"
    assert 'sample_accession="SAMN11835464"' in params["query"]
    assert 'secondary_sample_accession="SAMN11835464"' in params["query"]


def test_get_sample_studies_accepts_explicit_result_type() -> None:
    client = ENAClient()
    calls: list[dict[str, Any]] = []

    def fake_search(**kwargs: Any) -> list[dict[str, str]]:
        calls.append(kwargs)
        return []

    client.search = fake_search  # type: ignore[assignment]

    from ena_api_handler.models import ENAPortalResultType

    client.get_sample_studies("SAMN11835464", result=ENAPortalResultType.ANALYSIS)

    assert calls[0]["result"] is ENAPortalResultType.ANALYSIS


# ── Async tests ───────────────────────────────────────────────────────────────


async def test_search_async_success_returns_basemodel_instances() -> None:
    client = ENAClient()
    client._async_client = _make_async_mock(200, [{}])

    results = await client.search_async(
        result=_result_type(),
        query=_Q(study_accession="PRJEB1234"),
        fields=[],
        portals=(ENAPortalDataPortal.ENA,),
    )

    assert len(results) == 1
    assert isinstance(results[0], BaseModel)


async def test_search_async_204_returns_empty_list() -> None:
    client = ENAClient()
    client._async_client = _make_async_mock(204)

    results = await client.search_async(result=_result_type(), query=_Q(), fields=[])

    assert results == []


async def test_search_async_http_error_raises_client_error() -> None:
    client = ENAClient()
    client._async_client = _make_async_mock(
        400, json_data={"message": "bad query"}, text="bad query"
    )

    with pytest.raises(ENAClientError, match="bad query"):
        await client.search_async(result=_result_type(), query=_Q(), fields=[])


async def test_search_async_post_body_contains_required_fields() -> None:
    client = ENAClient()
    mock_http = _make_async_mock(200, [{}])
    client._async_client = mock_http

    await client.search_async(
        result=_result_type(),
        query=_Q(study_accession="PRJEB1234"),
        fields=[],
        portals=(ENAPortalDataPortal.ENA,),
        limit=5,
    )

    _, kwargs = mock_http.get.call_args
    params: dict = kwargs["params"]
    assert params["result"] == "read_run"
    assert params["format"] == "json"
    assert params["dataPortal"] == "ena"
    assert params["limit"] == 5
    assert 'study_accession="PRJEB1234"' in params["query"]


async def test_search_async_include_metagenomes_flag() -> None:
    client = ENAClient()
    mock_http = _make_async_mock(200, [{}])
    client._async_client = mock_http

    # Without flag
    await client.search_async(result=_result_type(), query=_Q(), fields=[])
    _, kwargs = mock_http.get.call_args
    assert "includeMetagenomes" not in kwargs["params"]

    # With flag
    await client.search_async(
        result=_result_type(), query=_Q(), fields=[], include_metagenomes=True
    )
    _, kwargs = mock_http.get.call_args
    assert kwargs["params"]["includeMetagenomes"] == "true"


async def test_get_study_async_supports_secondary_accession_only() -> None:
    client = ENAClient()
    calls: list[dict[str, Any]] = []

    async def fake_search_async(**kwargs: Any) -> list[dict[str, str]]:
        calls.append(kwargs)
        return [{"study_accession": "PRJEB1234"}]

    client.search_async = fake_search_async  # type: ignore[assignment]

    result = await client.get_study_async(secondary_accession="ERP1234")

    assert result == {"study_accession": "PRJEB1234"}
    assert len(calls) == 1
    assert calls[0]["query"].to_query_string() == 'secondary_study_accession="ERP1234"'


async def test_get_study_runs_async_adds_library_strategy_when_filtering() -> None:
    client = ENAClient()

    async def fake_search_async(**kwargs: Any) -> list[dict[str, str]]:
        field_values = [
            field.value if hasattr(field, "value") else field
            for field in (kwargs["fields"] or [])
        ]
        assert "run_accession" in field_values
        assert "library_strategy" in field_values
        return [{"run_accession": "ERR1234"}]

    client.search_async = fake_search_async  # type: ignore[assignment]

    result = await client.get_study_runs_async("PRJEB1234", fields=["run_accession"])

    assert result == [{"run_accession": "ERR1234", "raw_data_size": None}]


async def test_get_sample_studies_async_queries_both_accession_fields() -> None:
    client = ENAClient()
    mock_http = _make_async_mock(200, [{"secondary_study_accession": "ERP1234"}])
    client._async_client = mock_http

    result = await client.get_sample_studies_async("SAMN11835464")

    assert result == {"ERP1234"}
    _, kwargs = mock_http.get.call_args
    params: dict = kwargs["params"]
    assert params["result"] == "read_run"
    assert params["fields"] == "secondary_study_accession"
    assert 'sample_accession="SAMN11835464"' in params["query"]
    assert 'secondary_sample_accession="SAMN11835464"' in params["query"]


async def test_get_run_async_attaches_raw_data_size() -> None:
    client = ENAClient()
    client._async_client = _make_async_mock(
        200, [{"run_accession": "ERR1234", "fastq_bytes": "100;200"}]
    )

    run = await client.get_run_async("ERR1234")

    assert run is not None
    assert run.raw_data_size == 300  # type: ignore[union-attr]


async def test_get_study_runs_async_attaches_raw_data_size_to_every_row() -> None:
    client = ENAClient()
    client._async_client = _make_async_mock(
        200,
        [
            {"run_accession": "ERR1", "fastq_bytes": "100"},
            {"run_accession": "ERR2", "submitted_bytes": "50;50"},
        ],
    )

    runs = await client.get_study_runs_async("PRJEB1234", filter_assembly_runs=False)

    assert [r.raw_data_size for r in runs] == [100, 100]  # type: ignore[union-attr]


async def test_search_async_no_context_manager() -> None:
    """Client works fine without `async with`; aclose() cleans up."""
    client = ENAClient()
    client._async_client = _make_async_mock(200, [{}])

    results = await client.search_async(result=_result_type(), query=_Q(), fields=[])
    await client.aclose()

    assert len(results) == 1
    assert client._async_client is None


async def test_async_context_manager_closes_on_exit() -> None:
    async with ENAClient() as client:
        client._async_client = _make_async_mock(200, [{}])
        results = await client.search_async(
            result=_result_type(), query=_Q(), fields=[]
        )

    assert len(results) == 1
    assert client._async_client is None


# ── Auth ──────────────────────────────────────────────────────────────────────


def test_auth_sets_basic_auth() -> None:
    client = ENAClient(username="user", password="pass")
    assert isinstance(client._auth, httpx.BasicAuth)


def test_no_auth_by_default() -> None:
    client = ENAClient()
    assert client._auth is None


def test_auth_from_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENA_API_USER", "envuser")
    monkeypatch.setenv("ENA_API_PASSWORD", "envpass")
    client = ENAClient()
    assert isinstance(client._auth, httpx.BasicAuth)


def test_explicit_auth_overrides_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENA_API_USER", "envuser")
    monkeypatch.setenv("ENA_API_PASSWORD", "envpass")
    client = ENAClient(username="explicit", password="explicit")
    # Both result in BasicAuth; just confirm it was created
    assert isinstance(client._auth, httpx.BasicAuth)


def test_per_request_auth_is_forwarded() -> None:
    """auth= passed to search() reaches the underlying HTTP call."""
    client = ENAClient()
    mock_http = _make_sync_mock(200, [{}])
    client._client = mock_http

    per_request_auth = httpx.BasicAuth("req_user", "req_pass")
    client.search(
        result=_result_type(),
        query=_Q(),
        fields=[],
        auth=per_request_auth,
    )

    _, kwargs = mock_http.get.call_args
    assert kwargs["auth"] is per_request_auth


# ── Portal fallback ────────────────────────────────────────────────────────────


def test_portal_fallback_returns_first_nonempty() -> None:
    """First portal returns empty; second portal returns a row."""
    client = ENAClient()

    empty_resp = MagicMock(spec=httpx.Response)
    empty_resp.status_code = 204
    empty_resp.is_success = True

    hit_resp = MagicMock(spec=httpx.Response)
    hit_resp.status_code = 200
    hit_resp.is_success = True
    hit_resp.json.return_value = [{}]

    mock_http = MagicMock(spec=httpx.Client)
    mock_http.get.side_effect = [empty_resp, hit_resp]
    client._client = mock_http

    results = client.search(
        result=_result_type(),
        query=_Q(),
        portals=(ENAPortalDataPortal.METAGENOME, ENAPortalDataPortal.ENA),
    )

    assert len(results) == 1
    assert mock_http.get.call_count == 2


def test_raise_on_empty_raises_availability_error() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(204)

    with pytest.raises(ENAAvailabilityError):
        client.search(
            result=_result_type(),
            query=_Q(),
            raise_on_empty=True,
        )


# ── Post-processing ────────────────────────────────────────────────────────────


def test_field_coercions_applied() -> None:
    """base_count and read_count are coerced to int by default."""
    client = ENAClient()
    client._client = _make_sync_mock(200, [{"base_count": "12345", "read_count": "99"}])

    results = client.search(result=_result_type(), query=_Q())
    row = results[0]
    assert row.base_count == 12345  # type: ignore[attr-defined]
    assert row.read_count == 99  # type: ignore[attr-defined]


def test_field_coercions_disabled_with_none() -> None:
    """Passing field_coercions=None skips coercions."""
    client = ENAClient()
    client._client = _make_sync_mock(200, [{"base_count": "12345"}])

    results = client.search(result=_result_type(), query=_Q(), field_coercions=None)
    row = results[0]
    assert row.base_count == "12345"  # type: ignore[attr-defined]


def test_field_aliases_applied() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(200, [{"old_name": "value"}])

    results = client.search(
        result=_result_type(),
        query=_Q(),
        field_aliases={"old_name": "new_name"},
        field_coercions=None,
    )
    row = results[0]
    assert row.new_name == "value"  # type: ignore[attr-defined]
    assert not hasattr(row, "old_name")


def test_exclude_filters_rows() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(
        200,
        [
            {"library_strategy": "AMPLICON"},
            {"library_strategy": "WGS"},
        ],
    )

    results = client.search(
        result=_result_type(),
        query=_Q(),
        exclude={"library_strategy": "AMPLICON"},
        field_coercions=None,
    )
    assert len(results) == 1
    assert results[0].library_strategy == "WGS"  # type: ignore[attr-defined]


def test_compute_raw_data_size_sums_fastq_bytes() -> None:
    assert compute_raw_data_size({"fastq_bytes": "100;200"}) == 300


def test_compute_raw_data_size_falls_back_to_submitted_bytes() -> None:
    assert compute_raw_data_size({"submitted_bytes": "10;20;30"}) == 60


def test_compute_raw_data_size_prefers_fastq_bytes_over_submitted_bytes() -> None:
    row = {"fastq_bytes": "100", "submitted_bytes": "999"}
    assert compute_raw_data_size(row) == 100


def test_compute_raw_data_size_none_when_neither_present() -> None:
    assert compute_raw_data_size({}) is None


def test_compute_raw_data_size_none_when_fastq_bytes_empty() -> None:
    assert compute_raw_data_size({"fastq_bytes": "", "submitted_bytes": "50"}) == 50


def test_get_run_attaches_raw_data_size() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(
        200, [{"run_accession": "ERR1234", "fastq_bytes": "100;200"}]
    )

    run = client.get_run("ERR1234")

    assert run is not None
    assert run.raw_data_size == 300  # type: ignore[union-attr]


def test_get_run_raw_data_size_none_when_bytes_fields_absent() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(200, [{"run_accession": "ERR1234"}])

    run = client.get_run("ERR1234")

    assert run is not None
    assert run.raw_data_size is None  # type: ignore[union-attr]


def test_get_study_runs_attaches_raw_data_size_to_every_row() -> None:
    client = ENAClient()
    client._client = _make_sync_mock(
        200,
        [
            {"run_accession": "ERR1", "fastq_bytes": "100"},
            {"run_accession": "ERR2", "submitted_bytes": "50;50"},
        ],
    )

    runs = client.get_study_runs("PRJEB1234", filter_assembly_runs=False)

    assert [r.raw_data_size for r in runs] == [100, 100]  # type: ignore[union-attr]


# ── download_runs stub ─────────────────────────────────────────────────────────


def test_download_runs_raises_not_implemented() -> None:
    client = ENAClient()
    with pytest.warns(DeprecationWarning):
        with pytest.raises(NotImplementedError):
            client.download_runs([])
