from __future__ import annotations

import json
import os
import warnings
from enum import Enum
from typing import Any

import httpx
from pydantic import BaseModel

from ena_api_handler._processing import (
    DEFAULT_FIELD_COERCIONS,
    apply_aliases,
    apply_coercions,
    apply_exclude,
    compute_raw_data_size,
)
from ena_api_handler.exceptions import (
    ENAAvailabilityError,
    ENAClientError,
    ENAQueryValidationError,
)
from ena_api_handler.query import (
    ENABaseQuery,
    ENAQueryClause,
    ENAQueryNot,
    ENAQueryPair,
    ENARawQuery,
)
from ena_api_handler.types import ENAAvailability, ENAPortalDataPortal

_DEFAULT = object()  # sentinel: "use DEFAULT_FIELD_COERCIONS"

_CONVENIENCE_PORTALS = (ENAPortalDataPortal.METAGENOME, ENAPortalDataPortal.ENA)


def _query_leaves(clause: ENAQueryClause) -> list[ENAQueryClause]:
    """Recursively walk a composed query tree and return its leaf clauses."""
    if isinstance(clause, ENAQueryPair):
        return _query_leaves(clause.left) + _query_leaves(clause.right)
    if isinstance(clause, ENAQueryNot):
        return _query_leaves(clause.clause)
    return [clause]


def _portal_types_match(
    portal: ENAPortalDataPortal,
    result: Enum,
    query: ENABaseQuery | ENAQueryClause,
    fields: list[Enum | str] | None,
) -> bool:
    """
    Check that any typed query leaves / fields belong to the (portal, result)
    pair's generated Query/Fields classes. Untyped inputs (ENARawQuery, plain
    string field names) are always considered compatible.
    """
    from ena_api_handler.models import FIELDS_MODELS, QUERY_MODELS  # noqa: PLC0415

    expected_query_cls = QUERY_MODELS.get((portal, result))
    if expected_query_cls is not None:
        for leaf in _query_leaves(query):
            if isinstance(leaf, ENABaseQuery) and not isinstance(
                leaf, expected_query_cls
            ):
                return False

    expected_fields_cls = FIELDS_MODELS.get((portal, result))
    if expected_fields_cls is not None and fields:
        for field in fields:
            if isinstance(field, Enum) and not isinstance(field, expected_fields_cls):
                return False

    return True


class ENAClient:
    """
    Client for the ENA Portal API search endpoint.

    Supports both synchronous and asynchronous search, with or without a
    context manager.

    Synchronous usage::

        client = ENAClient()
        results = client.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession="PRJEB1234"),
            fields=[ENAReadRunFields.RUN_ACCESSION],
        )
        client.close()

        # or as a context manager
        with ENAClient() as client:
            results = client.search(...)

    Asynchronous usage::

        client = ENAClient()
        results = await client.search_async(
            result=ENAPortalResultType.READ_RUN,
            query=ENAReadRunQuery(study_accession="PRJEB1234"),
            fields=[ENAReadRunFields.RUN_ACCESSION],
        )
        await client.aclose()

        # or as an async context manager
        async with ENAClient() as client:
            results = await client.search_async(...)

    Credentials can be passed explicitly or read from environment variables
    ``ENA_API_USER`` and ``ENA_API_PASSWORD`` (explicit args take precedence).
    """

    def __init__(
        self,
        url: str = "https://www.ebi.ac.uk/ena/portal/api/",
        username: str | None = None,
        password: str | None = None,
        timeout: float = 30.0,
        retries: int = 3,
    ) -> None:
        self._url = url
        _user = username or os.environ.get("ENA_API_USER")
        _pass = password or os.environ.get("ENA_API_PASSWORD")
        self._auth = httpx.BasicAuth(_user, _pass) if _user and _pass else None
        self._timeout = timeout
        self._retries = retries
        self._client: httpx.Client | None = None
        self._async_client: httpx.AsyncClient | None = None

    # ── Sync ──────────────────────────────────────────────────────────────────

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            transport = (
                httpx.HTTPTransport(retries=self._retries) if self._retries else None
            )
            self._client = httpx.Client(
                base_url=self._url,
                auth=self._auth,
                timeout=self._timeout,
                headers={"accept": "*/*"},
                transport=transport,
            )
        return self._client

    def close(self) -> None:
        """Close the underlying sync HTTP client."""
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> ENAClient:
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def search(
        self,
        result: Enum,
        query: ENABaseQuery | ENAQueryClause,
        fields: list[Enum | str] | None = None,
        portals: list[ENAPortalDataPortal] | tuple[ENAPortalDataPortal, ...] = (
            ENAPortalDataPortal.ENA,
        ),
        limit: int | None = None,
        include_metagenomes: bool = False,
        raise_on_empty: bool = False,
        field_coercions: dict | None = _DEFAULT,  # type: ignore[assignment]
        field_aliases: dict[str, str] | None = None,
        exclude: dict[str, Any] | None = None,
        auth: httpx.Auth | None = None,
    ) -> list[BaseModel]:
        """
        Search the ENA Portal API synchronously and return typed result objects.

        Parameters
        ----------
        result:
            The result type to search (e.g. ``ENAPortalResultType.READ_RUN``).
        query:
            A query model or composed clause.
        fields:
            List of ``ENAXxxFields`` enum members or field name strings to include.
            ``None`` omits the parameter and uses ENA's default field set.
        portals:
            Data portals to try in order (default: ``(ENAPortalDataPortal.ENA,)``).
            Returns the first non-empty result.
        limit:
            Maximum number of results. ``None`` omits the parameter (ENA default:
            10). ``0`` returns all results.
        include_metagenomes:
            Pass ``includeMetagenomes=true`` to the API.
        raise_on_empty:
            Raise ``ENAAvailabilityError`` if all portals return no results.
        field_coercions:
            Dict of ``{field: callable}`` applied to raw rows before model
            validation. Defaults to ``DEFAULT_FIELD_COERCIONS`` (coerces
            ``base_count`` and ``read_count`` to int). Pass ``None`` to disable.
        field_aliases:
            Dict of ``{original_field: new_name}`` applied before coercions.
        exclude:
            Dict of ``{field: value}``; rows matching any entry are dropped.
        auth:
            Optional per-request auth override. When provided, takes precedence
            over the client-level auth set at construction time.

        Returns
        -------
        list[BaseModel]
            Instances of the appropriate ``ENAXxxResult`` model.

        Raises
        ------
        ENAClientError
            If the API returns a non-2xx response.
        ENAQueryValidationError
            If ``fields``/``query`` don't match ``result`` for any portal in
            ``portals`` (typed inputs only; ``ENARawQuery``/plain strings are
            never rejected).
        ENAAvailabilityError
            If ``raise_on_empty=True`` and no results were found.
        """
        from ena_api_handler.models import RESULT_MODELS  # noqa: PLC0415

        coercions = (
            DEFAULT_FIELD_COERCIONS if field_coercions is _DEFAULT else field_coercions
        )

        attempted = False
        for portal in portals:
            if not _portal_types_match(portal, result, query, fields):
                continue
            attempted = True
            result_model = RESULT_MODELS.get((portal, result))
            params = self._build_params(
                result, query, fields, portal, limit, include_metagenomes
            )
            resp = self._get_client().get("search", params=params, auth=auth)
            rows = self._raw_rows(resp, result)
            if rows:
                rows = _pre_process(rows, field_aliases, exclude)
                if result_model is None:
                    return _coerce_dicts(rows, coercions)  # type: ignore[return-value]
                models = [result_model.model_validate(r) for r in rows]
                return _coerce_models(models, coercions)

        if not attempted:
            raise ENAQueryValidationError(
                f"fields/query do not match result type {result.value} for any portal"
                f" in {[p.value for p in portals]}"
            )
        if raise_on_empty:
            raise ENAAvailabilityError(
                f"No results for {result.value} across portals: {[p.value for p in portals]}"
            )
        return []

    # ── Async ─────────────────────────────────────────────────────────────────

    def _get_async_client(self) -> httpx.AsyncClient:
        if self._async_client is None:
            transport = (
                httpx.AsyncHTTPTransport(retries=self._retries)
                if self._retries
                else None
            )
            self._async_client = httpx.AsyncClient(
                base_url=self._url,
                auth=self._auth,
                timeout=self._timeout,
                headers={"accept": "*/*"},
                transport=transport,
            )
        return self._async_client

    async def aclose(self) -> None:
        """Close the underlying async HTTP client."""
        if self._async_client:
            await self._async_client.aclose()
            self._async_client = None

    async def __aenter__(self) -> ENAClient:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def search_async(
        self,
        result: Enum,
        query: ENABaseQuery | ENAQueryClause,
        fields: list[Enum | str] | None = None,
        portals: list[ENAPortalDataPortal] | tuple[ENAPortalDataPortal, ...] = (
            ENAPortalDataPortal.ENA,
        ),
        limit: int | None = None,
        include_metagenomes: bool = False,
        raise_on_empty: bool = False,
        field_coercions: dict | None = _DEFAULT,  # type: ignore[assignment]
        field_aliases: dict[str, str] | None = None,
        exclude: dict[str, Any] | None = None,
        auth: httpx.Auth | None = None,
    ) -> list[BaseModel]:
        """
        Search the ENA Portal API asynchronously and return typed result objects.

        Parameters
        ----------
        result:
            The result type to search (e.g. ``ENAPortalResultType.READ_RUN``).
        query:
            A query model or composed clause.
        fields:
            List of ``ENAXxxFields`` enum members or field name strings to include.
            ``None`` omits the parameter and uses ENA's default field set.
        portals:
            Data portals to try in order (default: ``(ENAPortalDataPortal.ENA,)``).
            Returns the first non-empty result.
        limit:
            Maximum number of results. ``None`` omits the parameter (ENA default:
            10). ``0`` returns all results.
        include_metagenomes:
            Pass ``includeMetagenomes=true`` to the API.
        raise_on_empty:
            Raise ``ENAAvailabilityError`` if all portals return no results.
        field_coercions:
            Dict of ``{field: callable}`` applied to raw rows before model
            validation. Defaults to ``DEFAULT_FIELD_COERCIONS``. Pass ``None`` to
            disable.
        field_aliases:
            Dict of ``{original_field: new_name}`` applied before coercions.
        exclude:
            Dict of ``{field: value}``; rows matching any entry are dropped.
        auth:
            Optional per-request auth override. When provided, takes precedence
            over the client-level auth set at construction time.

        Returns
        -------
        list[BaseModel]
            Instances of the appropriate ``ENAXxxResult`` model.

        Raises
        ------
        ENAClientError
            If the API returns a non-2xx response.
        ENAQueryValidationError
            If ``fields``/``query`` don't match ``result`` for any portal in
            ``portals`` (typed inputs only; ``ENARawQuery``/plain strings are
            never rejected).
        ENAAvailabilityError
            If ``raise_on_empty=True`` and no results were found.
        """
        from ena_api_handler.models import RESULT_MODELS  # noqa: PLC0415

        coercions = (
            DEFAULT_FIELD_COERCIONS if field_coercions is _DEFAULT else field_coercions
        )

        attempted = False
        for portal in portals:
            if not _portal_types_match(portal, result, query, fields):
                continue
            attempted = True
            result_model = RESULT_MODELS.get((portal, result))
            params = self._build_params(
                result, query, fields, portal, limit, include_metagenomes
            )
            resp = await self._get_async_client().get(
                "search", params=params, auth=auth
            )
            rows = self._raw_rows(resp, result)
            if rows:
                rows = _pre_process(rows, field_aliases, exclude)
                if result_model is None:
                    return _coerce_dicts(rows, coercions)  # type: ignore[return-value]
                models = [result_model.model_validate(r) for r in rows]
                return _coerce_models(models, coercions)

        if not attempted:
            raise ENAQueryValidationError(
                f"fields/query do not match result type {result.value} for any portal"
                f" in {[p.value for p in portals]}"
            )
        if raise_on_empty:
            raise ENAAvailabilityError(
                f"No results for {result.value} across portals: {[p.value for p in portals]}"
            )
        return []

    # ── Shared helpers ─────────────────────────────────────────────────────────

    def _build_params(
        self,
        result: Enum,
        query: ENABaseQuery | ENAQueryClause,
        fields: list[Enum | str] | None,
        portal: ENAPortalDataPortal,
        limit: int | None,
        include_metagenomes: bool,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "result": result.value,
            "query": query.to_query_string(),
            "format": "json",
            "dataPortal": portal.value,
        }
        if limit is not None:
            params["limit"] = limit
        if fields:
            params["fields"] = ",".join(
                f.value if isinstance(f, Enum) else f for f in fields
            )
        if include_metagenomes:
            params["includeMetagenomes"] = "true"
        return params

    def _raw_rows(self, resp: httpx.Response, result: Enum) -> list[dict]:
        if not resp.is_success:
            message = _extract_error(resp)
            raise ENAClientError(
                f"ENA API error {resp.status_code} for {result.value}: {message}"
            )
        if resp.status_code == 204:
            return []
        return resp.json()

    # ── Convenience methods (sync) ─────────────────────────────────────────────

    def get_study(
        self,
        primary_accession: str | None = None,
        secondary_accession: str | None = None,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """
        Fetch a study by accession, trying multiple result types in order.

        Tries READ_RUN, READ_STUDY, ANALYSIS_STUDY, then STUDY result types
        across METAGENOME and ENA portals, returning the first match.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        if not primary_accession and not secondary_accession:
            raise ValueError(
                "Either primary_accession or secondary_accession must be provided"
            )

        query_parts: list[ENAQueryClause] = []
        if primary_accession:
            query_parts.append(ENARawQuery(f'study_accession="{primary_accession}"'))
        if secondary_accession:
            query_parts.append(
                ENARawQuery(f'secondary_study_accession="{secondary_accession}"')
            )

        query: ENABaseQuery | ENAQueryClause = query_parts[0]
        for part in query_parts[1:]:
            query = query | part

        for result_type, aliases in [
            (ENAPortalResultType.READ_STUDY, None),
            (ENAPortalResultType.ANALYSIS_STUDY, None),
            (
                ENAPortalResultType.STUDY,
                {"study_description": "description", "study_name": "study_alias"},
            ),
        ]:
            rows = self.search(
                result=result_type,
                query=query,
                fields=fields,
                portals=_CONVENIENCE_PORTALS,
                field_aliases=aliases,
                limit=1,
            )
            if rows:
                return rows[0]
        return None

    def check_study_availability(
        self,
        primary_accession: str | None = None,
        secondary_accession: str | None = None,
        *,
        auth: httpx.Auth,
    ) -> ENAAvailability:
        """
        Determine whether a study is public, privately accessible with
        ``auth``, or unavailable under either.

        Tries READ_STUDY, ANALYSIS_STUDY, then STUDY result types across
        METAGENOME and ENA portals (same order as ``get_study()``), first
        unauthenticated, then with ``auth`` if nothing was found publicly.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        if not primary_accession and not secondary_accession:
            raise ValueError(
                "Either primary_accession or secondary_accession must be provided"
            )

        query_parts: list[ENAQueryClause] = []
        if primary_accession:
            query_parts.append(ENARawQuery(f'study_accession="{primary_accession}"'))
        if secondary_accession:
            query_parts.append(
                ENARawQuery(f'secondary_study_accession="{secondary_accession}"')
            )

        query: ENABaseQuery | ENAQueryClause = query_parts[0]
        for part in query_parts[1:]:
            query = query | part

        for probe_auth in (None, auth):
            for result_type in (
                ENAPortalResultType.READ_STUDY,
                ENAPortalResultType.ANALYSIS_STUDY,
                ENAPortalResultType.STUDY,
            ):
                rows = self.search(
                    result=result_type,
                    query=query,
                    portals=_CONVENIENCE_PORTALS,
                    limit=1,
                    auth=probe_auth,
                )
                if rows:
                    return (
                        ENAAvailability.PUBLIC
                        if probe_auth is None
                        else ENAAvailability.PRIVATE
                    )
        return ENAAvailability.SUPPRESSED

    def get_sample(
        self,
        sample_accession: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch a sample by accession or secondary accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f'sample_accession="{sample_accession}"') | ENARawQuery(
            f'secondary_sample_accession="{sample_accession}"'
        )
        rows = self.search(
            result=ENAPortalResultType.SAMPLE,
            query=query,
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            raise_on_empty=True,
            limit=1,
        )
        return rows[0] if rows else None

    def get_sample_studies(
        self,
        sample_accession: str,
        result: Enum | None = None,
    ) -> set[str]:
        """Fetch the set of secondary_study_accession values linked to a sample."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f'sample_accession="{sample_accession}"') | ENARawQuery(
            f'secondary_sample_accession="{sample_accession}"'
        )
        rows = self.search(
            result=result or ENAPortalResultType.READ_RUN,
            query=query,
            fields=["secondary_study_accession"],
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )
        return {
            r.secondary_study_accession
            for r in rows
            if getattr(r, "secondary_study_accession", None)
        }

    def get_run(
        self,
        run_accession: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch a single run by accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        rows = self.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENARawQuery(f'run_accession="{run_accession}"'),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=1,
        )
        rows = _attach_raw_data_size(rows)
        return rows[0] if rows else None

    def get_study_runs(
        self,
        study_accession: str,
        fields: list[Enum | str] | None = None,
        filter_assembly_runs: bool = True,
        filter_accessions: list[str] | None = None,
    ) -> list[BaseModel]:
        """
        Fetch all runs for a study.

        Parameters
        ----------
        filter_assembly_runs:
            Exclude runs with ``library_strategy="AMPLICON"`` (default: True).
        filter_accessions:
            If given, only return runs whose ``run_accession`` is in this list.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f'study_accession="{study_accession}"') | ENARawQuery(
            f'secondary_study_accession="{study_accession}"'
        )
        search_fields = list(fields) if fields is not None else None
        if (
            filter_assembly_runs
            and search_fields is not None
            and all(
                (f.value if isinstance(f, Enum) else f) != "library_strategy"
                for f in search_fields
            )
        ):
            search_fields.append("library_strategy")
        exclude = {"library_strategy": "AMPLICON"} if filter_assembly_runs else None
        rows = self.search(
            result=ENAPortalResultType.READ_RUN,
            query=query,
            fields=search_fields,
            portals=_CONVENIENCE_PORTALS,
            exclude=exclude,
            limit=0,
        )
        rows = _attach_raw_data_size(rows)
        if filter_accessions:
            rows = [
                r
                for r in rows
                if getattr(r, "run_accession", None) in filter_accessions
            ]
        return rows

    def get_study_assemblies(
        self,
        study_accession: str,
        fields: list[Enum | str] | None = None,
        filter_accessions: list[str] | None = None,
        allow_non_primary_assembly: bool = False,
    ) -> list[BaseModel]:
        """
        Fetch assemblies for a study.

        Parameters
        ----------
        filter_accessions:
            If given, only return assemblies whose ``analysis_accession`` is in
            this list.
        allow_non_primary_assembly:
            If False (default), restricts to ``assembly_type="primary metagenome"``.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query: ENABaseQuery | ENAQueryClause = ENARawQuery(
            f'study_accession="{study_accession}"'
        ) | ENARawQuery(f'secondary_study_accession="{study_accession}"')
        if not allow_non_primary_assembly:
            query = query & ENARawQuery('assembly_type="primary metagenome"')
        rows = self.search(
            result=ENAPortalResultType.ANALYSIS,
            query=query,
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )
        if filter_accessions:
            rows = [
                r
                for r in rows
                if getattr(r, "analysis_accession", None) in filter_accessions
            ]
        return rows

    def get_assembly(
        self,
        assembly_accession: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch a single assembly by analysis accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        rows = self.search(
            result=ENAPortalResultType.ANALYSIS,
            query=ENARawQuery(f'analysis_accession="{assembly_accession}"'),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=1,
        )
        return rows[0] if rows else None

    def get_assembly_from_sample(
        self,
        sample_name: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch an assembly by sample accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        rows = self.search(
            result=ENAPortalResultType.ANALYSIS,
            query=ENARawQuery(f'sample_accession="{sample_name}"'),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=1,
        )
        return rows[0] if rows else None

    def get_updated_studies(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch studies updated on or after ``cutoff_date`` (``YYYY-MM-DD``)."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        return self.search(
            result=ENAPortalResultType.STUDY,
            query=ENARawQuery(f"last_updated>={cutoff_date}"),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    def get_updated_runs(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch runs updated on or after ``cutoff_date`` (``YYYY-MM-DD``)."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        return self.search(
            result=ENAPortalResultType.READ_RUN,
            query=ENARawQuery(f"last_updated>={cutoff_date}"),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    def get_updated_assemblies(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch analyses updated on or after ``cutoff_date`` (``YYYY-MM-DD``)."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        return self.search(
            result=ENAPortalResultType.ANALYSIS,
            query=ENARawQuery(f"last_updated>={cutoff_date}"),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    def get_updated_tpa_assemblies(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch primary-metagenome assemblies updated on or after ``cutoff_date``."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f"last_updated>={cutoff_date}") & ENARawQuery(
            'assembly_type="primary metagenome"'
        )
        return self.search(
            result=ENAPortalResultType.ANALYSIS,
            query=query,
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    # ── Convenience methods (async) ────────────────────────────────────────────

    async def get_study_async(
        self,
        primary_accession: str | None = None,
        secondary_accession: str | None = None,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """
        Fetch a study by accession, trying multiple result types in order.

        Tries READ_RUN, READ_STUDY, ANALYSIS_STUDY, then STUDY result types
        across METAGENOME and ENA portals, returning the first match.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        if not primary_accession and not secondary_accession:
            raise ValueError(
                "Either primary_accession or secondary_accession must be provided"
            )

        query_parts: list[ENAQueryClause] = []
        if primary_accession:
            query_parts.append(ENARawQuery(f'study_accession="{primary_accession}"'))
        if secondary_accession:
            query_parts.append(
                ENARawQuery(f'secondary_study_accession="{secondary_accession}"')
            )

        query: ENABaseQuery | ENAQueryClause = query_parts[0]
        for part in query_parts[1:]:
            query = query | part

        for result_type, aliases in [
            (ENAPortalResultType.READ_STUDY, None),
            (ENAPortalResultType.ANALYSIS_STUDY, None),
            (
                ENAPortalResultType.STUDY,
                {"study_description": "description", "study_name": "study_alias"},
            ),
        ]:
            rows = await self.search_async(
                result=result_type,
                query=query,
                fields=fields,
                portals=_CONVENIENCE_PORTALS,
                field_aliases=aliases,
                limit=1,
            )
            if rows:
                return rows[0]
        return None

    async def check_study_availability_async(
        self,
        primary_accession: str | None = None,
        secondary_accession: str | None = None,
        *,
        auth: httpx.Auth,
    ) -> ENAAvailability:
        """
        Determine whether a study is public, privately accessible with
        ``auth``, or unavailable under either.

        Tries READ_STUDY, ANALYSIS_STUDY, then STUDY result types across
        METAGENOME and ENA portals (same order as ``get_study_async()``),
        first unauthenticated, then with ``auth`` if nothing was found
        publicly.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        if not primary_accession and not secondary_accession:
            raise ValueError(
                "Either primary_accession or secondary_accession must be provided"
            )

        query_parts: list[ENAQueryClause] = []
        if primary_accession:
            query_parts.append(ENARawQuery(f'study_accession="{primary_accession}"'))
        if secondary_accession:
            query_parts.append(
                ENARawQuery(f'secondary_study_accession="{secondary_accession}"')
            )

        query: ENABaseQuery | ENAQueryClause = query_parts[0]
        for part in query_parts[1:]:
            query = query | part

        for probe_auth in (None, auth):
            for result_type in (
                ENAPortalResultType.READ_STUDY,
                ENAPortalResultType.ANALYSIS_STUDY,
                ENAPortalResultType.STUDY,
            ):
                rows = await self.search_async(
                    result=result_type,
                    query=query,
                    portals=_CONVENIENCE_PORTALS,
                    limit=1,
                    auth=probe_auth,
                )
                if rows:
                    return (
                        ENAAvailability.PUBLIC
                        if probe_auth is None
                        else ENAAvailability.PRIVATE
                    )
        return ENAAvailability.SUPPRESSED

    async def get_sample_async(
        self,
        sample_accession: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch a sample by accession or secondary accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f'sample_accession="{sample_accession}"') | ENARawQuery(
            f'secondary_sample_accession="{sample_accession}"'
        )
        rows = await self.search_async(
            result=ENAPortalResultType.SAMPLE,
            query=query,
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            raise_on_empty=True,
            limit=1,
        )
        return rows[0] if rows else None

    async def get_sample_studies_async(
        self,
        sample_accession: str,
        result: Enum | None = None,
    ) -> set[str]:
        """Fetch the set of secondary_study_accession values linked to a sample."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f'sample_accession="{sample_accession}"') | ENARawQuery(
            f'secondary_sample_accession="{sample_accession}"'
        )
        rows = await self.search_async(
            result=result or ENAPortalResultType.READ_RUN,
            query=query,
            fields=["secondary_study_accession"],
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )
        return {
            r.secondary_study_accession
            for r in rows
            if getattr(r, "secondary_study_accession", None)
        }

    async def get_run_async(
        self,
        run_accession: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch a single run by accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        rows = await self.search_async(
            result=ENAPortalResultType.READ_RUN,
            query=ENARawQuery(f'run_accession="{run_accession}"'),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=1,
        )
        rows = _attach_raw_data_size(rows)
        return rows[0] if rows else None

    async def get_study_runs_async(
        self,
        study_accession: str,
        fields: list[Enum | str] | None = None,
        filter_assembly_runs: bool = True,
        filter_accessions: list[str] | None = None,
    ) -> list[BaseModel]:
        """
        Fetch all runs for a study.

        Parameters
        ----------
        filter_assembly_runs:
            Exclude runs with ``library_strategy="AMPLICON"`` (default: True).
        filter_accessions:
            If given, only return runs whose ``run_accession`` is in this list.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f'study_accession="{study_accession}"') | ENARawQuery(
            f'secondary_study_accession="{study_accession}"'
        )
        search_fields = list(fields) if fields is not None else None
        if (
            filter_assembly_runs
            and search_fields is not None
            and all(
                (f.value if isinstance(f, Enum) else f) != "library_strategy"
                for f in search_fields
            )
        ):
            search_fields.append("library_strategy")
        exclude = {"library_strategy": "AMPLICON"} if filter_assembly_runs else None
        rows = await self.search_async(
            result=ENAPortalResultType.READ_RUN,
            query=query,
            fields=search_fields,
            portals=_CONVENIENCE_PORTALS,
            exclude=exclude,
            limit=0,
        )
        rows = _attach_raw_data_size(rows)
        if filter_accessions:
            rows = [
                r
                for r in rows
                if getattr(r, "run_accession", None) in filter_accessions
            ]
        return rows

    async def get_study_assemblies_async(
        self,
        study_accession: str,
        fields: list[Enum | str] | None = None,
        filter_accessions: list[str] | None = None,
        allow_non_primary_assembly: bool = False,
    ) -> list[BaseModel]:
        """
        Fetch assemblies for a study.

        Parameters
        ----------
        filter_accessions:
            If given, only return assemblies whose ``analysis_accession`` is in
            this list.
        allow_non_primary_assembly:
            If False (default), restricts to ``assembly_type="primary metagenome"``.
        """
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query: ENABaseQuery | ENAQueryClause = ENARawQuery(
            f'study_accession="{study_accession}"'
        ) | ENARawQuery(f'secondary_study_accession="{study_accession}"')
        if not allow_non_primary_assembly:
            query = query & ENARawQuery('assembly_type="primary metagenome"')
        rows = await self.search_async(
            result=ENAPortalResultType.ANALYSIS,
            query=query,
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )
        if filter_accessions:
            rows = [
                r
                for r in rows
                if getattr(r, "analysis_accession", None) in filter_accessions
            ]
        return rows

    async def get_assembly_async(
        self,
        assembly_accession: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch a single assembly by analysis accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        rows = await self.search_async(
            result=ENAPortalResultType.ANALYSIS,
            query=ENARawQuery(f'analysis_accession="{assembly_accession}"'),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=1,
        )
        return rows[0] if rows else None

    async def get_assembly_from_sample_async(
        self,
        sample_name: str,
        fields: list[Enum | str] | None = None,
    ) -> BaseModel | None:
        """Fetch an assembly by sample accession."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        rows = await self.search_async(
            result=ENAPortalResultType.ANALYSIS,
            query=ENARawQuery(f'sample_accession="{sample_name}"'),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=1,
        )
        return rows[0] if rows else None

    async def get_updated_studies_async(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch studies updated on or after ``cutoff_date`` (``YYYY-MM-DD``)."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        return await self.search_async(
            result=ENAPortalResultType.STUDY,
            query=ENARawQuery(f"last_updated>={cutoff_date}"),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    async def get_updated_runs_async(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch runs updated on or after ``cutoff_date`` (``YYYY-MM-DD``)."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        return await self.search_async(
            result=ENAPortalResultType.READ_RUN,
            query=ENARawQuery(f"last_updated>={cutoff_date}"),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    async def get_updated_assemblies_async(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch analyses updated on or after ``cutoff_date`` (``YYYY-MM-DD``)."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        return await self.search_async(
            result=ENAPortalResultType.ANALYSIS,
            query=ENARawQuery(f"last_updated>={cutoff_date}"),
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    async def get_updated_tpa_assemblies_async(
        self,
        cutoff_date: str,
        fields: list[Enum | str] | None = None,
    ) -> list[BaseModel]:
        """Fetch primary-metagenome assemblies updated on or after ``cutoff_date``."""
        from ena_api_handler.models import ENAPortalResultType  # noqa: PLC0415

        query = ENARawQuery(f"last_updated>={cutoff_date}") & ENARawQuery(
            'assembly_type="primary metagenome"'
        )
        return await self.search_async(
            result=ENAPortalResultType.ANALYSIS,
            query=query,
            fields=fields,
            portals=_CONVENIENCE_PORTALS,
            limit=0,
        )

    def download_runs(self, runs: Any) -> None:
        warnings.warn(
            "download_runs() is removed. Download files directly via httpx.",
            DeprecationWarning,
            stacklevel=2,
        )
        raise NotImplementedError(
            "download_runs() has been removed. Download files directly via httpx."
        )


# ── Module-level helpers ───────────────────────────────────────────────────────


def _pre_process(
    rows: list[dict],
    aliases: dict[str, str] | None,
    exclude: dict[str, Any] | None,
) -> list[dict]:
    """Apply aliases and exclusion filter to raw dicts before model validation."""
    if aliases:
        rows = [apply_aliases(r, aliases) for r in rows]
    if exclude:
        rows = apply_exclude(rows, exclude)
    return rows


def _coerce_dicts(
    rows: list[dict],
    coercions: dict | None,
) -> list[dict]:
    """Apply coercions to raw dicts (used when no result model is available)."""
    if not coercions:
        return rows
    return [apply_coercions(r, coercions) for r in rows]


def _coerce_models(
    models: list[BaseModel],
    coercions: dict | None,
) -> list[BaseModel]:
    """Apply coercions to validated model instances via model_copy (no re-validation)."""
    if not coercions:
        return models
    result = []
    for model in models:
        updates: dict[str, Any] = {}
        for field, coerce in coercions.items():
            val = getattr(model, field, None)
            if val is not None:
                try:
                    updates[field] = coerce(val)
                except (ValueError, TypeError):
                    pass
        result.append(model.model_copy(update=updates) if updates else model)
    return result


def _attach_raw_data_size(rows: list[Any]) -> list[Any]:
    """Attach a computed raw_data_size field (sum of fastq_bytes/submitted_bytes)."""
    result: list[Any] = []
    for row in rows:
        size = compute_raw_data_size(row)
        if isinstance(row, BaseModel):
            result.append(row.model_copy(update={"raw_data_size": size}))
        else:
            result.append({**row, "raw_data_size": size})
    return result


def _extract_error(resp: httpx.Response) -> str:
    try:
        body = resp.json()
        if isinstance(body, dict):
            return body.get("message") or resp.text
    except (json.JSONDecodeError, ValueError):
        pass
    return resp.text or "no response body"
