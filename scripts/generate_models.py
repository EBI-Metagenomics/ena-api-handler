#!/usr/bin/env python3
"""
Fetch ENA Portal API field metadata and generate typed Pydantic models.

Two phases:
  1. Fetch  — hit /results, /searchFields, /returnFields and update snapshots/
  2. Generate — read snapshots and write src/ena_api_handler/models/

Usage:
    uv run scripts/generate_models.py [OPTIONS]

Options:
    --dry-run           Print what would be written; write nothing
    --ignore-snapshots  Always overwrite snapshots (skip diff comparison)
    --skip-fetch        Skip API calls; only regenerate from existing snapshots
    --portals P1,P2     Portals to include (default: ena,faang,metagenome,pathogen)
    --results R1,R2     Result types to include (default: all)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path

import httpx

# ── Constants ─────────────────────────────────────────────────────────────────

BASE_URL = "https://www.ebi.ac.uk/ena/portal/api/"
ALL_PORTALS = ["ena", "faang", "metagenome", "pathogen"]
FIELD_ENDPOINTS = ["searchFields", "returnFields"]
MAX_CONCURRENT = 10

REPO_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOTS_DIR = REPO_ROOT / "snapshots"
MODELS_DIR = REPO_ROOT / "src" / "ena_api_handler" / "models"
CLIENT_STUB_PATH = REPO_ROOT / "src" / "ena_api_handler" / "client.pyi"

# Portals tried, in order, by ENAClient's convenience methods (get_run, get_study, ...).
# Must match _CONVENIENCE_PORTALS in src/ena_api_handler/client.py.
CONVENIENCE_PORTALS = ["metagenome", "ena"]

# Portal → Python class name prefix
PORTAL_PREFIX: dict[str, str] = {
    "ena": "ENA",
    "faang": "FAANG",
    "metagenome": "Metagenome",
    "pathogen": "Pathogen",
}

# ENA field type → Python type annotation
ENA_TYPE_MAP: dict[str, str] = {
    "text": "str",
    "numeric": "float",
    "date": "str",
    "boolean": "bool",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ── Naming helpers ─────────────────────────────────────────────────────────────


def result_to_class_segment(result_type: str) -> str:
    """'read_run' → 'ReadRun', 'tls_set' → 'TlsSet'"""
    return "".join(word.capitalize() for word in result_type.split("_"))


def class_prefix(portal: str) -> str:
    return PORTAL_PREFIX.get(portal, portal.capitalize())


def py_type(ena_type: str) -> str:
    return ENA_TYPE_MAP.get(ena_type.lower(), "str")


# ── Fetch phase ───────────────────────────────────────────────────────────────


async def fetch_result_types(client: httpx.AsyncClient) -> list[str]:
    resp = await client.get("results", params={"format": "json"})
    resp.raise_for_status()
    return sorted({entry["resultId"] for entry in resp.json() if "resultId" in entry})


async def fetch_fields(
    client: httpx.AsyncClient,
    endpoint: str,
    result: str,
    portal: str,
    sem: asyncio.Semaphore,
) -> list[dict] | None:
    """Return list of {columnId, description, type} dicts, sorted by columnId."""
    async with sem:
        try:
            resp = await client.get(
                endpoint,
                params={"result": result, "dataPortal": portal, "format": "json"},
            )
            if resp.status_code == 204:
                return []
            resp.raise_for_status()
            normalized = []
            for entry in resp.json():
                if "columnId" not in entry:
                    continue
                normalized.append(
                    {
                        "columnId": entry["columnId"],
                        "description": (
                            entry.get("description") or entry.get("label") or ""
                        ),
                        "type": entry.get("type") or entry.get("fieldType") or "text",
                    }
                )
            return sorted(normalized, key=lambda x: x["columnId"])
        except httpx.HTTPStatusError as exc:
            log.warning(
                "HTTP %s — skipping %s / %s / %s",
                exc.response.status_code,
                endpoint,
                result,
                portal,
            )
            return None
        except Exception as exc:
            log.error("Error fetching %s / %s / %s: %s", endpoint, result, portal, exc)
            return None


def load_snapshot(path: Path) -> list[dict] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Handle legacy format (list of strings)
        if data and isinstance(data[0], str):
            return [{"columnId": c, "description": "", "type": "text"} for c in data]
        return data
    except Exception:
        return None


def write_snapshot(path: Path, fields: list[dict], dry_run: bool) -> bool:
    """Write snapshot; return True if content changed."""
    content = json.dumps(fields, indent=2) + "\n"
    if dry_run:
        existing = path.read_text(encoding="utf-8") if path.exists() else None
        return existing != content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return True


async def run_fetch(
    portals: list[str],
    result_filter: list[str] | None,
    ignore_snapshots: bool,
    dry_run: bool,
) -> tuple[list[str], set[tuple[str, str]]]:
    """
    Fetch snapshots from ENA API.
    Returns (result_types, changed_pairs) where changed_pairs is
    a set of (portal, result_type) that had any snapshot change.
    """
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    changed: set[tuple[str, str]] = set()

    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=30, headers={"accept": "*/*"}
    ) as client:
        log.info("Fetching result types...")
        result_types = await fetch_result_types(client)
        if result_filter:
            result_types = [r for r in result_types if r in result_filter]
        log.info("Using %d result types", len(result_types))

        async def process(portal: str, result: str, endpoint: str) -> None:
            fields = await fetch_fields(client, endpoint, result, portal, sem)
            if fields is None:
                return

            path = SNAPSHOTS_DIR / portal / result / f"{endpoint}.json"
            existing = load_snapshot(path)

            if not ignore_snapshots and existing is not None:
                existing_ids = {f["columnId"] for f in existing}
                new_ids = {f["columnId"] for f in fields}
                added = new_ids - existing_ids
                removed = existing_ids - new_ids
                if not added and not removed:
                    return  # no change

            changed.add((portal, result))
            changed_flag = write_snapshot(path, fields, dry_run)
            if changed_flag:
                action = "[dry-run]" if dry_run else "saved"
                log.info("  %s %s/%s/%s.json", action, portal, result, endpoint)

        combos = [
            (portal, result, endpoint)
            for portal in portals
            for result in result_types
            for endpoint in FIELD_ENDPOINTS
        ]
        log.info(
            "Fetching %d field lists (max %d concurrent)...",
            len(combos),
            MAX_CONCURRENT,
        )
        await asyncio.gather(*[process(p, r, e) for p, r, e in combos])

    return result_types, changed


# ── Code generation ───────────────────────────────────────────────────────────


def generate_model_file(portal: str, result: str) -> str:
    """Generate Python source for one (portal, result_type) model file."""
    prefix = class_prefix(portal)
    segment = result_to_class_segment(result)
    base = f"{prefix}{segment}"

    search_fields: list[dict] = (
        load_snapshot(SNAPSHOTS_DIR / portal / result / "searchFields.json") or []
    )
    return_fields: list[dict] = (
        load_snapshot(SNAPSHOTS_DIR / portal / result / "returnFields.json") or []
    )

    lines: list[str] = [
        "# AUTO-GENERATED by scripts/generate_models.py — do not edit manually.",
        f"# Portal: {portal} | Result type: {result}",
        "from __future__ import annotations",
        "",
        "from enum import Enum",
        "from typing import Optional",
        "",
        "from pydantic import BaseModel, ConfigDict, Field",
        "",
        "from ena_api_handler.query import ENABaseQuery",
        "",
        "",
        f"class {base}Fields(str, Enum):",
        f'    """Returnable fields for {result} via the {portal} portal."""',
        "",
    ]
    if return_fields:
        for f in return_fields:
            member = f["columnId"].upper()
            value = f["columnId"]
            desc = f.get("description", "").replace('"', "'")
            comment = f"  # {desc}" if desc else ""
            lines.append(f'    {member} = "{value}"{comment}')
    else:
        lines.append("    pass")

    lines += [
        "",
        "",
        f"class {base}Query(ENABaseQuery):",
        f'    """Searchable fields for {result} via the {portal} portal."""',
        "",
    ]
    if search_fields:
        for f in search_fields:
            name = f["columnId"]
            ptype = py_type(f.get("type", "text"))
            desc = f.get("description", "").replace('"', '\\"')
            lines.append(
                f'    {name}: Optional[{ptype}] = Field(default=None, description="{desc}")'
            )
    else:
        lines.append("    pass")

    lines += [
        "",
        "",
        f"class {base}Result(BaseModel):",
        f'    """Typed result row for {result} via the {portal} portal."""',
        "",
        "    model_config = ConfigDict(extra='allow')",
        "",
    ]
    if return_fields:
        for f in return_fields:
            name = f["columnId"]
            ptype = py_type(f.get("type", "text"))
            desc = f.get("description", "").replace('"', '\\"')
            lines.append(
                f'    {name}: Optional[{ptype}] = Field(default=None, description="{desc}")'
            )
    else:
        lines.append("    pass")

    lines.append("")
    return "\n".join(lines)


def generate_portal_init(portal: str, result_types: list[str]) -> str:
    """Generate models/{portal}/__init__.py"""
    prefix = class_prefix(portal)
    lines = [
        "# AUTO-GENERATED by scripts/generate_models.py — do not edit manually.",
        "from __future__ import annotations",
        "",
    ]
    all_names: list[str] = []
    for result in sorted(result_types):
        module = result
        segment = result_to_class_segment(result)
        base = f"{prefix}{segment}"
        names = [f"{base}Fields", f"{base}Query", f"{base}Result"]
        all_names.extend(names)
        lines.append(f"from .{module} import {', '.join(names)}")

    lines += [
        "",
        "__all__ = [",
        *[f'    "{n}",' for n in all_names],
        "]",
        "",
    ]
    return "\n".join(lines)


def generate_models_init(
    portals: list[str],
    all_result_types: list[str],
    portal_results: dict[str, list[str]],
) -> str:
    """Generate models/__init__.py with ENAPortalResultType + RESULT_MODELS."""
    lines = [
        "# AUTO-GENERATED by scripts/generate_models.py — do not edit manually.",
        "from __future__ import annotations",
        "",
        "from enum import Enum",
        "",
        "from pydantic import BaseModel",
        "",
        "from ena_api_handler.types import ENAPortalDataPortal",
        "",
    ]

    # Portal imports
    for portal in sorted(portals):
        results = portal_results.get(portal, [])
        if not results:
            continue
        prefix = class_prefix(portal)
        names = []
        for result in sorted(results):
            segment = result_to_class_segment(result)
            base = f"{prefix}{segment}"
            names += [f"{base}Fields", f"{base}Query", f"{base}Result"]
        pkg = f"ena_api_handler.models.{portal}"
        lines.append(f"from {pkg} import (")
        for name in names:
            lines.append(f"    {name},")
        lines.append(")")

    # ENAPortalResultType enum
    lines += [
        "",
        "",
        "class ENAPortalResultType(str, Enum):",
        '    """All result types available from the ENA Portal API."""',
        "",
    ]
    for result in sorted(all_result_types):
        member = result.upper()
        lines.append(f'    {member} = "{result}"')

    # RESULT_MODELS registry
    lines += [
        "",
        "",
        "RESULT_MODELS: dict[tuple[ENAPortalDataPortal, ENAPortalResultType], type[BaseModel]] = {",
    ]
    for portal in sorted(portals):
        results = portal_results.get(portal, [])
        prefix = class_prefix(portal)
        portal_const = f"ENAPortalDataPortal.{portal.upper()}"
        for result in sorted(results):
            segment = result_to_class_segment(result)
            result_const = f"ENAPortalResultType.{result.upper()}"
            result_cls = f"{prefix}{segment}Result"
            lines.append(f"    ({portal_const}, {result_const}): {result_cls},")
    lines += ["}", ""]

    # __all__
    all_names = ["ENAPortalResultType", "RESULT_MODELS"]
    for portal in sorted(portals):
        results = portal_results.get(portal, [])
        prefix = class_prefix(portal)
        for result in sorted(results):
            segment = result_to_class_segment(result)
            base = f"{prefix}{segment}"
            all_names += [f"{base}Fields", f"{base}Query", f"{base}Result"]

    lines += [
        "__all__ = [",
        *[f'    "{n}",' for n in all_names],
        "]",
        "",
    ]
    return "\n".join(lines)


def _result_class_name(portal: str, result: str, kind: str) -> str:
    return f"{class_prefix(portal)}{result_to_class_segment(result)}{kind}"


def _convenience_result_classes(
    result_types: list[str],
    portal_results: dict[str, list[str]],
) -> list[str]:
    """
    Result class names reachable by a convenience method that queries
    `result_types` in order across CONVENIENCE_PORTALS (in order), matching
    the (result_type, portal) iteration order ENAClient.search() actually uses.
    """
    names: list[str] = []
    for result in result_types:
        for portal in CONVENIENCE_PORTALS:
            if result not in portal_results.get(portal, []):
                continue
            name = _result_class_name(portal, result, "Result")
            if name not in names:
                names.append(name)
    return names


def generate_client_stub(portal_results: dict[str, list[str]]) -> str:
    """
    Generate a .pyi stub for ena_api_handler.client giving ENAClient.search()/
    search_async() precise overloads keyed on the *Query class passed in, and
    giving each convenience method (get_run, get_study, ...) its true return
    type instead of the generic BaseModel the runtime signatures declare.
    """
    query_result_pairs = [
        (portal, result)
        for portal in sorted(portal_results)
        for result in sorted(portal_results[portal])
    ]

    def union(result_types: list[str]) -> str:
        names = _convenience_result_classes(result_types, portal_results)
        return " | ".join(names) if names else "BaseModel"

    imported: set[str] = set()
    for portal, result in query_result_pairs:
        imported.add(_result_class_name(portal, result, "Query"))
        imported.add(_result_class_name(portal, result, "Result"))

    search_param_tail = [
        "        fields: list[Enum | str] | None = ...,",
        "        portals: list[ENAPortalDataPortal] | tuple[ENAPortalDataPortal, ...] = ...,",
        "        limit: int | None = ...,",
        "        include_metagenomes: bool = ...,",
        "        raise_on_empty: bool = ...,",
        "        field_coercions: dict | None = ...,",
        "        field_aliases: dict[str, str] | None = ...,",
        "        exclude: dict[str, Any] | None = ...,",
        "        auth: httpx.Auth | None = ...,",
    ]

    def emit_search_overloads(method: str, is_async: bool) -> list[str]:
        keyword = "async def" if is_async else "def"
        out: list[str] = []
        for portal, result in query_result_pairs:
            query_cls = _result_class_name(portal, result, "Query")
            result_cls = _result_class_name(portal, result, "Result")
            out += [
                "    @overload",
                f"    {keyword} {method}(",
                "        self,",
                "        result: Enum,",
                f"        query: {query_cls},",
                *search_param_tail,
                f"    ) -> list[{result_cls}]: ...",
            ]
        out += [
            "    @overload",
            f"    {keyword} {method}(",
            "        self,",
            "        result: Enum,",
            "        query: ENABaseQuery | ENAQueryClause,",
            *search_param_tail,
            "    ) -> list[BaseModel]: ...",
        ]
        return out

    def emit_method(
        name: str,
        is_async: bool,
        params: list[str],
        return_type: str,
    ) -> list[str]:
        keyword = "async def" if is_async else "def"
        return [
            f"    {keyword} {name}(",
            "        self,",
            *params,
            f"    ) -> {return_type}: ...",
        ]

    convenience_specs: list[tuple[str, list[str], str]] = [
        (
            "get_study",
            [
                "        primary_accession: str | None = ...,",
                "        secondary_accession: str | None = ...,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"{union(['read_study', 'analysis_study', 'study'])} | None",
        ),
        (
            "get_sample",
            [
                "        sample_accession: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"{union(['sample'])} | None",
        ),
        (
            "get_run",
            [
                "        run_accession: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"{union(['read_run'])} | None",
        ),
        (
            "get_study_runs",
            [
                "        study_accession: str,",
                "        fields: list[Enum | str] | None = ...,",
                "        filter_assembly_runs: bool = ...,",
                "        filter_accessions: list[str] | None = ...,",
            ],
            f"list[{union(['read_run'])}]",
        ),
        (
            "get_study_assemblies",
            [
                "        study_accession: str,",
                "        fields: list[Enum | str] | None = ...,",
                "        filter_accessions: list[str] | None = ...,",
                "        allow_non_primary_assembly: bool = ...,",
            ],
            f"list[{union(['analysis'])}]",
        ),
        (
            "get_assembly",
            [
                "        assembly_accession: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"{union(['analysis'])} | None",
        ),
        (
            "get_assembly_from_sample",
            [
                "        sample_name: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"{union(['analysis'])} | None",
        ),
        (
            "get_updated_studies",
            [
                "        cutoff_date: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"list[{union(['study'])}]",
        ),
        (
            "get_updated_runs",
            [
                "        cutoff_date: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"list[{union(['read_run'])}]",
        ),
        (
            "get_updated_assemblies",
            [
                "        cutoff_date: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"list[{union(['analysis'])}]",
        ),
        (
            "get_updated_tpa_assemblies",
            [
                "        cutoff_date: str,",
                "        fields: list[Enum | str] | None = ...,",
            ],
            f"list[{union(['analysis'])}]",
        ),
    ]

    lines: list[str] = [
        "# AUTO-GENERATED by scripts/generate_models.py — do not edit manually.",
        "# Precise return-type stub for ena_api_handler.client, kept separate so the",
        "# runtime module stays free of the generated @overload block.",
        "from __future__ import annotations",
        "",
        "from enum import Enum",
        "from typing import Any, overload",
        "",
        "import httpx",
        "from pydantic import BaseModel",
        "",
        "from ena_api_handler.models import (",
        *[f"    {name}," for name in sorted(imported)],
        ")",
        "from ena_api_handler.query import ENABaseQuery, ENAQueryClause",
        "from ena_api_handler.types import ENAPortalDataPortal",
        "",
        "",
        "class ENAClientError(Exception): ...",
        "",
        "",
        "class ENAAvailabilityError(ENAClientError): ...",
        "",
        "",
        "class ENAClient:",
        "    _url: str",
        "    _auth: httpx.BasicAuth | None",
        "    _timeout: float",
        "    _retries: int",
        "    _client: httpx.Client | None",
        "    _async_client: httpx.AsyncClient | None",
        "    def __init__(",
        "        self,",
        "        url: str = ...,",
        "        username: str | None = ...,",
        "        password: str | None = ...,",
        "        timeout: float = ...,",
        "        retries: int = ...,",
        "    ) -> None: ...",
        "    def close(self) -> None: ...",
        "    def __enter__(self) -> ENAClient: ...",
        "    def __exit__(self, *_: Any) -> None: ...",
        "    async def aclose(self) -> None: ...",
        "    async def __aenter__(self) -> ENAClient: ...",
        "    async def __aexit__(self, *_: Any) -> None: ...",
        "    def download_runs(self, runs: Any) -> None: ...",
        "",
    ]
    lines += emit_search_overloads("search", is_async=False)
    lines.append("")
    lines += emit_search_overloads("search_async", is_async=True)
    lines.append("")
    for name, params, return_type in convenience_specs:
        lines += emit_method(name, False, params, return_type)
    lines.append("")
    for name, params, return_type in convenience_specs:
        lines += emit_method(f"{name}_async", True, params, return_type)
    lines.append("")
    return "\n".join(lines)


def run_generate(
    portals: list[str],
    all_result_types: list[str],
    result_filter: list[str] | None,
    dry_run: bool,
) -> None:
    """Read snapshots and write model files."""
    log.info("Generating models...")

    # Determine which (portal, result) pairs have both snapshot files
    portal_results: dict[str, list[str]] = {}
    for portal in portals:
        available = []
        for result in all_result_types:
            search_path = SNAPSHOTS_DIR / portal / result / "searchFields.json"
            return_path = SNAPSHOTS_DIR / portal / result / "returnFields.json"
            if search_path.exists() and return_path.exists():
                available.append(result)
        if available:
            portal_results[portal] = available

    for portal, results in portal_results.items():
        for result in results:
            path = MODELS_DIR / portal / f"{result}.py"
            content = generate_model_file(portal, result)
            if dry_run:
                log.info("  [dry-run] would write %s", path.relative_to(REPO_ROOT))
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content, encoding="utf-8")
                log.info("  wrote %s", path.relative_to(REPO_ROOT))

        # Portal __init__.py
        init_path = MODELS_DIR / portal / "__init__.py"
        init_content = generate_portal_init(portal, results)
        if dry_run:
            log.info("  [dry-run] would write %s", init_path.relative_to(REPO_ROOT))
        else:
            init_path.parent.mkdir(parents=True, exist_ok=True)
            init_path.write_text(init_content, encoding="utf-8")

    # Top-level models/__init__.py
    models_init_path = MODELS_DIR / "__init__.py"
    models_init_content = generate_models_init(
        portals, all_result_types, portal_results
    )
    if dry_run:
        log.info("  [dry-run] would write %s", models_init_path.relative_to(REPO_ROOT))
    else:
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        models_init_path.write_text(models_init_content, encoding="utf-8")

    # client.pyi — precise search()/convenience-method overloads
    client_stub_content = generate_client_stub(portal_results)
    if dry_run:
        log.info("  [dry-run] would write %s", CLIENT_STUB_PATH.relative_to(REPO_ROOT))
    else:
        CLIENT_STUB_PATH.parent.mkdir(parents=True, exist_ok=True)
        CLIENT_STUB_PATH.write_text(client_stub_content, encoding="utf-8")
        log.info("  wrote %s", CLIENT_STUB_PATH.relative_to(REPO_ROOT))

    total = sum(len(v) for v in portal_results.values())
    log.info(
        "Done: %d model file(s) across %d portal(s).",
        total,
        len(portal_results),
    )


# ── CLI ───────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ENA Portal API fields and generate typed Pydantic models."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without writing any files.",
    )
    parser.add_argument(
        "--ignore-snapshots",
        action="store_true",
        help="Always overwrite snapshots (skip diff comparison).",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip API calls; only regenerate models from existing snapshots.",
    )
    parser.add_argument(
        "--portals",
        default=",".join(ALL_PORTALS),
        help=f"Comma-separated portals (default: {','.join(ALL_PORTALS)}).",
    )
    parser.add_argument(
        "--results",
        default=None,
        help="Comma-separated result types (default: all available).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    portals = [p.strip() for p in args.portals.split(",") if p.strip()]
    result_filter = (
        [r.strip() for r in args.results.split(",") if r.strip()]
        if args.results
        else None
    )

    if args.skip_fetch:
        # Discover result types from existing snapshot directories
        result_types: list[str] = []
        for portal_dir in SNAPSHOTS_DIR.iterdir():
            if not portal_dir.is_dir():
                continue
            for result_dir in portal_dir.iterdir():
                if result_dir.is_dir() and result_dir.name not in result_types:
                    result_types.append(result_dir.name)
        result_types = sorted(set(result_types))
        if result_filter:
            result_types = [r for r in result_types if r in result_filter]
        log.info(
            "Skipping fetch; using %d result types from snapshots.", len(result_types)
        )
    else:
        result_types, changed = asyncio.run(
            run_fetch(portals, result_filter, args.ignore_snapshots, args.dry_run)
        )
        if not changed and not args.ignore_snapshots:
            log.info("No snapshot changes detected; skipping model generation.")
            return

    run_generate(portals, result_types, result_filter, args.dry_run)


if __name__ == "__main__":
    main()
