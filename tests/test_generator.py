"""Unit tests for scripts/generate_models.py.

These tests exercise the code-generation functions directly, using tiny
fixture snapshots written to a temp directory.  They do not touch the real
`snapshots/` directory or make any network calls.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Make the scripts/ directory importable without it being a package.
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
import generate_models as gm  # noqa: E402

# ── Fixture data ──────────────────────────────────────────────────────────────

SEARCH_FIELDS = [
    {"columnId": "study_accession", "description": "Study accession", "type": "text"},
    {"columnId": "base_count", "description": "Base count", "type": "numeric"},
]

RETURN_FIELDS = [
    {"columnId": "run_accession", "description": "Run accession", "type": "text"},
    {"columnId": "base_count", "description": "Base count", "type": "numeric"},
]


@pytest.fixture()
def snapshot_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Populate a minimal (ena, read_run) snapshot and redirect SNAPSHOTS_DIR."""
    result_dir = tmp_path / "ena" / "read_run"
    result_dir.mkdir(parents=True)
    (result_dir / "searchFields.json").write_text(json.dumps(SEARCH_FIELDS))
    (result_dir / "returnFields.json").write_text(json.dumps(RETURN_FIELDS))
    monkeypatch.setattr(gm, "SNAPSHOTS_DIR", tmp_path)
    return tmp_path


# ── Naming helpers ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "result_type,expected",
    [
        ("read_run", "ReadRun"),
        ("tls_set", "TlsSet"),
        ("analysis_study", "AnalysisStudy"),
        ("sample", "Sample"),
    ],
)
def test_result_to_class_segment(result_type: str, expected: str) -> None:
    assert gm.result_to_class_segment(result_type) == expected


@pytest.mark.parametrize(
    "portal,expected",
    [
        ("ena", "ENA"),
        ("faang", "FAANG"),
        ("metagenome", "Metagenome"),
        ("pathogen", "Pathogen"),
    ],
)
def test_class_prefix(portal: str, expected: str) -> None:
    assert gm.class_prefix(portal) == expected


@pytest.mark.parametrize(
    "ena_type,expected_py_type",
    [
        ("text", "str"),
        ("numeric", "float"),
        ("date", "str"),
        ("boolean", "bool"),
        ("unknown", "str"),
        ("TEXT", "str"),  # case-insensitive
    ],
)
def test_py_type(ena_type: str, expected_py_type: str) -> None:
    assert gm.py_type(ena_type) == expected_py_type


# ── Snapshot I/O ──────────────────────────────────────────────────────────────


def test_load_snapshot_missing_returns_none(tmp_path: Path) -> None:
    assert gm.load_snapshot(tmp_path / "nonexistent.json") is None


def test_load_snapshot_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "fields.json"
    gm.write_snapshot(path, SEARCH_FIELDS, dry_run=False)
    assert gm.load_snapshot(path) == SEARCH_FIELDS


def test_write_snapshot_dry_run_does_not_create_file(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "fields.json"
    gm.write_snapshot(path, SEARCH_FIELDS, dry_run=True)
    assert not path.exists()


def test_write_snapshot_creates_file(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "fields.json"
    gm.write_snapshot(path, SEARCH_FIELDS, dry_run=False)
    assert path.exists()
    loaded = json.loads(path.read_text())
    assert loaded == SEARCH_FIELDS


# ── generate_model_file ───────────────────────────────────────────────────────


def test_generate_model_file_class_names(snapshot_dir: Path) -> None:
    source = gm.generate_model_file("ena", "read_run")
    assert "class ENAReadRunFields(str, Enum):" in source
    assert "class ENAReadRunQuery(ENABaseQuery):" in source
    assert "class ENAReadRunResult(BaseModel):" in source


def test_generate_model_file_fields_enum_members(snapshot_dir: Path) -> None:
    source = gm.generate_model_file("ena", "read_run")
    assert 'RUN_ACCESSION = "run_accession"' in source
    assert 'BASE_COUNT = "base_count"' in source


def test_generate_model_file_query_field_types(snapshot_dir: Path) -> None:
    source = gm.generate_model_file("ena", "read_run")
    assert "study_accession: Optional[str]" in source
    assert "base_count: Optional[float]" in source


def test_generate_model_file_result_field_types(snapshot_dir: Path) -> None:
    source = gm.generate_model_file("ena", "read_run")
    assert "run_accession: Optional[str]" in source
    assert "base_count: Optional[float]" in source


def test_generate_model_file_descriptions_present(snapshot_dir: Path) -> None:
    source = gm.generate_model_file("ena", "read_run")
    assert "Study accession" in source
    assert "Run accession" in source
    assert "Base count" in source


def test_generate_model_file_empty_snapshots_uses_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When no snapshot files exist, classes fall back to `pass`."""
    monkeypatch.setattr(gm, "SNAPSHOTS_DIR", tmp_path)
    source = gm.generate_model_file("ena", "read_run")
    # Both Fields and Query/Result should contain `pass`
    assert source.count("    pass") >= 2


def test_generate_model_file_omitting_field_changes_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Removing a field from searchFields changes the generated Query class."""
    result_dir = tmp_path / "ena" / "read_run"
    result_dir.mkdir(parents=True)
    (result_dir / "returnFields.json").write_text(json.dumps(RETURN_FIELDS))
    monkeypatch.setattr(gm, "SNAPSHOTS_DIR", tmp_path)

    # Full search fields
    (result_dir / "searchFields.json").write_text(json.dumps(SEARCH_FIELDS))
    source_full = gm.generate_model_file("ena", "read_run")

    # Reduced to one field
    (result_dir / "searchFields.json").write_text(json.dumps(SEARCH_FIELDS[:1]))
    source_reduced = gm.generate_model_file("ena", "read_run")

    assert source_full != source_reduced
    assert "study_accession" in source_full
    assert "base_count: Optional[float]" in source_full  # still in returnFields
    # base_count query field removed
    assert (
        "    base_count: Optional[float] = Field"
        not in source_reduced.split("class ENAReadRunResult")[0]
    )


# ── generate_portal_init ──────────────────────────────────────────────────────


def test_generate_portal_init_import_lines() -> None:
    source = gm.generate_portal_init("ena", ["read_run", "sample"])
    assert (
        "from .read_run import ENAReadRunFields, ENAReadRunQuery, ENAReadRunResult"
        in source
    )
    assert (
        "from .sample import ENASampleFields, ENASampleQuery, ENASampleResult" in source
    )


def test_generate_portal_init_all_contains_class_names() -> None:
    source = gm.generate_portal_init("metagenome", ["read_run"])
    assert '"MetagenomeReadRunFields"' in source
    assert '"MetagenomeReadRunQuery"' in source
    assert '"MetagenomeReadRunResult"' in source


# ── generate_models_init ──────────────────────────────────────────────────────


def test_generate_models_init_result_type_enum() -> None:
    source = gm.generate_models_init(
        portals=["ena"],
        all_result_types=["read_run", "sample"],
        portal_results={"ena": ["read_run", "sample"]},
    )
    assert "class ENAPortalResultType(str, Enum):" in source
    assert 'READ_RUN = "read_run"' in source
    assert 'SAMPLE = "sample"' in source


def test_generate_models_init_result_models_registry() -> None:
    source = gm.generate_models_init(
        portals=["ena"],
        all_result_types=["read_run"],
        portal_results={"ena": ["read_run"]},
    )
    assert (
        "(ENAPortalDataPortal.ENA, ENAPortalResultType.READ_RUN): ENAReadRunResult,"
        in source
    )


# ── generate_client_stub ──────────────────────────────────────────────────────

_STUB_PORTAL_RESULTS = {
    "ena": ["read_run", "read_study", "study", "analysis", "analysis_study", "sample"],
    "metagenome": [
        "read_run",
        "read_study",
        "study",
        "analysis",
        "analysis_study",
        "sample",
    ],
}


def test_generate_client_stub_parses_as_python() -> None:
    import ast

    source = gm.generate_client_stub(_STUB_PORTAL_RESULTS)
    ast.parse(source)


def test_generate_client_stub_search_overload_per_query_type() -> None:
    source = gm.generate_client_stub(_STUB_PORTAL_RESULTS)
    assert "@overload" in source
    assert (
        "        query: ENAReadRunQuery,\n" in source
        and "    ) -> list[ENAReadRunResult]: ..." in source
    )
    assert (
        "        query: MetagenomeReadRunQuery,\n" in source
        and "    ) -> list[MetagenomeReadRunResult]: ..." in source
    )


def test_generate_client_stub_search_fallback_overload() -> None:
    source = gm.generate_client_stub(_STUB_PORTAL_RESULTS)
    assert "query: ENABaseQuery | ENAQueryClause,\n" in source
    assert source.count("-> list[BaseModel]: ...") == 2  # search + search_async


def test_generate_client_stub_search_async_present() -> None:
    source = gm.generate_client_stub(_STUB_PORTAL_RESULTS)
    assert "async def search_async(" in source


def test_generate_client_stub_get_run_returns_portal_union() -> None:
    source = gm.generate_client_stub(_STUB_PORTAL_RESULTS)
    assert (
        "    def get_run(\n"
        "        self,\n"
        "        run_accession: str,\n"
        "        fields: list[Enum | str] | None = ...,\n"
        "    ) -> MetagenomeReadRunResult | ENAReadRunResult | None: ...\n" in source
    )


def test_generate_client_stub_get_study_returns_six_way_union() -> None:
    source = gm.generate_client_stub(_STUB_PORTAL_RESULTS)
    assert (
        "MetagenomeReadStudyResult | ENAReadStudyResult | "
        "MetagenomeAnalysisStudyResult | ENAAnalysisStudyResult | "
        "MetagenomeStudyResult | ENAStudyResult | None" in source
    )


def test_generate_client_stub_omits_portal_without_result() -> None:
    """A result type only present for one portal shouldn't reference the other's class."""
    source = gm.generate_client_stub({"ena": ["sample"]})
    assert "MetagenomeSampleResult" not in source
    assert "ENASampleResult | None" in source
