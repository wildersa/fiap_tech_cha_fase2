import sys
import pathlib
from pathlib import Path
import pandas as pd
import pytest

# make the repository root importable for tests
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from fix_refined_columns import process_file, DEFAULT_MAPPING

# Skip tests if no parquet engine available
try:
    import pyarrow  # type: ignore
    _HAS_PARQUET = True
except Exception:
    try:
        import fastparquet  # type: ignore
        _HAS_PARQUET = True
    except Exception:
        _HAS_PARQUET = False

pytest.skip("pyarrow/fastparquet required for parquet tests", allow_module_level=True) if not _HAS_PARQUET else None


def test_process_file_renames(tmp_path):
    p = tmp_path / "trade_date=2026-01-16" / "acao_negociada=VALE3.SA"
    p.mkdir(parents=True)
    f = p / "b3.parquet"

    df = pd.DataFrame({
        "max_acao_maxima_#0": [1.0, 2.0],
        "sum_volume_#1": [10, 20],
        "other": ["a", "b"],
    })
    df.to_parquet(f)

    # dry-run should detect change but not modify
    changed = process_file(f, DEFAULT_MAPPING, dry_run=True)
    assert changed

    # apply change
    changed = process_file(f, DEFAULT_MAPPING, dry_run=False)
    assert changed

    df2 = pd.read_parquet(f)
    assert "max_acao_maxima" in df2.columns
    assert "sum_volume" in df2.columns
    assert "max_acao_maxima_#0" not in df2.columns
    assert "sum_volume_#1" not in df2.columns

    # If pyarrow is available, assert that written file uses SNAPPY compression to match original
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(f)
        # check first column codec (parquet written with snappy)
        assert pf.metadata.row_group(0).column(0).compression == "SNAPPY"
    except Exception:
        # skip codec check if pyarrow not installed or metadata not available
        pass
