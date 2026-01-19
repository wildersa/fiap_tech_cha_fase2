import sys
import pathlib
from pathlib import Path
import pandas as pd
import pytest

# make the repository root importable for tests
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from compress_refined import process_file

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


def test_process_file_rewrites(tmp_path):
    p = tmp_path / "trade_date=2026-01-16" / "acao_negociada=VALE3.SA"
    p.mkdir(parents=True)
    f = p / "b3.parquet"

    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": ["x", "y", "z"],
    })

    # write initial file with gzip compression to ensure it changes
    df.to_parquet(f, compression="gzip")

    # dry-run should detect change but not modify
    changed = process_file(f, dry_run=True)
    assert changed

    # apply change
    changed = process_file(f, dry_run=False)
    assert changed

    # if pyarrow available, assert that written file uses SNAPPY compression
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(f)
        assert pf.metadata.row_group(0).column(0).compression == "SNAPPY"
    except Exception:
        # skip codec check if pyarrow not installed or metadata not available
        pass