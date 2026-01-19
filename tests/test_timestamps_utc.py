import sys
import pathlib
import pandas as pd

# make the repository root importable for tests
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from ingestor import _to_tidy


def test_to_tidy_converts_timezone_aware_index_to_utc():
    # create a small DataFrame that mimics yfinance single-ticker output
    idx = pd.DatetimeIndex(["2026-01-09 10:00"], tz="America/Sao_Paulo")
    df = pd.DataFrame(
        {
            "Open": [1.0],
            "High": [1.0],
            "Low": [1.0],
            "Close": [1.0],
            "Adj Close": [1.0],
            "Volume": [100],
        },
        index=idx,
    )

    tidy = _to_tidy(df, ["VALE3.SA"])

    assert not tidy.empty
    # original 10:00 America/Sao_Paulo == 13:00 UTC
    assert tidy.loc[0, "trade_date"] == pd.Timestamp("2026-01-09 13:00:00")
    assert tidy.loc[0, "trade_date"].tzinfo is None


def test_arrow_schema_uses_timestamp_ms_if_available():
    import importlib
    import pytest

    ingestor = importlib.import_module("ingestor")
    if importlib.util.find_spec("pyarrow") is None:
        pytest.skip("pyarrow not installed; skipping Arrow schema check")

    ARROW_SCHEMA = getattr(ingestor, "ARROW_SCHEMA", None)
    assert ARROW_SCHEMA is not None
    f = ARROW_SCHEMA.field("trade_date")
    # ensure field is timestamp with millisecond unit
    assert hasattr(f.type, "unit") and f.type.unit == "ms"


def test_validate_date_arg_accepts_and_normalizes():
    from ingestor import validate_date_arg

    assert validate_date_arg("2026-01-16") == "2026-01-16"


def test_validate_date_arg_rejects_invalid():
    import pytest
    from argparse import ArgumentTypeError
    from ingestor import validate_date_arg

    with pytest.raises(ArgumentTypeError):
        validate_date_arg("16-01-2026")