import sys
import pathlib
from pathlib import Path

# make the repository root importable for tests
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from read_refined import parse_partitions_from_path, filter_files


def test_parse_partitions_from_path():
    p = Path("data/refined/trade_date=2026-01-16/acao_negociada=VALE3.SA/b3.parquet")
    parts = parse_partitions_from_path(p)
    assert parts["trade_date"] == "2026-01-16"
    assert parts["acao_negociada"] == "VALE3.SA"


def test_filter_files_by_date_and_acao(tmp_path):
    d = tmp_path / "refined"
    (d / "trade_date=2026-01-16" / "acao_negociada=VALE3.SA").mkdir(parents=True)
    f = (d / "trade_date=2026-01-16" / "acao_negociada=VALE3.SA" / "b3.parquet")
    f.write_text("x")

    # create another date
    (d / "trade_date=2026-01-17" / "acao_negociada=PETR4.SA").mkdir(parents=True)
    f2 = (d / "trade_date=2026-01-17" / "acao_negociada=PETR4.SA" / "b3.parquet")
    f2.write_text("x")

    files = list(d.rglob("*.parquet"))
    res = filter_files(files, trade_dates=["2026-01-16"], acoes=["VALE3.SA"])
    assert len(res) == 1
    assert res[0].name == "b3.parquet"
