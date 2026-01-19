import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict
import pandas as pd
import re
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("read_refined")

PARTITION_KEY_ALIASES = {
    "dt": "data_pregao",
    "date": "data_pregao",
    "data_pregao": "data_pregao",
    "ticker": "acao_negociada",
    "acao": "acao_negociada",
    "acao_negociada": "acao_negociada",
}


def parse_partitions_from_path(p: Path) -> Dict[str, str]:
    """Parse partition key=val segments from a path into a dict with normalized keys.

    Examples:
      data/refined/data_pregao=2026-01-16/acao_negociada=VALE3.SA/b3.parquet
      or data/refined/dt=2026-01-16/ticker=VALE3.SA/...
    """
    parts = [part for part in p.parts]
    out: Dict[str, str] = {}
    for part in parts:
        if "=" in part:
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip()
            norm = PARTITION_KEY_ALIASES.get(k.lower())
            if norm:
                out[norm] = v
    return out


def find_parquet_files(base: Path) -> List[Path]:
    if not base.exists():
        raise RuntimeError(f"base path not found: {base}")
    files = list(base.rglob("*.parquet"))
    return files


def filter_files(
    files: Iterable[Path],
    data_pregaos: Optional[List[str]] = None,
    acoes: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[Path]:
    data_pregaos_set = set(data_pregaos or [])
    acoes_set = set(acoes or [])

    def ok(p: Path) -> bool:
        parts = parse_partitions_from_path(p)
        td = parts.get("data_pregao")
        ac = parts.get("acao_negociada")
        if data_pregaos_set and (td not in data_pregaos_set):
            return False
        if acoes_set and (ac not in acoes_set):
            return False
        if start and td and td < start:
            return False
        if end and td and td > end:
            return False
        return True

    return [p for p in files if ok(p)]


def read_parquet_files(files: List[Path], columns: Optional[List[str]] = None) -> pd.DataFrame:
    if not files:
        return pd.DataFrame()
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f, columns=columns)
            # inject partition columns if missing
            parts = parse_partitions_from_path(f)
            if "data_pregao" in parts and "data_pregao" not in df.columns:
                df["data_pregao"] = pd.to_datetime(parts["data_pregao"])  # naive date
            if "acao_negociada" in parts and "acao_negociada" not in df.columns:
                df["acao_negociada"] = parts["acao_negociada"]
            dfs.append(df)
        except Exception as e:
            logger.warning("Failed to read %s: %s", f, e)
    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    return out


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Read refined parquet partitioned by data_pregao and acao_negociada")
    parser.add_argument("--path", default="data/refined", help="Local root path for refined data (default: data/refined)")
    parser.add_argument("--trade-date", action="append", help="Filter by data_pregao (YYYY-MM-DD). Can be repeated.")
    parser.add_argument("--acao", action="append", help="Filter by acao_negociada (e.g. VALE3.SA). Can be repeated.")
    parser.add_argument("--start", help="Start data_pregao (inclusive) YYYY-MM-DD")
    parser.add_argument("--end", help="End data_pregao (inclusive) YYYY-MM-DD")
    parser.add_argument("--out-csv", help="Write combined CSV to this path")
    parser.add_argument("--sample", type=int, help="Print head(sample) of resulting DataFrame and exit")
    parser.add_argument("--stats", action="store_true", help="Print basic counts by data_pregao and acao_negociada")
    parser.add_argument("--max-files", type=int, default=0, help="Limit number of parquet files to read (0 = no limit)")

    args = parser.parse_args(argv)

    base = Path(args.path)
    files = find_parquet_files(base)
    if not files:
        logger.info("No parquet files found under %s", base)
        return 0

    # filter
    files = filter_files(
        files,
        data_pregaos=args.data_pregao,
        acoes=args.acao,
        start=args.start,
        end=args.end,
    )

    if args.max_files and args.max_files > 0:
        files = sorted(files)[: args.max_files]

    logger.info("Files to read: %d", len(files))

    df = read_parquet_files(files)

    if df.empty:
        logger.info("No rows after reading selected files.")
        return 0

    if args.sample:
        print(df.head(args.sample).to_string(index=False))
        return 0

    if args.stats:
        print("Counts by data_pregao:")
        print(df.groupby(df["data_pregao"].dt.strftime("%Y-%m-%d")).size().sort_index())
        print("\nCounts by acao_negociada:")
        print(df.groupby("acao_negociada").size().sort_values(ascending=False))
        return 0

    if args.out_csv:
        outp = Path(args.out_csv)
        outp.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(outp, index=False)
        logger.info("Wrote combined CSV to %s", outp)
        return 0

    # If user didn't specify a trade-date filter, list all rows (ordered)
    if not args.data_pregao:
        df_sorted = df.sort_values(["data_pregao", "acao_negociada"])
        # print the full table; if very large, warn the user
        if len(df_sorted) > 10000:
            logger.warning("Result contains %d rows; printing may be large.", len(df_sorted))
        print(df_sorted.to_string(index=False))
        return 0

    # default: show summary for filtered dates
    print(df.info())
    print(df.head().to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())