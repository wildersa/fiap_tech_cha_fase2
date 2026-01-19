import argparse
import logging
from pathlib import Path
import pandas as pd
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("fix_refined_columns")

DEFAULT_MAPPING = {
    "max_acao_maxima_#0": "max_acao_maxima",
    "sum_volume_#1": "sum_volume",
}


def process_file(p: Path, mapping: dict, dry_run: bool = False) -> bool:
    """Rename columns in a single parquet file according to mapping.

    Returns True if file was changed (or would be changed in dry_run), False otherwise.
    """
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        logger.warning("Failed to read %s: %s", p, e)
        return False

    cols = set(df.columns)
    to_rename = {k: v for k, v in mapping.items() if k in cols}
    if not to_rename:
        return False

    logger.info("Will rename in %s: %s", p, to_rename)
    if dry_run:
        return True

    # perform rename and write atomically
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        df = df.rename(columns=to_rename)
        # write parquet via pandas (using pyarrow or fastparquet depending on environment)
        # explicitly set snappy compression to match original parquet files
        df.to_parquet(tmp.as_posix(), index=False, compression="snappy")
        tmp.replace(p)
        logger.info("Updated %s", p)
        return True
    except Exception as e:
        logger.error("Failed to write updated parquet for %s: %s", p, e)
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def find_parquets(base: Path):
    return list(base.rglob("*.parquet"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Fix column names in refined parquet files")
    parser.add_argument("--path", default="data/refined", help="Base path containing refined parquets")
    parser.add_argument("--dry-run", action="store_true", help="Only show files that would be changed")
    parser.add_argument("--confirm", action="store_true", help="Apply changes (not just dry-run)")
    parser.add_argument("--mapping", nargs="*", help="Optional custom mapping entries in the form old=new")

    args = parser.parse_args(argv)

    base = Path(args.path)
    if not base.exists():
        logger.error("Base path not found: %s", base)
        return 2

    mapping = DEFAULT_MAPPING.copy()
    if args.mapping:
        for entry in args.mapping:
            if "=" not in entry:
                logger.error("Invalid mapping entry: %s (expect old=new)", entry)
                return 2
            old, new = entry.split("=", 1)
            mapping[old] = new

    files = find_parquets(base)
    logger.info("Parquet files found: %d", len(files))

    changed = 0
    for f in files:
        changed_this = process_file(f, mapping, dry_run=args.dry_run or not args.confirm)
        if changed_this and (args.confirm or args.dry_run):
            changed += 1

    if args.dry_run:
        logger.info("Dry-run complete. Files that would be modified: %d", changed)
    else:
        logger.info("Done. Files modified: %d", changed)

    return 0


if __name__ == "__main__":
    sys.exit(main())
