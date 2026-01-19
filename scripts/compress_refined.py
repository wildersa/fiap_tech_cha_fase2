import argparse
import logging
from pathlib import Path
import sys
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("compress_refined")

DEFAULT_COMPRESSION = "snappy"


def process_file(p: Path, compression: str = DEFAULT_COMPRESSION, dry_run: bool = False) -> bool:
    """Rewrite a parquet file with given compression. Returns True if file was (or would be) rewritten."""
    try:
        df = pd.read_parquet(p)
    except Exception as e:
        logger.warning("Failed to read %s: %s", p, e)
        return False

    if dry_run:
        logger.info("[dry-run] Would rewrite %s with compression=%s", p, compression)
        return True

    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        df.to_parquet(tmp.as_posix(), index=False, compression=compression)
        tmp.replace(p)
        logger.info("Rewrote %s with compression=%s", p, compression)
        return True
    except Exception as e:
        logger.error("Failed to write compressed parquet for %s: %s", p, e)
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass
        return False


def find_parquets(base: Path):
    return list(base.rglob("*.parquet"))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Rewrite parquet files under a path with specified compression")
    parser.add_argument("--path", default="data/refined", help="Base path containing refined parquets")
    parser.add_argument("--dry-run", action="store_true", help="Only show files that would be rewritten")
    parser.add_argument("--confirm", action="store_true", help="Apply changes (not just dry-run)")
    parser.add_argument("--compression", default=DEFAULT_COMPRESSION, help="Compression codec to use (default: snappy)")

    args = parser.parse_args(argv)

    base = Path(args.path)
    if not base.exists():
        logger.error("Base path not found: %s", base)
        return 2

    files = find_parquets(base)
    logger.info("Parquet files found: %d", len(files))

    rewritten = 0
    for f in files:
        rewritten_this = process_file(f, compression=args.compression, dry_run=args.dry_run or not args.confirm)
        if rewritten_this and (args.confirm or args.dry_run):
            rewritten += 1

    if args.dry_run:
        logger.info("Dry-run complete. Files that would be rewritten: %d", rewritten)
    else:
        logger.info("Done. Files rewritten: %d", rewritten)

    return 0


if __name__ == "__main__":
    sys.exit(main())