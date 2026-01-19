import os
import sys
import time
import logging
import argparse
from pathlib import Path
from typing import Iterator, Optional

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
except Exception:  # pragma: no cover - optional runtime dependency
    boto3 = None
    TransferConfig = None

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dev dependency
    load_dotenv = None

# Try to load .env with python-dotenv if available; otherwise fallback to a simple parser
def _load_env_fallback(path: str = ".env") -> None:
    """Lightweight .env loader that sets AWS-related variables if python-dotenv is not installed."""
    p = Path(path)
    if not p.exists():
        return
    try:
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip("\"'")
            # only load AWS and S3 related keys (safer)
            if k.startswith("AWS_") or k.startswith("S3_"):
                os.environ.setdefault(k, v)
    except Exception:
        # non-fatal; continue without raising
        return

if load_dotenv is not None:
    load_dotenv()
else:
    # attempt fallback load if dotenv not available
    from pathlib import Path

    _load_env_fallback()


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("s3_downloader")


def _list_objects_recursive(s3, bucket: str, prefix: str) -> Iterator[dict]:
    """Yield object metadata (dict) for all objects under `prefix` using pagination.

    Raises a helpful RuntimeError if AWS credentials are not available.
    """
    paginator = s3.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket, "Prefix": prefix}
    try:
        for page in paginator.paginate(**kwargs):
            for obj in page.get("Contents", []):
                yield obj
    except Exception as e:
        # Import here to avoid hard dependency at the module top
        try:
            import botocore.exceptions as _bce
        except Exception:
            raise
        if isinstance(e, _bce.NoCredentialsError):
            raise RuntimeError(
                "AWS credentials not found: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY in env or use a configured profile (or add them to .env)."
            ) from e
        raise


def _download_s3_checked(s3, bucket: str, key: str, local_path: Path, retries: int = 3) -> None:
    """Download S3 object to `local_path` with basic verification and retries."""
    cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=2,
        use_threads=True,
    ) if TransferConfig is not None else None

    # ensure directory exists
    local_path.parent.mkdir(parents=True, exist_ok=True)

    expected_size = None
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        expected_size = head.get("ContentLength")
    except Exception as e:
        logger.warning("Could not head object %s: %s", key, e)

    tmp = local_path.with_suffix(local_path.suffix + ".tmp")

    for attempt in range(1, retries + 1):
        try:
            if cfg is not None:
                s3.download_file(bucket, key, tmp.as_posix(), Config=cfg)
            else:
                s3.download_file(bucket, key, tmp.as_posix())

            # verify size when available
            if expected_size is not None:
                size = tmp.stat().st_size
                if size != expected_size:
                    raise RuntimeError("s3-size-mismatch")

            tmp.replace(local_path)
            logger.info("✅ Downloaded: s3://%s/%s -> %s (%s bytes)", bucket, key, local_path, expected_size)
            return
        except Exception as e:
            logger.warning("Download failed (attempt=%d): %s", attempt, e)
            time.sleep(attempt)

    raise RuntimeError(f"download_failed: s3://{bucket}/{key}")


def download_prefix(
    bucket: str,
    prefix: str,
    out_dir: Path,
    s3=None,
    dry_run: bool = False,
) -> int:
    """Download all objects under bucket/prefix into out_dir preserving prefix structure.

    Returns the number of objects downloaded (or that would be downloaded in dry-run).
    """
    if s3 is None:
        if boto3 is None:
            raise RuntimeError("boto3 is required to download from S3 — install boto3")
        # ensure credentials from .env are loaded even if python-dotenv isn't installed
        # (ingestor.py relies on dotenv similarly)
        try:
            import botocore
        except Exception:
            botocore = None

        # Try to detect credentials available in environment/session
        session = boto3.Session()
        creds = session.get_credentials()
        if creds is None and not any(os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE")):
            logger.warning(
                "AWS credentials not detected in environment. If you use a .env file, ensure it contains AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or pass --bucket with a configured profile."
            )

        s3 = boto3.client("s3")

    # normalize prefix no-leading-slash
    prefix = prefix.lstrip("/")

    count = 0
    for obj in _list_objects_recursive(s3, bucket, prefix):
        key = obj["Key"]
        # ignore keys that end with '/'
        if key.endswith("/"):
            continue

        rel = key  # keep full key under out_dir
        local_path = out_dir / rel

        if dry_run:
            logger.info("[dry-run] s3://%s/%s -> %s", bucket, key, local_path)
            count += 1
            continue

        _download_s3_checked(s3, bucket, key, local_path)
        count += 1

    logger.info("Done. Objects processed: %d", count)
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prefix",
        default="refined",
        help="S3 prefix to download (default: refined)",
    )
    parser.add_argument(
        "--out-dir",
        default="data",
        help="Local output root dir (default: data)",
    )
    parser.add_argument("--dry-run", action="store_true", help="List objects that would be downloaded and exit")
    parser.add_argument("--bucket", help="S3 bucket name (defaults to S3_BUCKET env var)")

    args = parser.parse_args()

    bucket = args.bucket or os.environ.get("S3_BUCKET")

    if not bucket:
        logger.error("S3 bucket not specified. Set --bucket or S3_BUCKET env var.")
        sys.exit(2)

    try:
        download_prefix(bucket=bucket, prefix=args.prefix, out_dir=Path(args.out_dir), dry_run=args.dry_run)
    except RuntimeError as e:
        logger.error("Runtime error: %s", e)
        sys.exit(4)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(99)