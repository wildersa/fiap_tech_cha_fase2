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


# Lightweight .env loader (same approach as s3_downloader)
def _load_env_fallback(path: str = ".env") -> None:
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
            if k.startswith("AWS_") or k.startswith("S3_"):
                os.environ.setdefault(k, v)
    except Exception:
        return


if load_dotenv is not None:
    load_dotenv()
else:
    _load_env_fallback()


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("s3_uploader")


def _list_local_files_recursive(root: Path) -> Iterator[Path]:
    """Yield all file paths under `root` (skips directories)."""
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def _upload_s3_checked(s3, bucket: str, key: str, local_path: Path, retries: int = 3) -> None:
    """Upload local file to s3://bucket/key with size verification and retries."""
    cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,  # avoid multipart for small files
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=2,
        use_threads=True,
    ) if TransferConfig is not None else None

    size = local_path.stat().st_size
    for attempt in range(1, retries + 1):
        try:
            if cfg is not None:
                s3.upload_file(local_path.as_posix(), bucket, key, Config=cfg)
            else:
                s3.upload_file(local_path.as_posix(), bucket, key)

            head = s3.head_object(Bucket=bucket, Key=key)
            if head.get("ContentLength", -1) != size:
                raise RuntimeError("s3-size-mismatch")

            logger.info("✅ Uploaded: s3://%s/%s (%d bytes)", bucket, key, size)
            return
        except Exception as e:
            # try to extract helpful info if botocore present
            try:
                import botocore
                from botocore.exceptions import ClientError
            except Exception:
                ClientError = None

            if ClientError is not None and isinstance(e, ClientError):
                logger.warning("S3 ClientError (attempt=%d): %s", attempt, e)
                code = e.response.get("Error", {}).get("Code", "")
                if code in {"403", "AccessDenied"}:
                    raise RuntimeError("s3-access-denied: check credentials and bucket policy") from e
            else:
                logger.warning("S3 upload failed (attempt=%d): %s", attempt, e)

            time.sleep(attempt)

    raise RuntimeError(f"upload_failed: s3://{bucket}/{key}")


def upload_prefix(bucket: str, prefix: str, local_root: Path, s3=None, dry_run: bool = False) -> int:
    """Upload all files under `local_root` to s3://bucket/prefix/... preserving relative paths.

    Returns number of objects uploaded (or would be uploaded in dry-run).
    """
    if s3 is None:
        if boto3 is None:
            raise RuntimeError("boto3 is required to upload to S3 — install boto3")

        # warn if credentials not detected
        session = boto3.Session()
        creds = session.get_credentials()
        if creds is None and not any(os.environ.get(k) for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE")):
            logger.warning(
                "AWS credentials not detected in environment. If you use a .env file, ensure it contains AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or use a configured profile."
            )

        s3 = boto3.client("s3")

    prefix = prefix.strip("/")
    count = 0

    root = local_root
    if not root.exists():
        raise RuntimeError(f"local_root does not exist: {root}")

    for local_path in _list_local_files_recursive(root):
        rel = local_path.relative_to(root)
        key = f"{prefix}/{rel.as_posix()}" if prefix else rel.as_posix()

        if dry_run:
            logger.info("[dry-run] %s -> s3://%s/%s", local_path, bucket, key)
            count += 1
            continue

        _upload_s3_checked(s3, bucket, key, local_path)
        count += 1

    logger.info("Done. Objects processed: %d", count)
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local-root",
        default="data/refined",
        help="Local root dir to upload (default: data/refined)",
    )
    parser.add_argument("--prefix", default="refined", help="S3 prefix to upload into (default: refined)")
    parser.add_argument("--bucket", help="S3 bucket name (defaults to S3_BUCKET env var)")
    parser.add_argument("--dry-run", action="store_true", help="List objects that would be uploaded and exit")

    args = parser.parse_args()

    bucket = args.bucket or os.environ.get("S3_BUCKET")

    if not bucket:
        logger.error("S3 bucket not specified. Set --bucket or S3_BUCKET env var.")
        sys.exit(2)

    try:
        upload_prefix(bucket=bucket, prefix=args.prefix, local_root=Path(args.local_root), dry_run=args.dry_run)
    except RuntimeError as e:
        logger.error("Runtime error: %s", e)
        sys.exit(4)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(99)
