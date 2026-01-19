import os
import time
import logging
import argparse
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

try:
    import boto3
    from boto3.s3.transfer import TransferConfig
except Exception:  # pragma: no cover - optional runtime dependency
    boto3 = None
    TransferConfig = None
import botocore.exceptions
import pandas as pd
try:
    import yfinance as yf
except Exception:  # pragma: no cover - optional runtime dependency
    yf = None
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - environment may lack pyarrow for lightweight tests
    pa = None
    pq = None
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dev dependency
    load_dotenv = None

if load_dotenv is not None:
    load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("b3_ingest")


# ---------- Parquet schema (stable, Glue-friendly) ----------
# trade_date is stored as UTC (timezone-naive) timestamp in milliseconds.
# - Logical TIMESTAMP (no tz) is the recommended Glue/Spark format; values
#   represent instants in UTC. Files are partitioned by calendar day (dt=YYYY-MM-DD).
if pa is not None:
    ARROW_SCHEMA = pa.schema(
        [
            pa.field("trade_date", pa.timestamp("ms")),
            pa.field("ticker", pa.string()),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("adj_close", pa.float64()),
            pa.field("volume", pa.int64()),
        ]
    )
else:
    ARROW_SCHEMA = None

COL_RENAME = {
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "adj close": "adj_close",
    "adj_close": "adj_close",
    "volume": "volume",
}


def _detect_ticker_level(cols: pd.MultiIndex, tickers: List[str]) -> int:
    """
    yfinance can return MultiIndex columns in different orders depending on group_by.
    Returns which level (0 or 1) represents tickers.
    """
    tset = {t.upper() for t in tickers}
    lvl0 = {str(x).upper() for x in cols.get_level_values(0).unique()}
    lvl1 = {str(x).upper() for x in cols.get_level_values(1).unique()}
    if len(tset & lvl0) >= len(tset) // 2:
        return 0
    if len(tset & lvl1) >= len(tset) // 2:
        return 1
    # fallback: assume level 0 is ticker (most common with group_by='ticker')
    return 0


def _to_tidy(df: pd.DataFrame, tickers: List[str]) -> pd.DataFrame:
    """
    Convert yfinance output to tidy:
    trade_date | ticker | open | high | low | close | adj_close | volume
    """
    if df.empty:
        return df

    # Index -> trade_date
    df = df.copy()
    # preserve timezone information if present on the index; prefer UTC for internal handling
    idx = pd.to_datetime(df.index)
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("UTC")
    df.index = idx

    if isinstance(df.columns, pd.MultiIndex) and df.columns.nlevels == 2:
        ticker_level = _detect_ticker_level(df.columns, tickers)
        # Make level 0 = ticker, level 1 = field
        if ticker_level == 1:
            df.columns = df.columns.swaplevel(0, 1)

        # stack ticker into rows
        tidy = df.stack(level=0, future_stack=True)
        tidy.index.names = ["trade_date", "ticker"]
        tidy = tidy.reset_index()
    else:
        # Single ticker case: yfinance sometimes returns simple columns
        # We require ticker column; infer from input if possible
        only = tickers[0] if tickers else "UNKNOWN"
        tidy = df.reset_index().rename(columns={"index": "trade_date"})
        tidy.insert(1, "ticker", only)

    # Normalize columns
    tidy.columns = [str(c).strip().lower() for c in tidy.columns]

    # Rename pricing columns
    renamed = {}
    for c in tidy.columns:
        key = c.strip().lower()
        if key in COL_RENAME:
            renamed[c] = COL_RENAME[key]
    tidy = tidy.rename(columns=renamed)

    # Keep only expected set (if missing, create as null)
    expected = ["trade_date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]
    for c in expected:
        if c not in tidy.columns:
            tidy[c] = pd.NA
    tidy = tidy[expected]

    # Types: trade_date as timezone-naive Timestamp (keep time for intraday),
    # ticker as string; numerics as float/int
    tidy["trade_date"] = pd.to_datetime(tidy["trade_date"], errors="coerce")

    # NORMALIZATION RULE (Glue/Spark-friendly):
    # - Convert any timezone-aware timestamps to UTC, then drop tz info.
    # - If timestamps are timezone-naive, assume they represent UTC (this is the
    #   storage standard). We log at DEBUG when we must assume UTC for traceability.
    try:
        # tz-aware -> convert to UTC
        if getattr(tidy["trade_date"].dt, "tz", None) is not None:
            tidy["trade_date"] = tidy["trade_date"].dt.tz_convert("UTC").dt.tz_localize(None)
        else:
            # tz-naive: assume UTC (don't loud-log at INFO to avoid noisy output)
            logger.debug("trade_date series is timezone-naive — assuming UTC for storage")
    except Exception:
        # Fallback: coerce and assume UTC
        tidy["trade_date"] = pd.to_datetime(tidy["trade_date"], errors="coerce").dt.tz_localize(None)

    # enforce millisecond precision (Arrow schema uses ms)
    tidy["trade_date"] = tidy["trade_date"].astype("datetime64[ms]")
    tidy["ticker"] = tidy["ticker"].astype(str) 

    for c in ["open", "high", "low", "close", "adj_close"]:
        tidy[c] = pd.to_numeric(tidy[c], errors="coerce").astype("float64")

    # volume: ensure int64 (non-null) for stable schema; coerce null -> 0
    vol = pd.to_numeric(tidy["volume"], errors="coerce")
    tidy["volume"] = vol.fillna(0).astype("int64")

    return tidy


def _validate_interval_period(interval: str, period: str) -> None:
    """Light validation and user-friendly warnings for interval × period combos.

    - Raises ValueError for unknown interval tokens when called at runtime.
    - When used from argparse validators, raise argparse.ArgumentTypeError for prettier CLI errors.
    - Emits warnings for intraday intervals combined with long periods.
    """
    intraday = {"1m", "2m", "5m", "15m", "30m", "60m", "90m"}
    daily = {"1d", "1wk", "1mo"}

    all_known = intraday | daily
    if interval not in all_known:
        raise ValueError(
            f"unsupported interval '{interval}'. supported examples: {sorted(list(all_known))}"
        )

    # Friendly warning: intraday intervals usually have limited historical window on Yahoo
    if interval in intraday:
        # treat long periods (years / many months) as potentially incompatible
        m = re.match(r"^(\d+)(mo|y)$", period)
        if period.endswith("y") or (m and m.group(1) and m.group(2) == "mo" and int(m.group(1)) > 6):
            logger.warning(
                "Intraday interval '%s' + long period '%s' may return truncated history from Yahoo.",
                interval,
                period,
            )


def validate_interval_arg(value: str) -> str:
    """Argparse 'type' validator for --interval. Raises ArgumentTypeError on invalid input."""
    try:
        _validate_interval_period(value, "1d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(str(e))
    return value


def validate_period_arg(value: str) -> str:
    """Basic argparse validator for --period. Accepts common yfinance tokens like '1d','7d','1mo','1y','max','ytd'."""
    allowed_special = {"max", "ytd"}
    if value in allowed_special:
        return value
    if not re.match(r"^\d+(m|d|mo|y|wk|w)$", value):
        raise argparse.ArgumentTypeError(
            "invalid period format; examples: 1d,7d,1mo,3mo,1y,max,ytd"
        )
    return value


def validate_date_arg(value: str) -> str:
    """Argparse validator for dates in YYYY-MM-DD format (inclusive)."""
    try:
        # enforce exact YYYY-MM-DD format to avoid ambiguous parsing
        d = pd.to_datetime(value, format="%Y-%m-%d", errors="raise")
    except Exception:
        raise argparse.ArgumentTypeError("invalid date format; expected YYYY-MM-DD")
    return d.strftime("%Y-%m-%d")


def _write_parquet_atomic(df_day: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".parquet.tmp")

    # Build Arrow table with fixed schema (avoids Spark/Glue infer weirdness)
    if pa is None or pq is None:
        raise RuntimeError("pyarrow is required to write Parquet files — install pyarrow before running this code")

    arrays = [
        # ensure timestamp in ms and timezone-naive
        pa.array(df_day["trade_date"].astype("datetime64[ms]"), type=pa.timestamp("ms")),
        pa.array(df_day["ticker"].astype(str), type=pa.string()),
        pa.array(df_day["open"].astype("float64"), type=pa.float64()),
        pa.array(df_day["high"].astype("float64"), type=pa.float64()),
        pa.array(df_day["low"].astype("float64"), type=pa.float64()),
        pa.array(df_day["close"].astype("float64"), type=pa.float64()),
        pa.array(df_day["adj_close"].astype("float64"), type=pa.float64()),
        pa.array(df_day["volume"].astype("int64"), type=pa.int64()),
    ]
    table = pa.Table.from_arrays(arrays, schema=ARROW_SCHEMA)

    pq.write_table(
        table,
        tmp.as_posix(),
        compression="snappy",
        version="1.0",
        flavor="spark",
        use_dictionary=False,
        write_statistics=False,
    )

    # Quick sanity (magic bytes)
    with open(tmp, "rb") as f:
        if f.read(4) != b"PAR1":
            raise RuntimeError("parquet-magic-missing")
    # ensure footer exists (also PAR1)
    with open(tmp, "rb") as f:
        f.seek(-4, 2)
        if f.read(4) != b"PAR1":
            raise RuntimeError("parquet-footer-magic-missing")

    tmp.replace(out_path)


def _upload_s3_checked(s3, bucket: str, key: str, local_path: Path, retries: int = 3) -> None:
    cfg = TransferConfig(
        multipart_threshold=64 * 1024 * 1024,  # avoid multipart for small files
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=2,
        use_threads=True,
    )

    size = local_path.stat().st_size
    for attempt in range(1, retries + 1):
        try:
            s3.upload_file(local_path.as_posix(), bucket, key, Config=cfg)

            head = s3.head_object(Bucket=bucket, Key=key)
            if head.get("ContentLength", -1) != size:
                raise RuntimeError("s3-size-mismatch")

            logger.info("🚀 S3 OK: s3://%s/%s (%d bytes)", bucket, key, size)
            return
        except botocore.exceptions.ClientError as e:
            logger.warning("S3 ClientError (attempt=%d): %s", attempt, e)
            # don't retry on 4xx auth errors
            code = e.response.get("Error", {}).get("Code", "")
            if code in {"403", "AccessDenied"}:
                raise RuntimeError("s3-access-denied: check credentials and bucket policy") from e
            time.sleep(attempt)
        except Exception as e:
            logger.warning("S3 upload failed (attempt=%d): %s", attempt, e)
            time.sleep(attempt)

    raise RuntimeError(f"upload_failed: s3://{bucket}/{key}")


def ingest(
    tickers: List[str],
    period: str,
    interval: str,
    local_only: bool,
    s3_bucket: Optional[str],
    s3_prefix: str,
    out_dir: Path,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    write_local: bool = True,
    upload_to_s3: bool = False,
) -> None:
    logger.info(
        "Downloading: %d tickers | period=%s | interval=%s",
        len(tickers),
        period,
        interval,
    )

    # validate and yfinance batch download
    try:
        _validate_interval_period(interval, period)
    except ValueError as e:
        raise ValueError(f"invalid interval/period: {e}")

    if yf is None:
        raise RuntimeError("yfinance is not installed in this environment — install 'yfinance' to download market data")

    try:
        download_kwargs = {
            "interval": interval,
            "group_by": "ticker",
            "progress": True,
            "auto_adjust": False,
            "threads": True,
        }
        # start/end override period when provided (dates are inclusive on input; end should be exclusive for yfinance)
        if start_date or end_date:
            if start_date:
                download_kwargs["start"] = start_date
            if end_date:
                download_kwargs["end"] = end_date
        else:
            download_kwargs["period"] = period

        df = yf.download(tickers, **download_kwargs)
    except Exception as e:
        # yfinance may raise a variety of errors (network/HTTP/parsing). Surface a helpful message.
        raise RuntimeError(
            "failed to download data from yfinance — check network, ticker symbols, and interval/period compatibility"
        ) from e

    if df is None or df.empty:
        logger.warning("No data returned. Possible causes: unknown tickers, unsupported interval/period, or empty market days.")
        return

    tidy = _to_tidy(df, tickers)
    if tidy.empty:
        logger.warning("Tidy dataframe empty.")
        return

    logger.info("Timestamps normalized to UTC for storage (logical TIMESTAMP, no tz).")

    # Write one file per day
    s3 = boto3.client("s3") if upload_to_s3 else None

    if upload_to_s3 and not s3_bucket:
        raise RuntimeError("S3_BUCKET not set.")

    processed = 0
    # partition by calendar day (YYYY-MM-DD) while keeping full timestamp values
    tidy["trade_day"] = tidy["trade_date"].dt.strftime("%Y-%m-%d")
    for dt, grp in tidy.groupby("trade_day", sort=True):
        if grp.empty:
            continue

        key = f"{s3_prefix.rstrip('/')}/dt={dt}/b3_stocks.parquet"
        local_path = out_dir / key

        # drop the helper column when writing
        to_write = grp.drop(columns=["trade_day"]) if "trade_day" in grp.columns else grp

        # always write the file locally first (needed for upload), optionally keep it
        _write_parquet_atomic(to_write, local_path)
        if write_local:
            logger.info("✅ Local OK: %s (%d rows)", local_path, len(to_write))
        else:
            logger.info("✅ Temp file written for upload: %s (%d rows)", local_path, len(to_write))

        if upload_to_s3:
            _upload_s3_checked(s3, s3_bucket, key, local_path)
            if not write_local:
                try:
                    local_path.unlink()
                    logger.info("🗑️ Removed local temp file: %s", local_path)
                except Exception:
                    logger.warning("Failed to remove temp local file: %s", local_path)

        processed += 1

    logger.info("Done. Days processed: %d", processed)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "tickers",
        nargs="*",
        default=[
            "VALE3.SA",
            "PETR4.SA",
            "ITUB4.SA",
            "BBDC4.SA",
            "ABEV3.SA",
            "WEGE3.SA",
            "BBAS3.SA",
            "B3SA3.SA",
            "RENT3.SA",
            "SUZB3.SA",
        ],
    )
    parser.add_argument(
        "--period",
        default="1mo",
        type=validate_period_arg,
        help="e.g. 1d, 5d, 1mo, 3mo, 1y (allowed: max,ytd)",
    )
    parser.add_argument(
        "--interval",
        default="1d",
        type=validate_interval_arg,
        help="data interval, e.g. 1m,5m,60m,1d (intraday may have limited history)",
    )
    parser.add_argument(
        "--start-date",
        type=validate_date_arg,
        help="Start date inclusive in YYYY-MM-DD (optional). If provided, overrides --period.",
    )
    parser.add_argument(
        "--end-date",
        type=validate_date_arg,
        help="End date inclusive in YYYY-MM-DD (optional). If omitted and --start-date is given, end = start.",
    )
    parser.add_argument(
        "--date",
        "-d",
        type=validate_date_arg,
        help="Single date YYYY-MM-DD (inclusive). Mutually exclusive with --start-date/--end-date.",
    )
    parser.add_argument("--local", action="store_true", help="Write locally only (no S3 upload)")
    parser.add_argument(
        "--s3-only",
        action="store_true",
        help="Upload to S3 only (do not keep local copy). Mutually exclusive with --local.",
    )
    parser.add_argument(
        "--both",
        action="store_true",
        help="Write locally and upload to S3 explicitly (mutually exclusive with --local/--s3-only).",
    )
    parser.add_argument("--prefix", default="raw", help="S3 prefix (default: raw)")
    parser.add_argument("--out-dir", default="data", help="Local output root dir (default: data)")

    args = parser.parse_args()

    # handle optional explicit dates (make end exclusive for yfinance)
    # --date is a shorthand for a single-day download and is mutually exclusive with --start-date/--end-date
    if args.date and (args.start_date or args.end_date):
        logger.error("--date cannot be used together with --start-date/--end-date")
        sys.exit(2)
    if args.date:
        start_date_str = args.date
        end_date_exclusive = (pd.to_datetime(args.date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        start_date = args.start_date
        end_date = args.end_date
        if start_date and not end_date:
            end_date = start_date
        if start_date or end_date:
            try:
                s = pd.to_datetime(start_date)
                e = pd.to_datetime(end_date)
            except Exception:
                logger.error("Invalid start/end dates")
                sys.exit(2)
            if e < s:
                logger.error("End date must be >= start date")
                sys.exit(2)
            # make end exclusive for yfinance by adding one day
            end_date_exclusive = (e + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
            start_date_str = s.strftime("%Y-%m-%d")
        else:
            start_date_str = None
            end_date_exclusive = None

    bucket = os.environ.get("S3_BUCKET")

    # validate mutually exclusive flags for local/s3 behavior
    if args.s3_only and args.local:
        logger.error("--s3-only cannot be used together with --local")
        sys.exit(2)
    if args.both and (args.local or args.s3_only):
        logger.error("--both cannot be combined with --local or --s3-only")
        sys.exit(2)

    # determine write_local / upload_to_s3 behavior
    if args.s3_only:
        write_local = False
        upload_to_s3 = True
    elif args.local:
        write_local = True
        upload_to_s3 = False
    elif args.both:
        write_local = True
        upload_to_s3 = True
    else:
        # default behavior: write local; upload only if bucket configured
        write_local = True
        upload_to_s3 = bool(bucket)

    try:
        ingest(
            tickers=args.tickers,
            period=args.period,
            interval=args.interval,
            local_only=False,  # deprecated usage internally; replace with explicit flags
            s3_bucket=bucket,
            s3_prefix=args.prefix,
            out_dir=Path(args.out_dir),
            start_date=start_date_str,
            end_date=end_date_exclusive,
            write_local=write_local,
            upload_to_s3=upload_to_s3,
        )
    except argparse.ArgumentTypeError as e:
        logger.error("Invalid CLI argument: %s", e)
        sys.exit(2)
    except ValueError as e:
        logger.error("Validation error: %s", e)
        sys.exit(3)
    except RuntimeError as e:
        logger.error("Runtime error: %s", e)
        logger.debug("Full traceback:", exc_info=True)
        sys.exit(4)
    except botocore.exceptions.NoCredentialsError:
        logger.error("AWS credentials not found. Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY or use a configured profile.")
        sys.exit(5)
    except Exception as e:
        logger.exception("Unexpected error: %s", e)
        sys.exit(99)
