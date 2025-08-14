import os
import argparse
from pathlib import Path
from typing import Tuple
from dotenv import load_dotenv
import boto3


# ----------------------------------------
# ✅ Load credentials from .env
# ----------------------------------------
load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def s3_count_zips(s3, bucket: str, output_prefix: str) -> int:
    prefix = (output_prefix or "").strip().strip("/")
    if prefix:
        prefix = prefix + "/"
    zips = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key", "")
            if key.endswith(".zip"):
                zips += 1
    return zips


def s3_download_errors(s3, bucket: str, errors_key: str, dest_path: Path) -> Tuple[bool, int]:
    try:
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, errors_key, str(dest_path))
        # count rows (excluding header if present)
        lines = dest_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        if not lines:
            return True, 0
        # If header present, assume first line is header
        error_rows = max(0, len(lines) - 1)
        return True, error_rows
    except Exception:
        return False, 0


def sqs_get_queue_counts(sqs, queue_name: str) -> Tuple[int, int, int]:
    url = sqs.get_queue_url(QueueName=queue_name)["QueueUrl"]
    attrs = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    visible = int(attrs.get("ApproximateNumberOfMessages", "0"))
    not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", "0"))
    delayed = int(attrs.get("ApproximateNumberOfMessagesDelayed", "0"))
    return visible, not_visible, delayed


def main():
    ap = argparse.ArgumentParser(description="Show processing status: S3 processed count, download errors.csv, and SQS pending counts.")
    ap.add_argument("--bucket", required=True, help="S3 bucket name (processed outputs + errors.csv)")
    ap.add_argument("--output-prefix", default="output/html", help="S3 output prefix for processed artifacts (default: output/html)")
    ap.add_argument("--errors-key", default="errors.csv", help="S3 key for errors CSV (default: errors.csv at bucket root)")
    ap.add_argument("--download-errors-to", default="./errors.csv", help="Local path to save errors.csv (default: ./errors.csv)")
    ap.add_argument("--queue-name", default="downloader-v2-batches", help="SQS queue name for pending batches")
    args = ap.parse_args()

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")
    sqs = session.client("sqs")

    # S3 processed (zip files only)
    zips = s3_count_zips(s3, args.bucket, args.output_prefix)
    print(f"S3 processed zips under s3://{args.bucket}/{args.output_prefix}: {zips}")

    # errors.csv
    ok, error_rows = s3_download_errors(s3, args.bucket, args.errors_key, Path(args.download_errors_to))
    if ok:
        print(f"Downloaded errors CSV to {args.download_errors_to} with {error_rows} error row(s)")
    else:
        print(f"errors.csv not found at s3://{args.bucket}/{args.errors_key}")

    # SQS pending
    try:
        visible, not_visible, delayed = sqs_get_queue_counts(sqs, args.queue_name)
        print(f"SQS queue {args.queue_name}: visible={visible}, in-flight={not_visible}, delayed={delayed}, total_pending~={visible + not_visible + delayed}")
    except Exception as e:
        print(f"Could not read SQS queue {args.queue_name}: {e}")


if __name__ == "__main__":
    main()


