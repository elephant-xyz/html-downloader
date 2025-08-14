import os
import re
import csv
import argparse
from pathlib import Path
from typing import List, Optional, Tuple
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError


# ----------------------------------------
# ✅ Load credentials from .env
# ----------------------------------------
load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# ----------------------------------------
# ✅ Constants (aligned with push-batches.py)
# ----------------------------------------
BATCH_FOLDER = "./batches"
S3_BUCKET_DEFAULT = "my-property-data-pipeline-uploads-test"
S3_PREFIX_DEFAULT = "batches"
SQS_QUEUE_NAME_DEFAULT = "downloader-v2-batches"

# ----------------------------------------
# ✅ AWS clients
# ----------------------------------------
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
s3_client = session.client("s3")
sqs_client = session.client("sqs")

# ----------------------------------------
# 🔎 Helpers
# ----------------------------------------
FILENAME_RE = re.compile(r"^seed_batch_(\d+)\.csv$")


def extract_batch_index(path: Path) -> Optional[int]:
    m = FILENAME_RE.match(path.name)
    return int(m.group(1)) if m else None


def next_batch_index(batch_dir: Path) -> int:
    indices: List[int] = []
    for p in batch_dir.glob("seed_batch_*.csv"):
        idx = extract_batch_index(p)
        if idx is not None:
            indices.append(idx)
    return (max(indices) + 1) if indices else 1


def pad_index(n: int) -> str:
    return f"{n:04d}"


def create_s3_bucket(bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ S3 bucket exists: {bucket_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"📦 Creating S3 bucket: {bucket_name}")
            if AWS_REGION == "us-east-1":
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': AWS_REGION},
                )
        else:
            raise


def get_or_create_sqs_queue(queue_name: str) -> str:
    try:
        response = sqs_client.get_queue_url(QueueName=queue_name)
        print(f"✅ SQS queue exists: {response['QueueUrl']}")
        return response['QueueUrl']
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            print(f"📩 Creating SQS queue: {queue_name}")
            response = sqs_client.create_queue(QueueName=queue_name)
            return response["QueueUrl"]
        else:
            raise


def upload_file_to_s3(file_path: Path, bucket: str, key: str) -> bool:
    try:
        s3_client.upload_file(str(file_path), bucket, key)
        print(f"✅ Uploaded {file_path.name} to s3://{bucket}/{key}")
        return True
    except Exception as e:
        print(f"❌ Upload failed for {file_path.name}: {e}")
        return False


def send_sqs_message(queue_url: str, s3_key: str, bucket: str) -> None:
    import json
    message = {"s3_key": s3_key, "bucket": bucket}
    try:
        sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
        print(f"📤 Sent SQS message for: {s3_key}")
    except Exception as e:
        print(f"❌ Failed to send message for {s3_key}: {e}")


def split_csv_into_batches(seed_csv: Path, batch_dir: Path, batch_size: int, start_index: int) -> Tuple[int, List[Path]]:
    os.makedirs(batch_dir, exist_ok=True)
    created: List[Path] = []
    with open(seed_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        if not headers:
            raise ValueError("No headers found in CSV")

        batch_idx = start_index
        rows_in_batch = 0
        writer = None
        out_file: Optional[Path] = None
        out_fh = None

        def open_new_batch(idx: int):
            nonlocal writer, out_file, out_fh, rows_in_batch
            if out_fh:
                out_fh.close()
            file_name = f"seed_batch_{pad_index(idx)}.csv"
            out_file = batch_dir / file_name
            out_fh = open(out_file, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(out_fh, fieldnames=headers)
            writer.writeheader()
            rows_in_batch = 0
            created.append(out_file)

        for row in reader:
            if writer is None or rows_in_batch >= batch_size:
                open_new_batch(batch_idx)
                batch_idx += 1
            # Normalize fields for new schema: keep the full row as-is
            # Ensure required columns exist (parcel_id, url, multiValueQueryString)
            if "parcel_id" not in row:
                row["parcel_id"] = row.get("parcelId") or row.get("id") or ""
            if "url" not in row:
                row["url"] = row.get("base_url") or row.get("link") or ""
            if "multiValueQueryString" not in row:
                row["multiValueQueryString"] = row.get("query") or row.get("params") or ""
            writer.writerow(row)
            rows_in_batch += 1

        if out_fh:
            out_fh.close()

    return len(created), created


def main():
    ap = argparse.ArgumentParser(description="Split a seed CSV into seed_batch_XXXX.csv files and push each to S3 and SQS.")
    ap.add_argument("--file", required=True, help="Path to seed CSV file (e.g., seed.csv)")
    ap.add_argument("--size", type=int, default=10, help="Max rows per batch (default: 500)")
    ap.add_argument("--bucket", default=S3_BUCKET_DEFAULT, help="S3 bucket for uploads")
    ap.add_argument("--prefix", default=S3_PREFIX_DEFAULT, help="S3 prefix (default: batches)")
    ap.add_argument("--queue-name", default=SQS_QUEUE_NAME_DEFAULT, help="SQS queue name")
    ap.add_argument("--start", type=int, default=None, help="Starting batch index (default: next available in batches folder)")
    args = ap.parse_args()

    seed_csv = Path(args.file)
    if not seed_csv.exists():
        print(f"❌ File not found: {seed_csv}")
        return

    batch_dir = Path(BATCH_FOLDER)
    start_index = args.start if (args.start and args.start > 0) else next_batch_index(batch_dir)

    print(f"➡️ Splitting {seed_csv} into batches of {args.size} rows starting at index {start_index}...")
    num_created, created_paths = split_csv_into_batches(seed_csv, batch_dir, args.size, start_index)
    print(f"✅ Created {num_created} batch file(s) under {batch_dir}")

    # Prepare AWS targets
    create_s3_bucket(args.bucket)
    queue_url = get_or_create_sqs_queue(args.queue_name)

    # Upload and enqueue
    key_prefix = (args.prefix or "").strip().strip("/")
    uploaded = 0
    for p in created_paths:
        key = f"{key_prefix}/{p.name}" if key_prefix else p.name
        if upload_file_to_s3(p, args.bucket, key):
            send_sqs_message(queue_url, key, args.bucket)
            uploaded += 1

    print(f"🎉 Done. Uploaded & queued {uploaded}/{num_created} new batch file(s).")


if __name__ == "__main__":
    main()


