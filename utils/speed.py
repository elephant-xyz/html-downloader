import os
import argparse
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import boto3


load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def list_zip_timestamps(s3, bucket: str, prefix: str):
    p = (prefix or "").strip().strip("/")
    if p:
        p += "/"
    ts = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=p):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key", "")
            if key.endswith(".zip"):
                lm = obj.get("LastModified")
                # Ensure tz-aware UTC
                if lm and lm.tzinfo is None:
                    lm = lm.replace(tzinfo=timezone.utc)
                ts.append(lm)
    ts.sort()
    return ts


def get_queue_total_messages(sqs, queue_name: str) -> int:
    try:
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
        return visible + not_visible + delayed
    except Exception:
        return 0


def main():
    ap = argparse.ArgumentParser(description="Estimate Lambda processing speed from S3 zip outputs.")
    ap.add_argument("--bucket", required=True, help="S3 bucket name")
    ap.add_argument("--prefix", default="output/html", help="S3 output prefix (default: output/html)")
    ap.add_argument("--window-minutes", type=int, default=60, help="Window size in minutes for current rate (default: 60)")
    ap.add_argument("--queue-name", default="downloader-v2-batches", help="SQS queue name (default: downloader-v2-batches)")
    ap.add_argument("--properties-per-message", type=int, default=10, help="Number of properties per SQS message (default: 10)")
    args = ap.parse_args()

    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")
    sqs = session.client("sqs")

    timestamps = list_zip_timestamps(s3, args.bucket, args.prefix)
    total = len(timestamps)
    if total == 0:
        print("No properties found.")
        return

    now = datetime.now(timezone.utc)
    window_start = now - timedelta(minutes=args.window_minutes)
    in_window = [t for t in timestamps if t and t >= window_start]
    count_window = len(in_window)

    # Current rate (files per second/minute/hour) over window
    window_seconds = max(1, args.window_minutes * 60)
    rate_per_sec = count_window / window_seconds
    rate_per_min = rate_per_sec * 60.0
    rate_per_hour = rate_per_sec * 3600.0

    # Average rate across entire observed period
    first, last = timestamps[0], timestamps[-1]
    elapsed_seconds = max(1.0, (last - first).total_seconds())
    avg_per_second = total / elapsed_seconds
    avg_per_hour = avg_per_second * 3600.0

    print(f"Total processed properties: {total}")
    print(f"Current window ({args.window_minutes}m): {count_window} properties -> {rate_per_sec:.4f} properties/sec, {rate_per_min:.3f} properties/min, {rate_per_hour:.3f} properties/hour")
    print(f"Average since first property: {avg_per_second:.4f} properties/sec, {avg_per_hour:.3f} properties/hour (first={first.isoformat()} last={last.isoformat()})")

    # Estimate time to finish based on SQS backlog
    total_messages = get_queue_total_messages(sqs, args.queue_name)
    pending_properties = total_messages * max(1, args.properties_per_message)
    if rate_per_sec > 0 and pending_properties > 0:
        est_seconds = pending_properties / rate_per_sec
        est_hours = est_seconds / 3600.0
        print(f"Estimated time to finish remaining {pending_properties} properties (from {total_messages} messages): {est_hours:.2f} hours")
    else:
        print("Estimated time to finish: N/A (no current processing or no backlog)")


if __name__ == "__main__":
    main()


