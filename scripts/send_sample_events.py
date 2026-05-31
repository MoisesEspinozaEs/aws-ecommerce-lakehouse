"""
Generate fake clickstream events for the demo.

Two modes:
  --to-s3     write events straight to S3 bronze (the cheap path, no Kinesis).
              Use this for the lowest cost, or while a new AWS account is still
              being activated and Kinesis is not yet available.
  --to-kinesis send events to the Kinesis stream (the live streaming path).

Examples:
    python send_sample_events.py --to-s3 --bucket ecomlake-dev-lake-<account-id> --count 5000
    python send_sample_events.py --to-kinesis --stream ecomlake-dev-clickstream --count 5000
"""

import argparse
import gzip
import io
import json
import random
import time
from datetime import datetime, timedelta, timezone

import boto3

CATEGORIES = ["electronics", "books", "home", "fashion", "toys"]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]

# Spread events over the last N days so time-series charts show a real trend
# instead of a single spike. Sessions still group correctly per user per day.
SPREAD_DAYS = 14


def make_event() -> dict:
    # pick a random moment within the last SPREAD_DAYS
    days_ago = random.randint(0, SPREAD_DAYS - 1)
    seconds_into_day = random.randint(0, 86399)
    event_time = (
        datetime.now(timezone.utc)
        - timedelta(days=days_ago, seconds=seconds_into_day)
    )
    return {
        "user_id": f"u{random.randint(1, 2000)}",
        "event_type": random.choices(EVENT_TYPES, weights=[70, 20, 10])[0],
        "product_id": f"p{random.randint(1, 500)}",
        "url": f"/{random.choice(CATEGORIES)}/p{random.randint(1, 500)}",
        "event_time": event_time.isoformat(),
    }


def send_to_s3(count: int, bucket: str, region: str) -> None:
    """Write all events as one gzipped newline-delimited JSON file in bronze."""
    s3 = boto3.client("s3", region_name=region)
    lines = "\n".join(json.dumps(make_event()) for _ in range(count)) + "\n"

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz:
        gz.write(lines.encode("utf-8"))
    buffer.seek(0)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    key = f"bronze/raw_clicks/clicks-{stamp}.json.gz"
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"wrote {count} events to s3://{bucket}/{key}")


def send_to_kinesis(count: int, stream: str, region: str) -> None:
    """Stream events one by one into Kinesis (the live path)."""
    client = boto3.client("kinesis", region_name=region)
    sent = 0
    for _ in range(count):
        event = make_event()
        client.put_record(
            StreamName=stream,
            Data=(json.dumps(event) + "\n").encode("utf-8"),
            PartitionKey=event["user_id"],
        )
        sent += 1
        if sent % 500 == 0:
            print(f"sent {sent} events")
            time.sleep(0.2)
    print(f"done: {sent} events to {stream}")


def main() -> None:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--to-s3", action="store_true", help="write straight to S3 bronze")
    mode.add_argument("--to-kinesis", action="store_true", help="stream into Kinesis")

    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--bucket", help="lake bucket (required for --to-s3)")
    parser.add_argument("--stream", default="ecomlake-dev-clickstream")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    if args.to_s3:
        if not args.bucket:
            parser.error("--to-s3 needs --bucket")
        send_to_s3(args.count, args.bucket, args.region)
    else:
        send_to_kinesis(args.count, args.stream, args.region)


if __name__ == "__main__":
    main()
