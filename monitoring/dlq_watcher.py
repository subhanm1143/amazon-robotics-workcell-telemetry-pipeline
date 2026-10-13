"""
Cosmos would page on this with a CloudWatch Alarm; here it is a small
watcher that polls the workcell-events-dlq stream and the staged aggregates,
computes a rolling dead-letter RATE, and prints an alert the moment that
rate crosses a threshold. The rate matters, not the raw count: 40
dead-lettered events out of 40,000 is fine; 40 out of 200 means the schema
changed underneath you.
"""
from __future__ import annotations

import glob
import time

import boto3

ENDPOINT = "http://localhost:4566"
DLQ_STREAM = "workcell-events-dlq"
FAULT_RATE_THRESHOLD = 0.10  # alert once dead-lettered events pass 10% of total
POLL_SECONDS = 5


def _kinesis():
    return boto3.client(
        "kinesis",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def dead_letter_count(kinesis) -> int:
    shards = kinesis.list_shards(StreamName=DLQ_STREAM)["Shards"]
    total = 0
    for shard in shards:
        iterator = kinesis.get_shard_iterator(
            StreamName=DLQ_STREAM,
            ShardId=shard["ShardId"],
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]
        while iterator:
            resp = kinesis.get_records(ShardIterator=iterator, Limit=1000)
            total += len(resp["Records"])
            iterator = resp.get("NextShardIterator")
            if not resp["Records"] and resp.get("MillisBehindLatest", 0) == 0:
                break
    return total


def total_valid_count() -> int:
    total = 0
    for path in glob.glob("staging/valid/**/*.json", recursive=True):
        with open(path) as fh:
            total += sum(1 for line in fh if line.strip())
    return total


def check_once() -> None:
    kinesis = _kinesis()
    dead = dead_letter_count(kinesis)
    valid = total_valid_count()
    total = dead + valid
    if total == 0:
        print("no events seen yet")
        return
    rate = dead / total
    status = "ALARM" if rate > FAULT_RATE_THRESHOLD else "OK"
    print(f"[{status}] dead-letter rate {rate:.1%} ({dead}/{total})")
    if status == "ALARM":
        print(f"  -> would page on-call: dead-letter rate above {FAULT_RATE_THRESHOLD:.0%}")


def main() -> None:
    while True:
        check_once()
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
