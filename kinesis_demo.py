#!/usr/bin/env python3
"""
AWS Kinesis Data Streams — producer + Lambda consumer demo.

Modes:
  --demo   Run fully offline: simulates a stream in memory and shows
           partitioning, ordering per key, and partial-batch-failure handling.
  --live   Use real boto3 calls (needs AWS credentials + an existing stream).
           Producer only; the Lambda handler below is deployed separately.

Requires: boto3 (only for --live mode).
"""

import argparse
import base64
import json
import random
import time
from collections import defaultdict

STREAM_NAME = "clickstream-events"


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------
class KinesisProducer:
    """Batched producer with partition-key routing and retry on throughput."""

    def __init__(self, stream_name: str, client=None):
        self.stream_name = stream_name
        self.client = client  # boto3 kinesis client in --live mode

    def put_events(self, events: list[dict]) -> dict:
        """Send events in PutRecords batches of up to 500.

        Partition key = user_id, so one user's events stay ordered
        within a single shard. Returns a summary of successes/failures.
        """
        records = [
            {
                "Data": json.dumps(e).encode(),
                "PartitionKey": e["user_id"],  # high-cardinality key!
            }
            for e in events
        ]
        return self._put_records(records)

    def _put_records(self, records, attempt: int = 0) -> dict:
        if self.client is None:
            # Offline demo: pretend the call succeeded.
            return {"successful": len(records), "failed": 0, "records": records}

        response = self.client.put_records(
            StreamName=self.stream_name, Records=records
        )
        failed = response["FailedRecordCount"]
        if failed and attempt < 3:
            # Retry only the failed records, exponential backoff + jitter.
            failed_records = [
                r for r, res in zip(records, response["Records"])
                if "ErrorCode" in res
            ]
            time.sleep((2 ** attempt) + random.uniform(0, 0.5))
            retry = self._put_records(failed_records, attempt + 1)
            return {
                "successful": len(records) - failed + retry["successful"],
                "failed": retry["failed"],
                "records": records,
            }
        return {"successful": len(records) - failed, "failed": failed,
                "records": records}


# ---------------------------------------------------------------------------
# Consumer (AWS Lambda handler)
# ---------------------------------------------------------------------------
def process_event(event: dict) -> None:
    """Your business logic. Must be idempotent — a record may be
    delivered twice after a retry or a shard re-balance."""
    print(f"  processed: {event['user_id']} {event['event']} "
          f"page={event.get('page', event.get('sku', '-'))}")


def lambda_handler(event, context):
    """Lambda event-source-mapping handler for a Kinesis stream.

    Returns batchItemFailures so ONLY the failed records are retried —
    never reprocess a whole batch because of one bad record.
    """
    failures = []
    for record in event["Records"]:
        try:
            payload = json.loads(
                base64.b64decode(record["kinesis"]["data"]).decode()
            )
            process_event(payload)
        except Exception as exc:  # noqa: BLE001 — one bad record, keep going
            print(f"  failed record {record['kinesis']['sequenceNumber']}: {exc}")
            failures.append(
                {"itemIdentifier": record["kinesis"]["sequenceNumber"]}
            )
    return {"batchItemFailures": failures}


# ---------------------------------------------------------------------------
# Offline demo: in-memory stream with shard routing
# ---------------------------------------------------------------------------
def shard_for(partition_key: str, shard_count: int = 3) -> int:
    """Kinesis hashes the partition key (MD5) and maps it to a shard."""
    import hashlib
    digest = int(hashlib.md5(partition_key.encode()).hexdigest(), 16)
    return digest % shard_count


def demo():
    print("== Kinesis offline demo: 3 shards, 12 events ==")
    producer = KinesisProducer(STREAM_NAME)
    events = [
        {"user_id": f"u-{i:03d}", "event": "page_view", "page": "/pricing"}
        for i in range(12)
    ]
    # One hot key: 4 events from the same user all land on one shard.
    events += [
        {"user_id": "u-007", "event": "add_to_cart", "sku": "SKU-99"}
        for _ in range(4)
    ]
    result = producer.put_events(events)
    print(f"PutRecords: {result['successful']} ok, {result['failed']} failed")

    shards = defaultdict(list)
    for e in events:
        shards[shard_for(e["user_id"])].append(e)
    for shard_id in sorted(shards):
        users = sorted({e["user_id"] for e in shards[shard_id]})
        print(f"Shard {shard_id}: {len(shards[shard_id]):2d} records "
              f"from {len(users)} distinct keys {users}")

    print("\n== Lambda consumer: one bad record, partial failure ==")
    fake_event = {
        "Records": [
            {
                "kinesis": {
                    "sequenceNumber": "1",
                    "data": base64.b64encode(
                        json.dumps(events[0]).encode()
                    ).decode(),
                }
            },
            {
                "kinesis": {
                    "sequenceNumber": "2",
                    "data": base64.b64encode(b"not-json{{{").decode(),
                }
            },
        ]
    }
    print("Handler result:", lambda_handler(fake_event, None))


def shard_math(peak_mb_per_s: float, peak_records_per_s: int) -> int:
    """Size a provisioned stream: 1 MB/s or 1,000 records/s per shard,
    plus 25% headroom."""
    import math
    shards = max(peak_mb_per_s, peak_records_per_s / 1000)
    return math.ceil(shards * 1.25)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true",
                        help="run the offline simulation")
    parser.add_argument("--live", action="store_true",
                        help="use real boto3 calls (needs AWS creds)")
    args = parser.parse_args()

    if args.live:
        import boto3
        producer = KinesisProducer(
            STREAM_NAME, client=boto3.client("kinesis")
        )
        print(producer.put_events([
            {"user_id": "u-123", "event": "page_view", "page": "/pricing"},
        ]))
    else:
        demo()
        print(f"\nShard math: 3.5 MB/s peak -> "
              f"{shard_math(3.5, 3500)} shards (with 25% headroom)")
