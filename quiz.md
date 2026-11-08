# Quiz — AWS Kinesis Data Streams

Test what stuck from the guide. 5 questions, answers collapsed below.

---

## Q1 — Shard sizing

A stream ingests **3.5 MB/s at peak** (3,500 records/s). How many shards do you need with 25% headroom, and what happens if you provision exactly 3?

## Q2 — Partition keys

Your partition key is the event type, and there are only 4 distinct event types. What breaks, and what should the key be instead?

## Q3 — Ordering

Two events from the same user arrive in order `A` then `B`. Under what condition could a consumer see `B` before `A`?

## Q4 — Consumer lag

Your Lambda consumer is falling behind. Which CloudWatch metric tells you first, and what alarm threshold does the guide recommend?

## Q5 — Firehose vs Streams

You need clickstream data in S3 as Parquet for Athena queries, and nothing else reads the data in real time. Do you need a Kinesis Data Stream at all? Why or why not?

---

## Answers

<details>
<summary><b>Q1 — Answer</b></summary>

**5 shards.** `max(3.5 MB/s, 3500/1000) = 3.5 → ceil(3.5 × 1.25) = 5`. With exactly 3 shards you have zero headroom: any spike above 3 MB/s or 3,000 records/s throws `ProvisionedThroughputExceededException` and producers start failing.
</details>

<details>
<summary><b>Q2 — Answer</b></summary>

**Hot shards.** Only 4 keys means at most 4 shards ever receive traffic — and if one event type is 80% of volume, one shard takes 80% of the load while the rest sit idle. Scaling shards doesn't help; the key must be high-cardinality, e.g. `user_id` or `device_id`, so traffic spreads evenly.
</details>

<details>
<summary><b>Q3 — Answer</b></summary>

**Almost never — if the partition key is the same.** Records with the same partition key land on the same shard and are delivered in order by a single Lambda execution per shard. Order breaks only if the two events use *different* partition keys (different shards), or after a shard split/merge re-routes the key mid-stream.
</details>

<details>
<summary><b>Q4 — Answer</b></summary>

**`GetRecords.IteratorAgeMilliseconds`** — the age of the last record read. Alarm when it exceeds **60 seconds**: the consumer is falling behind real time. Also watch `ReadProvisionedThroughputExceeded` for shared-throughput contention.
</details>

<details>
<summary><b>Q5 — Answer</b></summary>

**No — use Kinesis Data Firehose directly.** Firehose is a delivery stream: it batches, converts to Parquet, and lands data in S3 without you running any consumer. A Data Stream only earns its keep when you need replay, multiple independent consumers, or sub-second processing.
</details>
