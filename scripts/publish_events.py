#!/usr/bin/env python3
"""
publish_events.py — Publish cached Kafka event payloads to the producer REST API.

Reads the first <sample_size> events from each of the 9 cache files and POSTs
them to the producer in interleaved, round-robin batches of up to 50 per topic
group.  This ensures all event types appear early in every topic's message
stream, which is critical for kafka-spy's schema inference.

Usage:
    python3 scripts/publish_events.py <sample_size> <cache_dir> [producer_url]

Arguments:
    sample_size   Number of events per event type to publish
    cache_dir     Directory containing <EVENT_TYPE>.json cache files
    producer_url  Base URL of the producer (default: http://localhost:8081)

Exit codes:
    0  all events published successfully
    1  a curl POST failed or a cache file is missing / has too few events

Examples:
    python3 scripts/publish_events.py 100 ./events-cache
    python3 scripts/publish_events.py 500 ./events-cache http://localhost:8081
"""

import json
import os
import subprocess
import sys

BATCH_SIZE = 50

# Topics whose payloads have no routing field — must use /api/events/raw?topic= instead of /bulk.
RAW_TOPICS = {"untyped-events", "shape-events"}

# Topic groups in the required interleaving order
TOPIC_GROUPS = [
    ("order-events",    ["ORDER_CREATED",     "ORDER_SHIPPED",      "ORDER_CANCELLED"]),
    ("payment-events",  ["PAYMENT_INITIATED", "PAYMENT_COMPLETED",  "PAYMENT_FAILED"]),
    ("user-events",     ["USER_REGISTERED",   "USER_UPDATED",       "USER_DELETED"]),
    ("inferred-events", ["ITEM_ADDED",        "ITEM_REMOVED",       "INVENTORY_ADJUSTED"]),
    ("untyped-events",  ["HEARTBEAT"]),
    ("shape-events",    ["CARD_PAYMENT",      "BANK_TRANSFER"]),
]


def load_events(cache_dir: str, event_type: str, sample_size: int) -> list:
    path = os.path.join(cache_dir, f"{event_type}.json")
    if not os.path.exists(path):
        print(f"ERROR: cache file missing: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path) as f:
        events = json.load(f)
    if len(events) < sample_size:
        print(
            f"ERROR: {event_type} cache has {len(events)} events "
            f"but {sample_size} requested. Run generate_events.py first.",
            file=sys.stderr,
        )
        sys.exit(1)
    return events[:sample_size]


def post_raw_batch(batch: list, topic: str, producer_url: str) -> None:
    payload_file = "/tmp/kafka-spy-batch.json"
    with open(payload_file, "w") as f:
        json.dump(batch, f)
    result = subprocess.run(
        [
            "curl", "-sf", "-X", "POST",
            f"{producer_url}/api/events/raw?topic={topic}",
            "-H", "Content-Type: application/json",
            "-d", f"@{payload_file}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"ERROR: POST to {producer_url}/api/events/raw?topic={topic} failed.\n"
            f"  curl stderr: {result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)


def post_batch(batch: list, producer_url: str) -> None:
    payload_file = "/tmp/kafka-spy-batch.json"
    with open(payload_file, "w") as f:
        json.dump(batch, f)
    result = subprocess.run(
        [
            "curl", "-sf", "-X", "POST",
            f"{producer_url}/api/events/bulk",
            "-H", "Content-Type: application/json",
            "-d", f"@{payload_file}",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(
            f"ERROR: POST to {producer_url}/api/events/bulk failed.\n"
            f"  curl stderr: {result.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)


def publish_topic_group(
    topic: str,
    event_types: list[str],
    events_map: dict[str, list],
    sample_size: int,
    producer_url: str,
) -> None:
    """Publish all event types for one topic in round-robin batches."""
    pointers = {t: 0 for t in event_types}
    total = {t: sample_size for t in event_types}

    while any(pointers[t] < total[t] for t in event_types):
        for t in event_types:
            if pointers[t] >= total[t]:
                continue
            batch = events_map[t][pointers[t]: pointers[t] + BATCH_SIZE]
            if topic in RAW_TOPICS:
                post_raw_batch(batch, topic, producer_url)
            else:
                post_batch(batch, producer_url)
            pointers[t] += len(batch)

    for t in event_types:
        print(f"✓ {t}: published {sample_size}", flush=True)


def main():
    args = sys.argv[1:]
    if len(args) < 2:
        print(__doc__)
        sys.exit(1)

    try:
        sample_size = int(args[0])
    except ValueError:
        print(f"ERROR: sample_size must be an integer, got '{args[0]}'", file=sys.stderr)
        sys.exit(1)

    cache_dir = args[1]
    producer_url = args[2] if len(args) >= 3 else "http://localhost:8081"

    # Load all 15 cache files up front so we fail fast before sending anything
    events_map: dict[str, list] = {}
    for _topic, event_types in TOPIC_GROUPS:
        for et in event_types:
            events_map[et] = load_events(cache_dir, et, sample_size)

    # Publish interleaved by topic group
    for topic, event_types in TOPIC_GROUPS:
        print(f"--- {topic} ---", flush=True)
        publish_topic_group(topic, event_types, events_map, sample_size, producer_url)

    total = sample_size * sum(len(ets) for _, ets in TOPIC_GROUPS)
    print(
        f"\nAll 15 event types published "
        f"({sample_size} each = {total} total messages).",
        flush=True,
    )


if __name__ == "__main__":
    main()
