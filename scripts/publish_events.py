#!/usr/bin/env python3
"""
publish_events.py — Publish cached Kafka event payloads to the producer REST API.

Reads the first <sample_size> events from each cache file and POSTs them to the
producer in interleaved, round-robin batches of up to 50 per topic group.  This
ensures all event types appear early in every topic's message stream, which is
critical for kafka-spy's schema inference.

Usage:
    python3 scripts/publish_events.py <sample_size> <cache_dir> [producer_url]
                                      [--topics topic1,topic2,...]

Arguments:
    sample_size         Number of events per event type to publish
    cache_dir           Directory containing <EVENT_TYPE>.json cache files
    producer_url        Base URL of the producer (default: http://localhost:8081)
    --topics t1,t2,...  Comma-separated list of topic names to publish; omit to
                        publish all topics in TOPIC_GROUPS.

Exit codes:
    0  all events published successfully
    1  a curl POST failed, a cache file is missing / has too few events, or an
       unknown topic name was specified via --topics

Examples:
    python3 scripts/publish_events.py 100 ./events-cache
    python3 scripts/publish_events.py 500 ./events-cache http://localhost:8081
    python3 scripts/publish_events.py 2 ./events-cache --topics small-sample-events
"""

import json
import os
import subprocess
import sys

BATCH_SIZE = 50

# Topics whose payloads have no routing field — must use /api/events/raw?topic= instead of /bulk.
# All edge-case topics also use raw publish (producer doesn't know their routing).
RAW_TOPICS = {
    "untyped-events", "shape-events",
    # Phase 1 confirm
    "competing-candidates-events", "false-positive-events",
    "partial-field-events", "generic-events",
    # Phase 1 negotiate (infra only)
    "partial-field-literal-events", "overlapping-values-events",
    # Phase 2 confirm (multi-N)
    "small-sample-events",
}

# Topic groups in the required interleaving order
TOPIC_GROUPS = [
    ("order-events",    ["ORDER_CREATED",     "ORDER_SHIPPED",      "ORDER_CANCELLED"]),
    ("payment-events",  ["PAYMENT_INITIATED", "PAYMENT_COMPLETED",  "PAYMENT_FAILED"]),
    ("user-events",     ["USER_REGISTERED",   "USER_UPDATED",       "USER_DELETED"]),
    ("inferred-events", ["ITEM_ADDED",        "ITEM_REMOVED",       "INVENTORY_ADJUSTED"]),
    ("untyped-events",  ["HEARTBEAT"]),
    ("shape-events",    ["CARD_PAYMENT",      "BANK_TRANSFER"]),
    # Phase 1 confirm
    ("competing-candidates-events",   ["CC_ORDER_CREATED",   "CC_ORDER_CANCELLED"]),
    ("false-positive-events",         ["FP_PAYMENT_INITIATED", "FP_PAYMENT_COMPLETED"]),
    ("partial-field-events",          ["PARTIAL_FIELD_EVENT"]),
    ("generic-events",                ["GENERIC_EVENT"]),
    # Phase 1 negotiate (infra only — no EXPECTED entry)
    ("partial-field-literal-events",  ["PARTIAL_LITERAL_EVENT"]),
    ("overlapping-values-events",     ["OVERLAPPING_VALUE_EVENT"]),
    # Phase 2 confirm (multi-N — use --topics small-sample-events with varied sample_size)
    ("small-sample-events",           ["SM_USER_REGISTERED", "SM_USER_DELETED"]),
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
    # When sample_size < BATCH_SIZE with multiple types, each round-robin turn would
    # post ALL events of one type before ANY of the next type, bunching them together.
    # Use batch_size=1 instead so types interleave at message level — kafka-spy's probe
    # then sees all types from the very first messages regardless of probe-count.
    effective_batch = 1 if (len(event_types) > 1 and sample_size < BATCH_SIZE) else BATCH_SIZE
    pointers = {t: 0 for t in event_types}
    total = {t: sample_size for t in event_types}

    while any(pointers[t] < total[t] for t in event_types):
        for t in event_types:
            if pointers[t] >= total[t]:
                continue
            batch = events_map[t][pointers[t]: pointers[t] + effective_batch]
            if topic in RAW_TOPICS:
                post_raw_batch(batch, topic, producer_url)
            else:
                post_batch(batch, producer_url)
            pointers[t] += len(batch)

    for t in event_types:
        print(f"✓ {t}: published {sample_size}", flush=True)


def main():
    raw_args = sys.argv[1:]

    # Extract --topics before positional parsing
    topics_filter: set[str] | None = None
    filtered_args = []
    i = 0
    while i < len(raw_args):
        if raw_args[i] == "--topics":
            if i + 1 >= len(raw_args):
                print("ERROR: --topics requires a value", file=sys.stderr)
                sys.exit(1)
            topics_filter = {t.strip() for t in raw_args[i + 1].split(",")}
            i += 2
        elif raw_args[i].startswith("--topics="):
            topics_filter = {t.strip() for t in raw_args[i][len("--topics="):].split(",")}
            i += 1
        else:
            filtered_args.append(raw_args[i])
            i += 1

    if len(filtered_args) < 2:
        print(__doc__)
        sys.exit(1)

    try:
        sample_size = int(filtered_args[0])
    except ValueError:
        print(f"ERROR: sample_size must be an integer, got '{filtered_args[0]}'", file=sys.stderr)
        sys.exit(1)

    cache_dir = filtered_args[1]
    producer_url = filtered_args[2] if len(filtered_args) >= 3 else "http://localhost:8081"

    # Resolve which topic groups to publish
    known_topics = {t for t, _ in TOPIC_GROUPS}
    if topics_filter is not None:
        unknown = topics_filter - known_topics
        if unknown:
            print(
                f"ERROR: unknown topic(s) in --topics: {sorted(unknown)}. "
                f"Known topics: {sorted(known_topics)}",
                file=sys.stderr,
            )
            sys.exit(1)
        active_groups = [(t, ets) for t, ets in TOPIC_GROUPS if t in topics_filter]
    else:
        active_groups = list(TOPIC_GROUPS)

    # Load only the needed cache files up front so we fail fast before sending anything
    events_map: dict[str, list] = {}
    for _topic, event_types in active_groups:
        for et in event_types:
            events_map[et] = load_events(cache_dir, et, sample_size)

    # Publish interleaved by topic group
    for topic, event_types in active_groups:
        print(f"--- {topic} ---", flush=True)
        publish_topic_group(topic, event_types, events_map, sample_size, producer_url)

    total = sample_size * sum(len(ets) for _, ets in active_groups)
    n_types = sum(len(ets) for _, ets in active_groups)
    print(
        f"\n{n_types} event type(s) published "
        f"({sample_size} each = {total} total messages).",
        flush=True,
    )


if __name__ == "__main__":
    main()
