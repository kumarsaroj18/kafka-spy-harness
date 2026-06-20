# Implementation Plan — Extend kafka-spy-manual-test for New Discriminator Inference Modes

## Problem Statement

Extend the manual test harness to exercise 3 new discriminator inference modes
(`ExplicitSingle` with `action` field, `NoVariants/SingleType`, `ImplicitShape`), topic
auto-discovery, the merged AsyncAPI spec command, and `metadata.json` round-trip validation.

## Background

The specmatic repo implements `DiscriminatorInferrer` that auto-detects discriminator fields
during a probe phase:
- When `--discriminator` is explicitly provided, probe is skipped; `inferenceResultsByTopic` is
  never populated; `SpyMetadataWriter.write(directory, null)` writes `{"discriminatorResultType":
  "SINGLE_TYPE"}`.
- When omitted, inference runs and `metadata.json` gets the actual result type (`EXPLICIT_SINGLE`,
  `IMPLICIT_SHAPE`, etc.).
- `kafka-asyncapi` reads `metadata.json` as fallback when no `--discriminator` flag is given.
- `kafka-asyncapi-merged` reads per-topic subdirectories and generates a single merged spec.
- `DiscriminatorInferrer.deriveImplicitShape()` names each cluster's `schemaFile` after
  `paths.first()` — the alphabetically first unique field in that cluster. For `shape-events`
  this produces `cardNumber.json` (CARD_PAYMENT cluster) and `accountNumber.json`
  (BANK_TRANSFER cluster), NOT `CARD_PAYMENT.json` / `BANK_TRANSFER.json`.

## Decisions

1. **validate-metadata.py**: expect `SINGLE_TYPE` for order/payment/user-events. These topics are
   run with explicit `--discriminator eventType`, so `inferenceResultsByTopic` is never populated
   and the writer receives `null` → `SINGLE_TYPE`.
2. **Bulk endpoint routing**: try `eventType` first, fall back to `action` for TOPIC_MAP lookup.
3. **Raw-topic routing**: `POST /api/events/raw?topic=<topic>` handles both HEARTBEAT (no
   discriminator field) and CARD_PAYMENT/BANK_TRANSFER. A single endpoint replaces the
   shape-only pattern from the original plan and closes the HEARTBEAT routing gap.
4. **Ground-truth schema names for shape-events**: `cardNumber.json` and `accountNumber.json`
   (matching what specmatic actually produces). Do NOT name them `CARD_PAYMENT.json` /
   `BANK_TRANSFER.json`.
5. **Auto-discovery output**: write to `{REPO_ROOT}/auto-discovered-schemas/` (separate root,
   NOT inside `inferred-schemas/`). This prevents the auto-discovered directory from polluting
   validate-metadata.py and validate-asyncapi.py, both of which scan all subdirectories of
   `inferred-schemas/`.

---

## Parallel Batches

Tasks within a batch have no mutual dependencies and can be implemented simultaneously.

| Batch | Tasks | Can start after |
|-------|-------|-----------------|
| A | 1, 2 | immediately |
| B | 3, 4, 5 | immediately |
| C | 6, 7 | immediately |
| D | 8, 9 | Tasks 3+4 build successfully |
| E | 10, 11 | Tasks 6+7 produce correct output |
| F | 12, 13, 14 | all of the above |

---

## Batch A — Static files (no dependencies)

### Task 1: Ground-truth schemas for new event types

Create 6 schema files. Use JSON Schema Draft-07 with `$schema`, `type: "object"`, `required`,
and `properties` keys. Follow the format of existing files in `schemas/order-events/`.

**`schemas/inferred-events/ITEM_ADDED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["action", "itemId", "name", "quantity", "category"],
  "properties": {
    "action":   { "type": "string", "const": "ITEM_ADDED" },
    "itemId":   { "type": "string" },
    "name":     { "type": "string" },
    "quantity": { "type": "integer", "minimum": 1, "maximum": 100 },
    "category": { "type": "string", "enum": ["electronics", "clothing", "food", "sports"] }
  }
}
```

**`schemas/inferred-events/ITEM_REMOVED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["action", "itemId", "reason"],
  "properties": {
    "action": { "type": "string", "const": "ITEM_REMOVED" },
    "itemId": { "type": "string" },
    "reason": { "type": "string", "enum": ["out-of-stock", "discontinued", "damaged", "recalled"] }
  }
}
```

**`schemas/inferred-events/INVENTORY_ADJUSTED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["action", "itemId", "delta", "adjustedBy"],
  "properties": {
    "action":     { "type": "string", "const": "INVENTORY_ADJUSTED" },
    "itemId":     { "type": "string" },
    "delta":      { "type": "integer" },
    "adjustedBy": { "type": "string", "format": "email" }
  }
}
```

**`schemas/untyped-events/untyped-events.json`** — no discriminator field; named after the
topic (matches what kafka-spy produces for a `SingleType` router: `<topicName>.json`).
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["serviceId", "timestamp", "healthy", "uptimeSeconds"],
  "properties": {
    "serviceId":     { "type": "string" },
    "timestamp":     { "type": "string" },
    "healthy":       { "type": "boolean" },
    "uptimeSeconds": { "type": "integer", "minimum": 0 }
  }
}
```

**`schemas/shape-events/cardNumber.json`** — named after the first alphabetical unique field
of the CARD_PAYMENT cluster (`cardNumber`, `cvv`, `expiry` are unique to this cluster; `amount`
is shared). kafka-spy sets `schemaFile = "${signaturePaths.first()}.json"` = `cardNumber.json`.
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["cardNumber", "expiry", "cvv", "amount"],
  "properties": {
    "cardNumber": { "type": "string" },
    "expiry":     { "type": "string" },
    "cvv":        { "type": "string" },
    "amount":     { "type": "number", "minimum": 0 }
  }
}
```

**`schemas/shape-events/accountNumber.json`** — named after the first alphabetical unique
field of the BANK_TRANSFER cluster (`accountNumber`, `bankName`, `routingNumber` are unique;
`amount` is shared).
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["routingNumber", "accountNumber", "bankName", "amount"],
  "properties": {
    "routingNumber":  { "type": "string" },
    "accountNumber":  { "type": "string" },
    "bankName":       { "type": "string" },
    "amount":         { "type": "number", "minimum": 0 }
  }
}
```

**Demo**: `ls schemas/inferred-events schemas/untyped-events schemas/shape-events` shows 6 files.

---

### Task 2: Config YAMLs for new topics and merged spec

Follow the format of `config/order-events.yaml`. The 3 topic configs intentionally omit
`discriminator`; kafka-asyncapi reads `metadata.json` from the inferred-schemas dir instead.

**`config/inferred-events.yaml`**
```yaml
broker: localhost:9092
topic: inferred-events
operation: send
title: Inferred Events API
version: 1.0.0
contentType: application/json
```

**`config/untyped-events.yaml`**
```yaml
broker: localhost:9092
topic: untyped-events
operation: send
title: Untyped Events API
version: 1.0.0
contentType: application/json
```

**`config/shape-events.yaml`**
```yaml
broker: localhost:9092
topic: shape-events
operation: send
title: Shape Events API
version: 1.0.0
contentType: application/json
```

**`config/merged.yaml`** — no `topic` field; kafka-asyncapi-merged discovers topics from
the input directory. `output-file` is optional; if omitted, derived from `title`.
```yaml
broker: localhost:9092
operation: send
title: All Kafka Events API
version: 1.0.0
contentType: application/json
output-file: all-kafka-events.yaml
```

**Demo**: `cat config/inferred-events.yaml config/merged.yaml` shows valid YAML.

---

## Batch B — Python scripts (no dependencies)

### Task 3: Update scripts/generate_events.py

All changes are additive. Add 6 generator functions and register them in `_GENERATORS`.

**New constants** (add near the top alongside existing ones):
```python
_ITEM_NAMES     = ["Widget", "Gadget", "Doohickey", "Thingamajig", "Gizmo",
                   "Sprocket", "Cog", "Bolt", "Lever", "Pulley"]
_ITEM_CATEGORIES = ["electronics", "clothing", "food", "sports"]
_REMOVE_REASONS  = ["out-of-stock", "discontinued", "damaged", "recalled"]
_ADJUST_EMAILS   = ["admin@example.com", "ops@warehouse.com", "manager@supply.com",
                    "stock@fulfillment.com"]
_SERVICE_IDS     = [f"svc-{i:03d}" for i in range(1, 11)]
_BANK_NAMES      = ["First National", "City Trust", "Coastal Bank",
                    "Union Federal", "Heritage Savings"]
```

**New generators**:
```python
def _gen_item_added(i: int) -> dict:
    return {
        "action":   "ITEM_ADDED",
        "itemId":   rand_uuid(),
        "name":     random.choice(_ITEM_NAMES),
        "quantity": random.randint(1, 100),
        "category": cycle(_ITEM_CATEGORIES, i),
    }

def _gen_item_removed(i: int) -> dict:
    return {
        "action": "ITEM_REMOVED",
        "itemId": rand_uuid(),
        "reason": cycle(_REMOVE_REASONS, i),
    }

def _gen_inventory_adjusted(i: int) -> dict:
    delta = random.choice([d for d in range(-50, 51) if d != 0])
    return {
        "action":     "INVENTORY_ADJUSTED",
        "itemId":     rand_uuid(),
        "delta":      delta,
        "adjustedBy": cycle(_ADJUST_EMAILS, i),
    }

def _gen_heartbeat(i: int) -> dict:
    # No action/eventType field — exactly 4 fields always, same structure every time.
    # This is intentional: kafka-spy must infer NoVariants → SingleType.
    return {
        "serviceId":     cycle(_SERVICE_IDS, i),
        "timestamp":     rand_ts(),
        "healthy":       random.choice([True, False]),
        "uptimeSeconds": random.randint(0, 86400),
    }

def _gen_card_payment(_i: int) -> dict:
    # No eventType, action, type, kind, or any string-token field that could act as discriminator.
    # Only field shared with BANK_TRANSFER is 'amount'.
    month = random.randint(1, 12)
    year  = random.randint(25, 30)
    return {
        "cardNumber": "".join(str(random.randint(0, 9)) for _ in range(16)),
        "expiry":     f"{month:02d}/{year}",
        "cvv":        "".join(str(random.randint(0, 9)) for _ in range(3)),
        "amount":     round(random.uniform(1.0, 999.99), 2),
    }

def _gen_bank_transfer(_i: int) -> dict:
    # No eventType, action, type, kind — shares only 'amount' with CARD_PAYMENT.
    return {
        "routingNumber": "".join(str(random.randint(0, 9)) for _ in range(9)),
        "accountNumber": "".join(str(random.randint(0, 9)) for _ in range(random.randint(8, 12))),
        "bankName":      random.choice(_BANK_NAMES),
        "amount":        round(random.uniform(1.0, 9999.99), 2),
    }
```

**Register in `_GENERATORS`**:
```python
"ITEM_ADDED":           _gen_item_added,
"ITEM_REMOVED":         _gen_item_removed,
"INVENTORY_ADJUSTED":   _gen_inventory_adjusted,
"HEARTBEAT":            _gen_heartbeat,
"CARD_PAYMENT":         _gen_card_payment,
"BANK_TRANSFER":        _gen_bank_transfer,
```

Update docstring: change "9 supported event types" → "15 supported event types".

**Demo**:
```bash
python3 scripts/generate_events.py HEARTBEAT 3 /tmp/test-cache
cat /tmp/test-cache/HEARTBEAT.json
# Verify: no "action"/"eventType" key, exactly 4 top-level fields.

python3 scripts/generate_events.py CARD_PAYMENT 3 /tmp/test-cache
cat /tmp/test-cache/CARD_PAYMENT.json
# Verify: no "action"/"eventType"/"type"/"kind", only cardNumber/expiry/cvv/amount.
```

---

### Task 4: Update scripts/publish_events.py

Three changes:

**1. Add new topic groups** (after the existing 3 entries in `TOPIC_GROUPS`):
```python
("inferred-events", ["ITEM_ADDED", "ITEM_REMOVED", "INVENTORY_ADJUSTED"]),
("untyped-events",  ["HEARTBEAT"]),
("shape-events",    ["CARD_PAYMENT", "BANK_TRANSFER"]),
```

**2. Add `post_raw_batch()` function** for topics whose payloads have no routing field:
```python
# Topics that use /api/events/raw?topic=<topic> because payloads have no routing field.
RAW_TOPICS = {"untyped-events", "shape-events"}

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
```

**3. Update `publish_topic_group()`** to dispatch to the correct endpoint:
```python
def publish_topic_group(
    topic: str,
    event_types: list[str],
    events_map: dict[str, list],
    sample_size: int,
    producer_url: str,
) -> None:
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
```

**4.** Update the "load all N cache files up front" comment and final summary string from
"9 event types" → "15 event types".

**Demo**: `python3 scripts/publish_events.py 5 /tmp/test-cache` — should print 15 FAIL lines
(cache dir exists but files are missing for the existing 9 types), not a Python error. The
script validates all 15 cache files exist before sending anything.

---

### Task 5: Create scripts/validate-metadata.py (new file)

```
Usage: python3 scripts/validate-metadata.py --inferred-schemas-dir <dir>
```

```python
#!/usr/bin/env python3
"""
Validate metadata.json files written by kafka-spy for each topic directory.

Usage:
    python3 scripts/validate-metadata.py --inferred-schemas-dir <dir>

Exit codes:
    0  all expected topics PASS
    1  one or more expected topics FAIL
"""

import argparse
import json
import sys
from pathlib import Path

EXPECTED = {
    "order-events":   {"discriminatorResultType": "SINGLE_TYPE"},
    "payment-events": {"discriminatorResultType": "SINGLE_TYPE"},
    "user-events":    {"discriminatorResultType": "SINGLE_TYPE"},
    "inferred-events": {
        "discriminatorResultType": "EXPLICIT_SINGLE",
        "discriminatorField": "action",
    },
    "untyped-events": {"discriminatorResultType": "SINGLE_TYPE"},
    "shape-events":   {"discriminatorResultType": "IMPLICIT_SHAPE", "minClusterSignatures": 2},
}

VALID_TYPES = {"EXPLICIT_SINGLE", "EXPLICIT_COMPOSITE", "IMPLICIT_SHAPE", "SINGLE_TYPE"}


def validate_topic(topic_dir: Path) -> list[str]:
    """Returns list of error strings (empty = PASS)."""
    errors = []
    meta_file = topic_dir / "metadata.json"

    if not meta_file.exists():
        return [f"metadata.json not found in {topic_dir}"]

    try:
        meta = json.loads(meta_file.read_text())
    except json.JSONDecodeError as e:
        return [f"metadata.json is not valid JSON: {e}"]

    result_type = meta.get("discriminatorResultType")
    if result_type not in VALID_TYPES:
        errors.append(
            f"discriminatorResultType is '{result_type}', "
            f"expected one of {sorted(VALID_TYPES)}"
        )
        return errors

    # Structural validation per type
    if result_type == "EXPLICIT_SINGLE":
        field = meta.get("discriminatorField")
        if not field:
            errors.append("EXPLICIT_SINGLE requires non-empty 'discriminatorField'")
    elif result_type == "EXPLICIT_COMPOSITE":
        fields = meta.get("discriminatorFields")
        if not isinstance(fields, list) or len(fields) != 2 or not all(fields):
            errors.append("EXPLICIT_COMPOSITE requires 'discriminatorFields': [<str>, <str>]")
    elif result_type == "IMPLICIT_SHAPE":
        sigs = meta.get("clusterSignatures")
        if not isinstance(sigs, list) or len(sigs) < 2:
            errors.append("IMPLICIT_SHAPE requires 'clusterSignatures' with >= 2 entries")
        else:
            for i, sig in enumerate(sigs):
                if not sig.get("schemaFile"):
                    errors.append(f"clusterSignatures[{i}] missing 'schemaFile'")
                paths = sig.get("signaturePaths")
                if not isinstance(paths, list) or len(paths) == 0:
                    errors.append(f"clusterSignatures[{i}] missing non-empty 'signaturePaths'")
    # SINGLE_TYPE: no extra keys required

    # Hard-coded expected values
    topic = topic_dir.name
    if topic not in EXPECTED:
        return errors  # Unknown topic — structural validation only, no expected-value check

    expected = EXPECTED[topic]
    expected_type = expected["discriminatorResultType"]
    if result_type != expected_type:
        errors.append(
            f"Expected discriminatorResultType '{expected_type}' for '{topic}', got '{result_type}'"
        )
    elif expected_type == "EXPLICIT_SINGLE":
        expected_field = expected.get("discriminatorField")
        actual_field = meta.get("discriminatorField")
        if expected_field and actual_field != expected_field:
            errors.append(
                f"Expected discriminatorField '{expected_field}' for '{topic}', got '{actual_field}'"
            )
    elif expected_type == "IMPLICIT_SHAPE":
        min_sigs = expected.get("minClusterSignatures", 2)
        sigs = meta.get("clusterSignatures", [])
        if len(sigs) < min_sigs:
            errors.append(
                f"Expected >= {min_sigs} clusterSignatures for '{topic}', got {len(sigs)}"
            )

    return errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inferred-schemas-dir", required=True, type=Path)
    args = parser.parse_args()

    topic_dirs = sorted(d for d in args.inferred_schemas_dir.iterdir() if d.is_dir())
    any_fail = False

    for topic_dir in topic_dirs:
        topic = topic_dir.name
        if topic not in EXPECTED:
            print(f"  WARN  {topic}: unexpected topic directory, skipping")
            continue
        errors = validate_topic(topic_dir)
        if errors:
            print(f"  FAIL  {topic}:")
            for e in errors:
                print(f"          - {e}")
            any_fail = True
        else:
            print(f"  PASS  {topic}")

    # Check all expected topics were present
    found = {d.name for d in topic_dirs}
    for topic in EXPECTED:
        if topic not in found:
            print(f"  FAIL  {topic}: directory not found in {args.inferred_schemas_dir}")
            any_fail = True

    if any_fail:
        print("\nMetadata validation failed.")
        sys.exit(1)
    else:
        print("\nAll metadata files validated successfully.")


if __name__ == "__main__":
    main()
```

**Demo**: After a full kafka-spy run:
```bash
python3 scripts/validate-metadata.py --inferred-schemas-dir inferred-schemas
# Expected output: PASS for all 6 topics
```

---

### Task 6: Update scripts/validate-asyncapi.py

Four targeted changes to the existing file:

**Change 1 — filter metadata.json from schema file enumeration** (critical — without this, the
validator tries to find a `metadata` message in every spec and fails for all 6 topics):

In `validate_spec()`, change:
```python
schema_files = list((inferred_dir / topic).glob("*.json"))
```
to:
```python
schema_files = [f for f in (inferred_dir / topic).glob("*.json")
                if f.name != "metadata.json"]
```

**Change 2 — conditional `const` check based on metadata.json**:

At the top of `validate_spec()`, read the result type from `metadata.json`:
```python
result_type = "EXPLICIT_SINGLE"  # default if metadata absent
meta_file = inferred_dir / topic / "metadata.json"
if meta_file.exists():
    try:
        meta = json.loads(meta_file.read_text())
        result_type = meta.get("discriminatorResultType", "EXPLICIT_SINGLE")
    except Exception:
        pass

requires_const = (result_type == "EXPLICIT_SINGLE")
```

Then guard the `const` check:
```python
if requires_const:
    for msg_name, msg_def in messages.items():
        payload = (msg_def or {}).get("payload") or {}
        properties = payload.get("properties") or {}
        has_const = any("const" in (v or {}) for v in properties.values() if isinstance(v, dict))
        if not has_const:
            errors.append(
                f"Message '{msg_name}': no property with 'const' found in payload. "
                f"Discriminator field may not have been converted correctly."
            )
```

**Change 3 — topic-specific checks for new types**:

After the generic checks, add per-topic assertions:
```python
# inferred-events: verify 'action' is the discriminator field, verify const values
if topic == "inferred-events" and meta_file.exists():
    meta = json.loads(meta_file.read_text())
    disc_field = meta.get("discriminatorField")
    if disc_field != "action":
        errors.append(
            f"inferred-events: expected discriminatorField 'action', got '{disc_field}'"
        )
    expected_consts = {"ITEM_ADDED", "ITEM_REMOVED", "INVENTORY_ADJUSTED"}
    found_consts = set()
    for msg_def in messages.values():
        props = ((msg_def or {}).get("payload") or {}).get("properties") or {}
        for prop_schema in props.values():
            if isinstance(prop_schema, dict) and "const" in prop_schema:
                found_consts.add(prop_schema["const"])
    missing = expected_consts - found_consts
    if missing:
        errors.append(f"inferred-events: missing const values in spec: {sorted(missing)}")

# untyped-events: verify exactly one schema file named <topic>.json
if topic == "untyped-events" and result_type == "SINGLE_TYPE":
    if len(schema_files) != 1:
        errors.append(
            f"untyped-events: expected exactly 1 schema file, found {len(schema_files)}: "
            f"{[f.name for f in schema_files]}"
        )
    elif schema_files[0].stem != topic:
        errors.append(
            f"untyped-events: schema file must be named '{topic}.json', "
            f"got '{schema_files[0].name}'"
        )

# shape-events: verify IMPLICIT_SHAPE with >= 2 cluster signatures, spec has >= 2 messages
if topic == "shape-events" and result_type == "IMPLICIT_SHAPE":
    if meta_file.exists():
        meta = json.loads(meta_file.read_text())
        sigs = meta.get("clusterSignatures", [])
        if len(sigs) < 2:
            errors.append(
                f"shape-events: expected >= 2 clusterSignatures in metadata.json, got {len(sigs)}"
            )
    if len(messages) < 2:
        errors.append(
            f"shape-events: expected >= 2 messages in spec, got {len(messages)}"
        )
```

**Change 4 — add `--merged-spec` argument and merged spec validation**:

In `main()`:
```python
parser.add_argument("--merged-spec", required=False, type=Path, default=None)
```

After the per-topic loop:
```python
if args.merged_spec:
    merged_errors = validate_merged_spec(args.merged_spec)
    if merged_errors:
        print(f"  FAIL  merged spec ({args.merged_spec.name}):")
        for e in merged_errors:
            print(f"          - {e}")
        overall_pass = False
    else:
        print(f"  PASS  merged spec: {args.merged_spec.name}")
```

New function:
```python
def validate_merged_spec(spec_file: Path) -> list[str]:
    errors = []
    if not spec_file.exists():
        return [f"File not found: {spec_file}"]
    try:
        spec = yaml.safe_load(spec_file.read_text())
    except yaml.YAMLError as e:
        return [f"Invalid YAML: {e}"]

    if spec.get("asyncapi") != "3.0.0":
        errors.append(f"Expected asyncapi: 3.0.0, got: {spec.get('asyncapi')}")

    channels   = spec.get("channels")   or {}
    operations = spec.get("operations") or {}
    messages   = (spec.get("components") or {}).get("messages") or {}

    if len(channels) != 6:
        errors.append(f"Expected 6 channels, got {len(channels)}: {list(channels.keys())}")

    if len(operations) != 6:
        errors.append(f"Expected 6 operations, got {len(operations)}: {list(operations.keys())}")

    # No duplicate message keys
    msg_keys = list(messages.keys())
    duplicates = [k for k in set(msg_keys) if msg_keys.count(k) > 1]
    if duplicates:
        errors.append(f"Duplicate message keys in merged spec: {duplicates}")

    return errors
```

**Demo**:
```bash
python3 scripts/validate-asyncapi.py \
  --asyncapi-dir asyncapi-specs \
  --inferred-schemas-dir inferred-schemas \
  --merged-spec asyncapi-specs/all-kafka-events.yaml
# Expected: PASS for all 6 topics + merged spec
```

---

### Task 7: Update scripts/validate-and-report.py

Two small changes:

**Change 1 — filter metadata.json from ground-truth schema scan**:

In `validate_all()`, in the inner `for schema_file in sorted(topic_dir.glob("*.json")):` loop,
add at the top:
```python
if schema_file.name == "metadata.json":
    continue
```

**Change 2 — no other changes needed**. With correct ground-truth filenames from Task 1
(`cardNumber.json`, `accountNumber.json`, `untyped-events.json`), the existing filename-matching
logic works for all 6 topics without modification.

**Demo**:
```bash
python3 scripts/validate-and-report.py \
  --known-schemas schemas/ \
  --inferred-schemas inferred-schemas/ \
  --output reports/report.html \
  --sample-size 100
# Expected: all 6 topics PASS or WARNING (not FAIL), report.html created
```

---

## Batch C — Kotlin services (independent from Batch A and B)

### Task 8: Update producer

**`producer/src/main/kotlin/com/example/producer/service/EventProducerService.kt`**

Add entries to `TOPIC_MAP` and a new `publishToTopic` method:

```kotlin
companion object {
    private val TOPIC_MAP = mapOf(
        "ORDER_CREATED"         to "order-events",
        "ORDER_SHIPPED"         to "order-events",
        "ORDER_CANCELLED"       to "order-events",
        "PAYMENT_INITIATED"     to "payment-events",
        "PAYMENT_COMPLETED"     to "payment-events",
        "PAYMENT_FAILED"        to "payment-events",
        "USER_REGISTERED"       to "user-events",
        "USER_UPDATED"          to "user-events",
        "USER_DELETED"          to "user-events",
        "ITEM_ADDED"            to "inferred-events",
        "ITEM_REMOVED"          to "inferred-events",
        "INVENTORY_ADJUSTED"    to "inferred-events",
        // HEARTBEAT, CARD_PAYMENT, BANK_TRANSFER are routed via /api/events/raw, not TOPIC_MAP.
        // They have no discriminator field so cannot be routed by payload inspection.
    )
}

fun publish(eventType: String, event: JsonNode) {
    val topic = TOPIC_MAP[eventType]
        ?: throw IllegalArgumentException("Unknown eventType: $eventType")
    kafkaTemplate.send(topic, eventType, objectMapper.writeValueAsString(event))
}

fun publishToTopic(topic: String, payload: JsonNode) {
    kafkaTemplate.send(topic, objectMapper.writeValueAsString(payload))
}
```

**`producer/src/main/kotlin/com/example/producer/controller/EventController.kt`**

Two changes:

```kotlin
@PostMapping("/bulk")
fun publishBulk(@RequestBody events: List<JsonNode>): Map<String, Any> {
    var count = 0
    events.forEach { event ->
        // Try eventType first (existing 9 types), fall back to action (inferred-events types).
        val eventType = event.get("eventType")?.asText()
            ?: event.get("action")?.asText()
            ?: return@forEach
        service.publish(eventType, event)
        count++
    }
    return mapOf("status" to "ok", "published" to count)
}

@PostMapping("/raw")
fun publishRaw(
    @RequestParam topic: String,
    @RequestBody events: List<JsonNode>
): Map<String, Any> {
    events.forEach { service.publishToTopic(topic, it) }
    return mapOf("status" to "ok", "published" to events.size)
}
```

**Test**: `./gradlew build` in `producer/` — compiles without errors.

---

### Task 9: Update consumer

Create 3 new listener files following the exact pattern of
`consumer/src/main/kotlin/com/example/consumer/listener/OrderEventListener.kt`.

**`InferredEventListener.kt`**:
```kotlin
package com.example.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class InferredEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["inferred-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        try {
            val node = mapper.readTree(message)
            log.info("inferred-events action={}: {}", node.get("action")?.asText(), message.take(120))
        } catch (_: Exception) {
            log.info("inferred-events: {}", message.take(120))
        }
    }
}
```

**`UntypedEventListener.kt`**:
```kotlin
package com.example.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class UntypedEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["untyped-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        try {
            val node = mapper.readTree(message)
            log.info("untyped-events serviceId={}: {}", node.get("serviceId")?.asText(), message.take(120))
        } catch (_: Exception) {
            log.info("untyped-events: {}", message.take(120))
        }
    }
}
```

**`ShapeEventListener.kt`**:
```kotlin
package com.example.consumer.listener

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class ShapeEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = ObjectMapper()
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["shape-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        try {
            val node = mapper.readTree(message)
            val shape = when {
                node.has("cardNumber")    -> "card-payment"
                node.has("routingNumber") -> "bank-transfer"
                else                      -> "unknown"
            }
            log.info("shape-events shape={}: {}", shape, message.take(120))
        } catch (_: Exception) {
            log.info("shape-events: {}", message.take(120))
        }
    }
}
```

**`StatusController.kt`** — inject 3 new listeners and add their counts:
```kotlin
@RestController
@RequestMapping("/api/status")
class StatusController(
    private val orderListener:    OrderEventListener,
    private val paymentListener:  PaymentEventListener,
    private val userListener:     UserEventListener,
    private val inferredListener: InferredEventListener,
    private val untypedListener:  UntypedEventListener,
    private val shapeListener:    ShapeEventListener,
) {
    @GetMapping
    fun status() = mapOf(
        "status" to "up",
        "received" to mapOf(
            "order-events"    to orderListener.count.get(),
            "payment-events"  to paymentListener.count.get(),
            "user-events"     to userListener.count.get(),
            "inferred-events" to inferredListener.count.get(),
            "untyped-events"  to untypedListener.count.get(),
            "shape-events"    to shapeListener.count.get(),
        )
    )
}
```

**Test**: `./gradlew build` in `consumer/` — compiles without errors.

---

## Batch D — Integration tests (requires Tasks 8+9 to build successfully)

### Task 10: Smoke-test the new endpoints

After starting the full stack, verify the two new routing paths work before running kafka-spy.

```bash
# Test action-based routing via bulk
curl -sf -X POST http://localhost:8081/api/events/bulk \
  -H "Content-Type: application/json" \
  -d '[{"action":"ITEM_ADDED","itemId":"test-id","name":"Widget","quantity":5,"category":"electronics"}]'
# Expect: {"status":"ok","published":1}

# Test raw topic routing (HEARTBEAT → untyped-events)
curl -sf -X POST "http://localhost:8081/api/events/raw?topic=untyped-events" \
  -H "Content-Type: application/json" \
  -d '[{"serviceId":"svc-001","timestamp":"2024-01-01T00:00:00Z","healthy":true,"uptimeSeconds":3600}]'
# Expect: {"status":"ok","published":1}

# Test raw topic routing (shape-events)
curl -sf -X POST "http://localhost:8081/api/events/raw?topic=shape-events" \
  -H "Content-Type: application/json" \
  -d '[{"cardNumber":"4111111111111111","expiry":"12/26","cvv":"123","amount":99.99}]'
# Expect: {"status":"ok","published":1}

# Verify consumer received all
curl -s http://localhost:8082/api/status | python3 -m json.tool
# Expect: counts > 0 for inferred-events, untyped-events, shape-events
```

---

## Batch E — Skills (can be done in parallel with Batches A–D)

### Task 11: Update the /run-kafka-spy-test skill

File: `.claude/commands/run-kafka-spy-test.md`

**Step 4 (cache decision)** — extend event type list from 9 to 15:
```
ORDER_CREATED ORDER_SHIPPED ORDER_CANCELLED
PAYMENT_INITIATED PAYMENT_COMPLETED PAYMENT_FAILED
USER_REGISTERED USER_UPDATED USER_DELETED
ITEM_ADDED ITEM_REMOVED INVENTORY_ADJUSTED
HEARTBEAT
CARD_PAYMENT BANK_TRANSFER
```

Publishing note: `publish_events.py` handles routing automatically. ITEM_ADDED/REMOVED/ADJUSTED
go to `/api/events/bulk` (action field routing). HEARTBEAT goes to
`/api/events/raw?topic=untyped-events`. CARD_PAYMENT/BANK_TRANSFER go to
`/api/events/raw?topic=shape-events`. No change needed to the publish invocation — the script
does the right thing based on `RAW_TOPICS`.

**Step 5 (kafka-spy)** — replace the 3-topic loop with 6 individual invocations:

```bash
rm -rf {REPO_ROOT}/inferred-schemas

# Existing 3 topics: explicit discriminator (probe phase skipped → SINGLE_TYPE in metadata.json)
for TOPIC in order-events payment-events user-events; do
  echo "Spying on $TOPIC (explicit discriminator) ..."
  java -jar {SPECMATIC_JAR} kafka-spy \
    --broker localhost:9092 \
    --topic "$TOPIC" \
    --discriminator eventType \
    --sample-size {SAMPLE_SIZE} \
    --offset beginning \
    "{REPO_ROOT}/inferred-schemas/$TOPIC/"
done

# New 3 topics: no discriminator — auto-inference runs
echo "Spying on inferred-events (auto-inference expected: EXPLICIT_SINGLE / action) ..."
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic inferred-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/inferred-events/"

echo "Spying on untyped-events (auto-inference expected: SINGLE_TYPE) ..."
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic untyped-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/untyped-events/"

echo "Spying on shape-events (auto-inference expected: IMPLICIT_SHAPE) ..."
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic shape-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/shape-events/"
```

**Step 5b (new) — auto-discovery smoke test**:

Write to a separate root (`auto-discovered-schemas/`), NOT inside `inferred-schemas/`, to avoid
polluting the metadata and asyncapi validators.

```bash
echo "Running auto-discovery smoke test ..."
rm -rf {REPO_ROOT}/auto-discovered-schemas

java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --probe-count 30 \
  --probe-duration-ms 15000 \
  --sample-size 10 \
  --offset beginning \
  "{REPO_ROOT}/auto-discovered-schemas/"

DISCOVERED=$(ls -d {REPO_ROOT}/auto-discovered-schemas/*/ 2>/dev/null | wc -l)
if [ "$DISCOVERED" -ge 6 ]; then
  echo "Auto-discovery PASS: $DISCOVERED topics discovered"
else
  echo "Auto-discovery FAIL: expected >= 6 topics, got $DISCOVERED"
fi
```

**Step 5c (new) — validate metadata.json**:

```bash
echo "Validating metadata.json for all 6 topics ..."
python3 {REPO_ROOT}/scripts/validate-metadata.py \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas

METADATA_EXIT=$?
```

Capture exit code. If non-zero, print "Metadata validation FAILED" and continue (do not abort).

**Step 6 (AsyncAPI generation)** — replace 3-topic loop with 6 + merged:

```bash
rm -rf {REPO_ROOT}/asyncapi-specs
mkdir -p {REPO_ROOT}/asyncapi-specs

# All 6 per-topic specs
for TOPIC in order-events payment-events user-events \
             inferred-events untyped-events shape-events; do
  echo "Generating AsyncAPI spec for $TOPIC ..."
  java -jar {SPECMATIC_JAR} kafka-asyncapi \
    --config "{REPO_ROOT}/config/$TOPIC.yaml" \
    --output-dir "{REPO_ROOT}/asyncapi-specs" \
    "{REPO_ROOT}/inferred-schemas/$TOPIC/"
done

# Merged spec
echo "Generating merged AsyncAPI spec ..."
java -jar {SPECMATIC_JAR} kafka-asyncapi-merged \
  --config "{REPO_ROOT}/config/merged.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs" \
  "{REPO_ROOT}/inferred-schemas/"
```

**Step 7 (validate-asyncapi)** — add `--merged-spec` argument:

```bash
python3 {REPO_ROOT}/scripts/validate-asyncapi.py \
  --asyncapi-dir         {REPO_ROOT}/asyncapi-specs \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas \
  --merged-spec          {REPO_ROOT}/asyncapi-specs/all-kafka-events.yaml
```

**Step 8 (validate-and-report)** — unchanged invocation; script auto-discovers topics from
`schemas/` dir.

**Final summary** — add metadata validation status:
```
=== AsyncAPI specs: {REPO_ROOT}/asyncapi-specs/ ===
AsyncAPI:  PASS ✅  (or FAIL ❌ if Step 7 exit code was 1)
Metadata:  PASS ✅  (or FAIL ❌ if Step 5c exit code was 1)
```

---

### Task 12: Update the /generate-events skill

File: `.claude/commands/generate-events.md`

- Change docstring from "9 event types" to "15 event types"
- Extend the "for each event type" loop to 15 types (same logic, just more entries)
- Update publishing section to list 6 topic groups including the new 3 and note that the script
  dispatches HEARTBEAT/CARD_PAYMENT/BANK_TRANSFER to `/api/events/raw` automatically
- Add 6 new schema reference blocks (copy structure from existing blocks; use the same field
  definitions as the ground-truth schemas in Task 1)

---

## Batch F — README (last, no code dependencies)

### Task 13: Update README.md

**Topics and Event Types table** — add 3 new rows:

| Topic | Event Types | Discriminator field | Auto-inferred? |
|---|---|---|---|
| `inferred-events` | `ITEM_ADDED`, `ITEM_REMOVED`, `INVENTORY_ADJUSTED` | `action` | Yes — `ExplicitSingle` |
| `untyped-events` | _(single structure)_ | _(none)_ | Yes — `NoVariants → SingleType` |
| `shape-events` | _(card shape: `cardNumber.json`, bank shape: `accountNumber.json`)_ | _(structural)_ | Yes — `ImplicitShape` |

Note in the shape-events row: schema files are named after the first alphabetical unique field of
each structural cluster, not after the business concept.

**Step-by-step section**:
- Step 5: describe probe phase, 6 separate kafka-spy invocations
- Step 5b: auto-discovery smoke test
- Step 5c: metadata validation
- Step 6: 6 × kafka-asyncapi + kafka-asyncapi-merged

**Add "Auto-Discriminator Inference" section** explaining:
- Probe phase mechanics (`--probe-count`, `--probe-duration-ms`)
- The 5 result types and which test topic exercises each
- Why `SINGLE_TYPE` appears for both the explicitly-discriminated topics (probe skipped, null
  result → SINGLE_TYPE) and for `untyped-events` (NoVariants → SINGLE_TYPE)

**Add "Merged AsyncAPI Spec" section** explaining `kafka-asyncapi-merged` and the output file.

**Add "ImplicitShape Schema File Naming" section** (or a note in Troubleshooting):
The schema files for `shape-events` are named `cardNumber.json` and `accountNumber.json`
because `DiscriminatorInferrer.deriveImplicitShape()` sets `schemaFile = "${signaturePaths.first()}.json"`.
This means the file name is the alphabetically first field that is unique to that structural
cluster. The ground-truth schemas in `schemas/shape-events/` are named to match.

**Troubleshooting**:
- "shape-events produces only 1 schema file (not 2)" — ImplicitShape clusters weren't detected;
  increase sample size so both card-payment and bank-transfer shapes each have ≥ 5 probe records
- "inferred-events metadata.json says SINGLE_TYPE instead of EXPLICIT_SINGLE" — all probe
  messages had the same `action` value; increase `--probe-count` or ensure generate_events
  produces all 3 action values before the probe phase ends
- "validate-metadata.py reports SINGLE_TYPE for order/payment/user-events as PASS" — this is
  correct and expected; those topics are run with explicit `--discriminator eventType`, which
  skips the probe phase and writes SINGLE_TYPE to metadata.json

---

## Acceptance Criteria

Running `/run-kafka-spy-test 100 --report` must:

1. Publish 1500 total events: 100 × 9 original + 100 × 3 inferred + 100 × 1 heartbeat + 100 × 2 shape
2. Run kafka-spy 6 times (3 with `--discriminator`, 3 without)
3. Produce `inferred-schemas/<topic>/` directories for all 6 topics, each containing `metadata.json`
4. `validate-metadata.py` prints PASS for all 6 topics
5. `inferred-schemas/inferred-events/metadata.json` has `discriminatorField: "action"`
6. `inferred-schemas/untyped-events/` contains exactly `untyped-events.json` and `metadata.json`
7. `inferred-schemas/shape-events/` contains `cardNumber.json`, `accountNumber.json`, and `metadata.json` with `IMPLICIT_SHAPE`
8. Auto-discovery smoke test passes (≥ 6 subdirectories in `auto-discovered-schemas/`)
9. Run kafka-asyncapi 6 times + kafka-asyncapi-merged once
10. `asyncapi-specs/` contains 7 files: one per topic + `all-kafka-events.yaml`
11. `validate-asyncapi.py` PASS for all 6 per-topic specs + the merged spec
12. `reports/report.html` shows PASS or WARNING (not FAIL) for all 6 topics
