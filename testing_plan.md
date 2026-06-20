You are updating the **`kafka-spy-manual-test`** project — a manual end-to-end test harness for `specmatic kafka-spy`. The harness lives at `~/workspace/github-personal/kafka-spy-manual-test/` and is a sibling of `~/workspace/github-personal/specmatic/`.

The `specmatic` repo has just had three commits land with significant new features. Your job is to extend the test harness to exercise all the edge cases of those new features via live Kafka traffic. **Do not change the specmatic source repo — only modify the test harness.**

---

## What was implemented in specmatic (context only — read-only)

### 1. Auto-discriminator inference (`DiscriminatorInferrer`)

The `--discriminator` flag on `kafka-spy` is now **optional**. When omitted, the spy runs a **probe phase** (collecting up to `--probe-count` records per topic within `--probe-duration-ms` ms), then calls `DiscriminatorInferrer.infer(payloads)` per topic to auto-detect the discriminator. Inference returns one of five sealed subtypes:

| Result type | Meaning |
|---|---|
| `ExplicitSingle` | A single field (e.g. `eventType`, `action`, or nested `payload.kind`) cleanly separates all event types |
| `ExplicitComposite` | Two fields combined separate event types (e.g. `channel` + `subtype`) |
| `ImplicitShape` | No single field works but event types differ by which top-level fields are present (structural discrimination) |
| `NoVariants` | All messages share one structure — single event type, no discriminator needed |
| `None` | Multiple structural clusters exist but no field is clean enough to act as discriminator |

The inference result is **persisted** to `metadata.json` in the topic's output directory (written by `SpyMetadataWriter`).

### 2. Multi-topic support

`KafkaSpyConfig` changed from a single `topic: String` + mandatory `discriminatorField: String` to:
- `topics: List<String>` — multiple topics in one run
- `discriminatorField: String?` — optional; `null` triggers auto-inference
- `probeCount: Int = 100` — records per topic for probe phase
- `probeDurationMs: Long = 15_000` — max ms for probe phase

`KafkaSpy.run()` now writes per-topic subdirectories under the output root: `<output-root>/<topic-name>/`.

### 3. Topic auto-discovery

`kafka-spy --broker localhost:9092` without `--topic` now auto-discovers all non-internal topics from the broker via `TopicDiscoverer` (Kafka AdminClient).

### 4. Metadata file round-trip

`SpyMetadataWriter` writes `metadata.json` in each topic directory after spy completes. `SpyMetadataReader` reads it back when `kafka-asyncapi` is run later without `--discriminator`, so const conversion still works.

`metadata.json` format by result type:
```json
// ExplicitSingle:
{"discriminatorResultType":"EXPLICIT_SINGLE","discriminatorField":"eventType"}

// ExplicitComposite:
{"discriminatorResultType":"EXPLICIT_COMPOSITE","discriminatorFields":["channel","subtype"]}

// ImplicitShape:
{"discriminatorResultType":"IMPLICIT_SHAPE","clusterSignatures":[...]}

// NoVariants / None:
{"discriminatorResultType":"SINGLE_TYPE"}
```

### 5. New `kafka-asyncapi-merged` command

```bash
java -jar specmatic.jar kafka-asyncapi-merged \
  --config config/merged.yaml \
  --output-dir asyncapi-specs \
  inferred-schemas/
```

Reads immediate subdirectories of `inferred-schemas/` (each is a topic name), generates a **single** merged AsyncAPI 3.0 YAML (`asyncapi-specs/merged.yaml`) with all topics, channels, operations, and messages combined. When two topics share an event-type name, it automatically prefixes all message keys with the channel name to avoid collisions.

Config file format (`config/merged.yaml`):
```yaml
broker: localhost:9092
discriminator: eventType        # optional — if omitted, reads metadata.json per topic
operation: send
title: All Topics API
version: 1.0.0
contentType: application/json
output-file: merged.yaml        # optional — derived from title if absent
```

### 6. `kafka-asyncapi` — `--discriminator` now optional

When `--discriminator` is omitted, the command reads `metadata.json` in the schemas directory. If the result type is `EXPLICIT_SINGLE`, it uses that field for const conversion. For all other result types it skips const conversion (schemas are used as-is).

### 7. `SchemaFileWriter` — `SingleType` file naming

For topics where the router is `SingleType` (from `NoVariants` or `None`), the schema file is named `<topicName>.json` (not `default.json`).

### 8. `StringExtensions`

Two top-level extension functions are now available (used internally by `kafka-asyncapi` and `kafka-asyncapi-merged`):
- `String.toPascalCase()` — `"order-events"` → `"OrderEvents"`
- `String.toHyphenatedFileStem()` — `"All Topics API"` → `"all-topics-api"`

---

## Current harness state (what already exists)

The harness at `~/workspace/github-personal/kafka-spy-manual-test/` tests **three topics** with an **explicit `--discriminator eventType`** flag on every `kafka-spy` and `kafka-asyncapi` call:

- **Topics**: `order-events`, `payment-events`, `user-events`
- **9 event types** (3 per topic): `ORDER_CREATED/SHIPPED/CANCELLED`, `PAYMENT_INITIATED/COMPLETED/FAILED`, `USER_REGISTERED/UPDATED/DELETED`
- All events have a top-level `eventType` field (the discriminator)
- Producer: Spring Boot on port 8081, `POST /api/events/bulk`
- Consumer: Spring Boot on port 8082, `GET /api/status`
- Skills: `/run-kafka-spy-test [N] [--fresh] [--report]` and `/generate-events [N] [--fresh]`
- Scripts: `scripts/generate_events.py`, `scripts/publish_events.py`, `scripts/validate-asyncapi.py`, `scripts/validate-and-report.py`
- Config: `config/order-events.yaml`, `config/payment-events.yaml`, `config/user-events.yaml`
- Ground-truth schemas: `schemas/order-events/`, `schemas/payment-events/`, `schemas/user-events/`

---

## What you must add / change

### A. Add three new Kafka topics

Add these three topics to the harness to exercise the new discriminator inference modes:

#### Topic 1: `inferred-events`
**Purpose**: test auto-detection of a discriminator that is NOT named `eventType`. Events on this topic use a top-level `action` field as the discriminator. `kafka-spy` must be run **without** `--discriminator` and must correctly infer `action` as `ExplicitSingle`.

Event types:
- `ITEM_ADDED` — e.g. `{ "action": "ITEM_ADDED", "itemId": "uuid", "name": "Widget", "quantity": 5, "category": "electronics" }`
- `ITEM_REMOVED` — e.g. `{ "action": "ITEM_REMOVED", "itemId": "uuid", "reason": "out-of-stock" }`
- `INVENTORY_ADJUSTED` — e.g. `{ "action": "INVENTORY_ADJUSTED", "itemId": "uuid", "delta": -10, "adjustedBy": "admin@example.com" }`

All three event types share `action` and `itemId` but differ in additional fields. The `action` field value is a short uppercase token, making it a clean discriminator candidate.

#### Topic 2: `untyped-events`
**Purpose**: test the `NoVariants` path — all messages have the exact same structure. `kafka-spy` auto-detects no discriminator needed (`NoVariants` → `SingleType` router). Schema file should be named `untyped-events.json` (not `default.json`). `metadata.json` should have `discriminatorResultType: SINGLE_TYPE`.

Event structure (only one type — no discriminator field at all):
- `HEARTBEAT` — e.g. `{ "serviceId": "svc-123", "timestamp": "2024-01-01T00:00:00Z", "healthy": true, "uptimeSeconds": 3600 }`

All messages share the same top-level fields: `serviceId`, `timestamp`, `healthy`, `uptimeSeconds`. No field distinguishes subtypes.

#### Topic 3: `shape-events`
**Purpose**: test `ImplicitShape` — events differ by which top-level fields are present (structural discrimination), with no shared string-token field to act as discriminator. Two structurally distinct clusters:

- Cluster A (`card-payment` shape): `{ "cardNumber": "4111...", "expiry": "12/25", "cvv": "123", "amount": 99.99 }`
- Cluster B (`bank-transfer` shape): `{ "routingNumber": "021000021", "accountNumber": "1234567", "bankName": "First National", "amount": 150.00 }`

The two shapes share only `amount`. Inference should return `ImplicitShape` with two cluster signatures.

---

### B. Add these event types to the producer and consumer

#### In `producer/src/main/kotlin/com/example/producer/service/EventProducerService.kt`

Extend `TOPIC_MAP` to include the new event types:

```kotlin
"ITEM_ADDED"            to "inferred-events",
"ITEM_REMOVED"          to "inferred-events",
"INVENTORY_ADJUSTED"    to "inferred-events",
"HEARTBEAT"             to "untyped-events",
"CARD_PAYMENT"          to "shape-events",
"BANK_TRANSFER"         to "shape-events",
```

Note: `CARD_PAYMENT` and `BANK_TRANSFER` are bucket names used internally — the actual JSON messages have no `eventType`/`action` field. The producer must publish these by routing on the bucket key **without** requiring `eventType` in the payload. Either: (a) the producer routes by the request parameter name rather than reading from the JSON body, or (b) the two shapes are posted to a dedicated endpoint `/api/events/shape` that always routes to `shape-events`. Choose whichever is simpler — a dedicated endpoint for shape events is cleaner.

#### In `consumer/`

Add Kafka listeners for the three new topics:
- `inferred-events` → log each record's `action` field and count
- `untyped-events` → log each record's `serviceId` and count
- `shape-events` → log whether the record has `cardNumber` or `routingNumber` and count

Expose counts in the existing `GET /api/status` endpoint.

---

### C. Update `scripts/generate_events.py`

The script currently generates events for the 9 existing event types. Add support for:

- `ITEM_ADDED` — generate realistic inventory add events with varied `itemId` (UUIDs), `name` (product names), `quantity` (1–100), `category` (one of: `"electronics"`, `"clothing"`, `"food"`, `"sports"`)
- `ITEM_REMOVED` — varied `itemId`, `reason` (one of: `"out-of-stock"`, `"discontinued"`, `"damaged"`, `"recalled"`)
- `INVENTORY_ADJUSTED` — varied `itemId`, `delta` (−50 to +50, excluding 0), `adjustedBy` (email addresses)
- `HEARTBEAT` — varied `serviceId` (e.g. `"svc-001"` through `"svc-010"`), `timestamp` (ISO-8601 strings), `healthy` (`true`/`false`), `uptimeSeconds` (0–86400)
- `CARD_PAYMENT` — NO `eventType` or `action` field. Fields: `cardNumber` (16-digit Luhn-valid), `expiry` (`MM/YY`), `cvv` (3-digit), `amount` (positive float)
- `BANK_TRANSFER` — NO `eventType` or `action` field. Fields: `routingNumber` (9-digit), `accountNumber` (8–12 digit), `bankName` (varied bank names), `amount` (positive float)

**Critical**: `CARD_PAYMENT` and `BANK_TRANSFER` must have **no shared string-token fields** — no `type`, `kind`, `action`, or any field whose value would make a good discriminator. The only shared field is `amount`.

For `HEARTBEAT`: generate at least 50 messages but they **must all have the same top-level fields** (`serviceId`, `timestamp`, `healthy`, `uptimeSeconds`) with varied values. Do not add any field that appears in only some messages.

For `ITEM_ADDED/REMOVED/INVENTORY_ADJUSTED`: generate enough variety in `action` values that inference can cleanly separate the three types. Each type must have ≥ 20 distinct messages for reliable inference.

---

### D. Update `scripts/publish_events.py`

Extend the publisher to handle the 6 new event types. The publish order/grouping by topic:
- `inferred-events`: `ITEM_ADDED` → `ITEM_REMOVED` → `INVENTORY_ADJUSTED`
- `untyped-events`: `HEARTBEAT`
- `shape-events`: `CARD_PAYMENT` → `BANK_TRANSFER`

For `CARD_PAYMENT` and `BANK_TRANSFER`, POST to a new endpoint `POST /api/events/shape` (or whatever endpoint you add in step B) that does not require `eventType` in the payload, routing instead by the endpoint path or a query parameter.

---

### E. Add config files for the three new topics

Create `config/inferred-events.yaml`:
```yaml
broker: localhost:9092
topic: inferred-events
# discriminator intentionally omitted — should be read from metadata.json
operation: send
title: Inferred Events API
version: 1.0.0
contentType: application/json
```

Create `config/untyped-events.yaml`:
```yaml
broker: localhost:9092
topic: untyped-events
# discriminator intentionally omitted
operation: send
title: Untyped Events API
version: 1.0.0
contentType: application/json
```

Create `config/shape-events.yaml`:
```yaml
broker: localhost:9092
topic: shape-events
# discriminator intentionally omitted — ImplicitShape has no field to specify
operation: send
title: Shape Events API
version: 1.0.0
contentType: application/json
```

Create `config/merged.yaml` (for the merged-spec command):
```yaml
broker: localhost:9092
# discriminator intentionally omitted — reads from per-topic metadata.json
operation: send
title: All Kafka Events API
version: 1.0.0
contentType: application/json
output-file: all-kafka-events.yaml
```

---

### F. Add ground-truth schemas

Add ground-truth JSON Schema Draft-07 files for the new event types. These go in `schemas/<topic>/`:

**`schemas/inferred-events/ITEM_ADDED.json`** — must include `action` with `const: "ITEM_ADDED"`, plus `itemId` (string), `name` (string), `quantity` (integer), `category` (string with enum of at least `electronics`, `clothing`, `food`, `sports`).

**`schemas/inferred-events/ITEM_REMOVED.json`** — `action` with `const: "ITEM_REMOVED"`, `itemId` (string), `reason` (string with enum).

**`schemas/inferred-events/INVENTORY_ADJUSTED.json`** — `action` with `const: "INVENTORY_ADJUSTED"`, `itemId` (string), `delta` (integer), `adjustedBy` (string, format: email).

**`schemas/untyped-events/untyped-events.json`** — (named after the topic, not an event type) `serviceId` (string), `timestamp` (string), `healthy` (boolean), `uptimeSeconds` (integer). No `type`/`action` field. This is the single schema file for the whole topic.

**`schemas/shape-events/CARD_PAYMENT.json`** — `cardNumber` (string), `expiry` (string), `cvv` (string), `amount` (number). No `eventType`.

**`schemas/shape-events/BANK_TRANSFER.json`** — `routingNumber` (string), `accountNumber` (string), `bankName` (string), `amount` (number).

---

### G. Update the `/run-kafka-spy-test` skill

The skill lives at `.claude/commands/run-kafka-spy-test.md`. Update it as follows:

#### Step 4 (event generation)
The cache decision and publish logic must cover all 15 event types:
- Existing 9: `ORDER_CREATED`, `ORDER_SHIPPED`, `ORDER_CANCELLED`, `PAYMENT_INITIATED`, `PAYMENT_COMPLETED`, `PAYMENT_FAILED`, `USER_REGISTERED`, `USER_UPDATED`, `USER_DELETED`
- New 6: `ITEM_ADDED`, `ITEM_REMOVED`, `INVENTORY_ADJUSTED`, `HEARTBEAT`, `CARD_PAYMENT`, `BANK_TRANSFER`

#### Step 5 — Run `kafka-spy` (now 6 topics instead of 3)

For the **existing 3 topics** (order-events, payment-events, user-events): run with `--discriminator eventType` as before.

For the **3 new topics**: run **without** `--discriminator` so auto-inference kicks in:

```bash
# inferred-events — should auto-detect 'action' as ExplicitSingle
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic inferred-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/inferred-events/"

# untyped-events — should detect NoVariants → SingleType
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic untyped-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/untyped-events/"

# shape-events — should detect ImplicitShape
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic shape-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/shape-events/"
```

#### Step 5b — Topic auto-discovery test (new step, runs once)

After the per-topic spy runs, add a step that runs `kafka-spy` **without `--topic`** and verifies it discovers topics automatically:

```bash
# Should auto-discover all 6 topics and spy on them
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --probe-count 30 \
  --probe-duration-ms 15000 \
  --sample-size 10 \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/auto-discovered/"
```

Then verify that `inferred-schemas/auto-discovered/` contains 6 subdirectories (one per topic). This is a smoke test — do not validate the schemas from this run; just print PASS/FAIL based on directory count.

#### Step 5c — Verify `metadata.json` (new step, runs before Step 6)

```bash
python3 {REPO_ROOT}/scripts/validate-metadata.py \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas
```

See section I for what this script must check.

#### Step 6 — Generate AsyncAPI specs (now 6 topics + merged)

For the **existing 3 topics**: run `kafka-asyncapi` with `--config config/{TOPIC}.yaml` as before (explicit discriminator in config).

For the **3 new topics**: run `kafka-asyncapi` **without** specifying `--discriminator` (it should read from `metadata.json`):

```bash
java -jar {SPECMATIC_JAR} kafka-asyncapi \
  --config "{REPO_ROOT}/config/inferred-events.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs" \
  "{REPO_ROOT}/inferred-schemas/inferred-events/"

java -jar {SPECMATIC_JAR} kafka-asyncapi \
  --config "{REPO_ROOT}/config/untyped-events.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs" \
  "{REPO_ROOT}/inferred-schemas/untyped-events/"

java -jar {SPECMATIC_JAR} kafka-asyncapi \
  --config "{REPO_ROOT}/config/shape-events.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs" \
  "{REPO_ROOT}/inferred-schemas/shape-events/"
```

After the 6 per-topic runs, run the **merged-spec command**:

```bash
java -jar {SPECMATIC_JAR} kafka-asyncapi-merged \
  --config "{REPO_ROOT}/config/merged.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs" \
  "{REPO_ROOT}/inferred-schemas/"
```

This generates `asyncapi-specs/all-kafka-events.yaml` covering all 6 topics.

#### Step 7 — Validate AsyncAPI specs

Update `scripts/validate-asyncapi.py` (see section H below) and run it with:
```bash
python3 {REPO_ROOT}/scripts/validate-asyncapi.py \
  --asyncapi-dir {REPO_ROOT}/asyncapi-specs \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas \
  --merged-spec {REPO_ROOT}/asyncapi-specs/all-kafka-events.yaml
```

#### Step 8 — Validate schemas (updated)

Pass all 6 topic directories to `validate-and-report.py`:
```bash
python3 {REPO_ROOT}/scripts/validate-and-report.py \
  --known-schemas    {REPO_ROOT}/schemas/ \
  --inferred-schemas {REPO_ROOT}/inferred-schemas/ \
  --output           {REPO_ROOT}/reports/report.html \
  --sample-size      {SAMPLE_SIZE}
```

---

### H. Update `scripts/validate-asyncapi.py`

The current script checks that:
1. The spec has `asyncapi: 3.0.0`
2. All required top-level keys are present
3. Every `*.json` file in the inferred schemas dir has a message entry
4. Every message payload has at least one property with `const`

Extend it with these new checks:

#### H1. Relax the `const` check for non-discriminated topics

For the `untyped-events` and `shape-events` specs, there is no explicit discriminator field converted to `const`. The validator must skip the `const` check for topics whose `metadata.json` says `discriminatorResultType` is `SINGLE_TYPE` or `IMPLICIT_SHAPE`.

Pass `--inferred-schemas-dir` to the script (already present) so it can read `metadata.json` before deciding whether to check for `const`.

Logic:
- If `metadata.json` has `discriminatorResultType: EXPLICIT_SINGLE` → require `const` on one property
- If `discriminatorResultType: SINGLE_TYPE` or `IMPLICIT_SHAPE` or `EXPLICIT_COMPOSITE` → skip `const` check, instead just verify the spec has valid structure

#### H2. Check `inferred-events` uses `action` as discriminator

After running the spy without `--discriminator`, `metadata.json` for `inferred-events` should have:
```json
{"discriminatorResultType":"EXPLICIT_SINGLE","discriminatorField":"action"}
```
The validator should read this and verify:
- The field is `action` (not `eventType`)
- The `inferred-events.yaml` spec contains `const: "ITEM_ADDED"`, `const: "ITEM_REMOVED"`, `const: "INVENTORY_ADJUSTED"` in the three message payloads

#### H3. Check `untyped-events` schema file is named correctly

For `untyped-events`, the inferred schema directory should contain exactly **one** file named `untyped-events.json` (not `default.json`, not `HEARTBEAT.json`). Add a check: if `metadata.json` has `discriminatorResultType: SINGLE_TYPE`, verify that the directory contains exactly one schema file and its stem matches the topic name.

#### H4. Check `shape-events` metadata

For `shape-events`, `metadata.json` should have `discriminatorResultType: IMPLICIT_SHAPE` with at least 2 cluster signatures. Verify this. Also verify the spec has at least 2 messages under `components.messages`.

#### H5. Validate the merged spec

Add a `--merged-spec` argument. When provided, check:
- The file exists and is valid YAML
- `asyncapi: 3.0.0`
- `channels` contains 6 entries (one per topic), each with PascalCase key
- `operations` contains 6 entries
- `components.messages` contains all event types from all 6 topics
- No message key appears twice (verifies collision-free naming for the 6-topic set)

---

### I. Add `scripts/validate-metadata.py` (new script)

Create this script to validate `metadata.json` files written by `kafka-spy`:

```
Usage: python3 scripts/validate-metadata.py --inferred-schemas-dir <dir>
```

For each subdirectory of `<dir>` (each is a topic):
1. Verify `metadata.json` exists
2. Parse it as JSON
3. Verify `discriminatorResultType` is one of: `EXPLICIT_SINGLE`, `EXPLICIT_COMPOSITE`, `IMPLICIT_SHAPE`, `SINGLE_TYPE`
4. For `EXPLICIT_SINGLE`: verify `discriminatorField` key is present and non-empty
5. For `EXPLICIT_COMPOSITE`: verify `discriminatorFields` is an array of exactly 2 non-empty strings
6. For `IMPLICIT_SHAPE`: verify `clusterSignatures` is an array of ≥ 2 objects, each with `schemaFile` (string) and `signaturePaths` (non-empty array)
7. For `SINGLE_TYPE`: no extra keys required

Per-topic expected values (hard-coded in the script):
- `order-events`: spy was run WITH `--discriminator eventType` → `EXPLICIT_SINGLE`, `discriminatorField: "eventType"`
- `payment-events`: same → `EXPLICIT_SINGLE`, `discriminatorField: "eventType"`
- `user-events`: same → `EXPLICIT_SINGLE`, `discriminatorField: "eventType"`
- `inferred-events`: spy was run WITHOUT `--discriminator` → `EXPLICIT_SINGLE`, `discriminatorField: "action"`
- `untyped-events`: spy WITHOUT discriminator → `SINGLE_TYPE`
- `shape-events`: spy WITHOUT discriminator → `IMPLICIT_SHAPE` with ≥ 2 cluster signatures

The script prints `PASS <topic>` or `FAIL <topic>: <reason>` per topic, and exits with code 1 if any topic fails.

---

### J. Update `scripts/validate-and-report.py`

The existing script compares inferred schemas against ground-truth schemas in `schemas/`. Extend it:

1. Handle the `untyped-events` topic where the schema file is named `untyped-events.json` (not an event-type-named file). The script should look for `schemas/untyped-events/untyped-events.json` as ground-truth and compare against `inferred-schemas/untyped-events/untyped-events.json`.

2. Handle the `shape-events` topic where schema files may be named by cluster index (e.g. whatever `ImplicitShape` produces). Compare against `schemas/shape-events/CARD_PAYMENT.json` and `schemas/shape-events/BANK_TRANSFER.json` if those names appear in the inferred dir; otherwise just verify the inferred files have the right field sets.

3. For `inferred-events`, ground-truth schemas use `action` as the discriminator field. The validation should check that the inferred schema has a `const` on `action` (not `eventType`).

---

### K. Update the `/generate-events` skill

The skill at `.claude/commands/generate-events.md` handles the 9 existing event types. Extend it to cover all 15 types (add the 6 new ones to the same cache-based logic). The generate script handles one type at a time; the skill just needs to enumerate all 15.

---

### L. Infrastructure notes

The existing three Kafka topics are auto-created by Kafka when the producer first publishes. No changes are needed to `docker-compose.yml` since `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` is already set. The three new topics (`inferred-events`, `untyped-events`, `shape-events`) will also be auto-created on first publish.

---

### M. Update the README

Update `README.md`:

1. **Topics and Event Types table** — add 3 new rows:

| Topic | Event Types | Discriminator field | Auto-inferred? |
|---|---|---|---|
| `inferred-events` | `ITEM_ADDED`, `ITEM_REMOVED`, `INVENTORY_ADJUSTED` | `action` | Yes — `ExplicitSingle` |
| `untyped-events` | _(single structure)_ | _(none)_ | Yes — `NoVariants/SingleType` |
| `shape-events` | _(card shape, bank shape)_ | _(structural)_ | Yes — `ImplicitShape` |

2. **Step-by-step table** — update Step 5 to mention probe phase and 6 topics, Step 6 to include merged-spec command, and add the new metadata validation step.

3. **New section: "Auto-Discriminator Inference"** — brief explanation of the probe phase, the 5 result types, and when each is used with an example for each of the 3 new topics.

4. **New section: "Merged AsyncAPI Spec"** — explain the `kafka-asyncapi-merged` command and what `asyncapi-specs/all-kafka-events.yaml` contains.

5. **Troubleshooting** — add:
   - "shape-events produces only 1 schema file (not 2)" — means ImplicitShape clusters weren't detected; increase sample size to ensure both card and bank-transfer shapes have ≥ 5 records each
   - "inferred-events metadata.json says SINGLE_TYPE instead of EXPLICIT_SINGLE" — means all probe messages had the same `action` value; increase `--probe-count` or ensure generate_events produces all 3 action values

---

## Acceptance criteria

After your changes, running `/run-kafka-spy-test 100 --report` must:

1. Publish 100 events × 9 original types + 100 × 3 inferred-events types + 100 × 1 heartbeat type + 100 × 2 shape types = 1500 total events
2. Run kafka-spy 6 times (3 with `--discriminator`, 3 without)
3. Produce `inferred-schemas/<topic>/` directories for all 6 topics
4. Verify `metadata.json` for all 6 topics passes `validate-metadata.py` with PASS
5. `inferred-schemas/inferred-events/metadata.json` has `discriminatorField: "action"`
6. `inferred-schemas/untyped-events/` contains exactly one file: `untyped-events.json`
7. `inferred-schemas/shape-events/metadata.json` has `discriminatorResultType: IMPLICIT_SHAPE`
8. Run kafka-asyncapi 6 times + kafka-asyncapi-merged once
9. `asyncapi-specs/` contains `order-events.yaml`, `payment-events.yaml`, `user-events.yaml`, `inferred-events.yaml`, `untyped-events.yaml`, `shape-events.yaml`, `all-kafka-events.yaml`
10. `validate-asyncapi.py` PASS for all 6 per-topic specs + the merged spec
11. `reports/report.html` shows PASS or WARNING (not FAIL) for all 6 topics
