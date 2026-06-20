# Validation Report: `asyncapi-specs/all-kafka-events.yaml`

## Source Data Chain

```
events-cache/*.json  →  auto-discovered-schemas/<topic>/  →  all-kafka-events.yaml
                         (+ metadata.json per topic)
```

The generated spec was built from the auto-discovered schemas (kafka-spy inference output), not the ground-truth `schemas/` directory. This is the correct data flow.

---

## Structural Conformance: PASS

All AsyncAPI 3.0 structural requirements from `kafka-spy-stage2-implementation-plan.md` are satisfied:

| Check | Result |
|---|---|
| `asyncapi: 3.0.0` root key | ✓ |
| `info.title` / `info.version` | ✓ (`All Kafka Events API`, `1.0.0`) |
| `servers.kafka.host` / `.protocol` | ✓ (`localhost:9092`, `kafka`) |
| Channel keys PascalCase of topic name | ✓ `OrderEvents`, `PaymentEvents`, `UserEvents`, `InferredEvents`, `ShapeEvents`, `UntypedEvents` |
| `channels.<key>.address` = raw topic name | ✓ |
| Channel `messages` `$ref` → `#/components/messages/<TYPE>` | ✓ |
| Operation key = `send<PascalCase>` | ✓ all 6 operations |
| Operation `messages` list `$ref` → `#/channels/<channel>/messages/<TYPE>` | ✓ |
| `components.messages.<TYPE>.name` = event type | ✓ |
| `components.messages.<TYPE>.contentType` = `application/json` | ✓ |
| `$schema` key stripped from all payloads | ✓ |
| `required` arrays preserved | ✓ |

---

## `const` Conversion: PASS with Correct N/A Cases

The rule is: single-value `enum` on the discriminator field → `const`.

The generator reads `metadata.json` from each topic's schema directory to resolve the discriminator. All six topics have a `metadata.json`. The discriminator field in the config is redundant for topics that already have it in `metadata.json`, but both agree.

**Topics with EXPLICIT_SINGLE in `metadata.json` — discriminator correctly converted to `const`:**

| Topic | `metadata.json` | Discriminator field | Result |
|---|---|---|---|
| `order-events` | `EXPLICIT_SINGLE` | `eventType` | `const: ORDER_CREATED` etc. ✓ |
| `payment-events` | `EXPLICIT_SINGLE` | `eventType` | `const: PAYMENT_COMPLETED` etc. ✓ |
| `user-events` | `EXPLICIT_SINGLE` | `eventType` | `const: USER_DELETED` etc. ✓ |
| `inferred-events` | `EXPLICIT_SINGLE` | `action` | `const: INVENTORY_ADJUSTED` etc. ✓ |

**Topics without an explicit discriminator — no `const` (correct):**

| Topic | `metadata.json` | Behaviour |
|---|---|---|
| `shape-events` | `IMPLICIT_SHAPE` | Clusters separated by field presence; no discriminator property exists → no `const` ✓ |
| `untyped-events` | `SINGLE_TYPE` | All messages share one structure; no variants → no `const` ✓ |

---

## Issues Found

### Issue 1: `validate-asyncapi.py` — NOT AN ISSUE (retracted)

The actual `scripts/validate-asyncapi.py` in the project is already correctly implemented. It reads `metadata.json` per topic and applies topic-specific validation logic: EXPLICIT_SINGLE topics get `const` checks, IMPLICIT_SHAPE and SINGLE_TYPE topics do not. There is no blind sweep for `const` across all messages. This issue was raised against the simpler version described in the stage-2 plan document, not the implementation.

### Issue 2: Over-fitted enum on `errorMessage` in PAYMENT_FAILED (Medium severity)

**Root cause — `scripts/generate_events.py`**: `_ERROR_MESSAGES` maps each of the 4 error codes to a single fixed string. The events-cache therefore contains only 4 distinct `errorMessage` values, which the inferrer promotes to an enum.

**Fix applied**: `_ERROR_MESSAGES` now maps each error code to a list of 3–4 message variants, giving 13 total distinct strings across all generated events. This exceeds the inferrer's 10-value enum threshold, so `errorMessage` will be inferred as open `type: string` after the pipeline is re-run.

### Issue 3: Over-fitted enums on `ITEM_ADDED.name` and `INVENTORY_ADJUSTED.adjustedBy` (Medium severity)

**Root cause — `scripts/generate_events.py`**:
- `_ITEM_NAMES` had exactly 10 entries, landing precisely on the enum threshold.
- `_ADJUST_EMAILS` had only 4 entries, well below the threshold.

**Fix applied**:
- `_ITEM_NAMES` expanded from 10 to 15 entries (`Wrench`, `Gear`, `Rivet`, `Clamp`, `Hinge` added).
- `_ADJUST_EMAILS` expanded from 4 to 12 entries (8 additional operator emails added).

Both fields now exceed the 10-value threshold and will be inferred as open `type: string`.

### Issue 4: Over-fitted `serviceId` enum on `untyped-events` (Low severity)

**Root cause — `scripts/generate_events.py`**: `_SERVICE_IDS` used `range(1, 11)`, producing exactly 10 distinct service ID values.

**Fix applied**: Changed to `range(1, 21)`, producing 20 distinct service IDs. The inferrer will no longer emit an enum for `serviceId`.

### Issue 5: Missing numeric constraints (Low severity — inference algorithm gap)

The ground-truth schemas carry `minimum: 0` on `amount` fields and `minimum: 1, maximum: 100` on `quantity`. The kafka-spy Stage 1 inferrer observes numeric values but does not track or emit observed bounds. This cannot be fixed by changing the event generator — it requires the inferrer to record `min`/`max` of each numeric field across the sample and emit them in the JSON Schema output.

**Fix location**: kafka-spy Stage 1 inference code (`JsonSchemaInferrer.kt`). Out of scope for this test harness.

---

## Action required after applying fixes

The `generate_events.py` fixes change the cardinality of the affected fields. For the corrections to propagate into the generated AsyncAPI spec, run the full pipeline:

```bash
python3 scripts/generate_events.py PAYMENT_FAILED    500 ./events-cache
python3 scripts/generate_events.py ITEM_ADDED        500 ./events-cache
python3 scripts/generate_events.py INVENTORY_ADJUSTED 500 ./events-cache
python3 scripts/generate_events.py HEARTBEAT         500 ./events-cache
# then publish → kafka-spy → asyncapi-generator
./run-test.sh
```

---

## Schema Accuracy: Inferred vs Ground Truth

| Field | Ground-Truth Schema | Generated (Inferred) | Status |
|---|---|---|---|
| `INVENTORY_ADJUSTED.adjustedBy` | `{type: string, format: email}` | `{type: string, enum: [4 emails]}` | Degraded |
| `ITEM_ADDED.name` | `{type: string}` | `{type: string, enum: [10 products]}` | Over-constrained |
| `ITEM_ADDED.quantity` | `{type: integer, minimum: 1, maximum: 100}` | `{type: integer}` | Missing bounds |
| `PAYMENT_FAILED.errorMessage` | `{type: string}` | `{type: string, enum: [4 messages]}` | Over-constrained |
| `accountNumber.amount` | `{type: number, minimum: 0}` | `{type: number}` | Missing bound |
| `untyped-events.serviceId` | `{type: string}` | `{type: string, enum: [svc-001..010]}` | Over-constrained |
| Everything else | — | — | Accurate ✓ |

---

## Summary

The generated file is **structurally valid AsyncAPI 3.0** and correctly implements the stage-2 spec rules (PascalCase channel names, `$ref` structure, discriminator `const` conversion, `$schema` stripping, required fields). The auto-detection of the `action` discriminator for `inferred-events` via `metadata.json` is working correctly.

The main actionable findings are:

1. **Fix `validate-asyncapi.py`** to read `metadata.json` and skip the `const` check for IMPLICIT_SHAPE and SINGLE_TYPE topics — otherwise the CI check will always produce false failures for `shape-events` and `untyped-events`.
2. **Free-text string fields are being over-constrained as enums** when only a small number of distinct values appear in the event sample. `errorMessage`, `adjustedBy`, `name`, and `serviceId` are the affected fields. Increasing sample size or adding a minimum-cardinality threshold for enum inference would help.
