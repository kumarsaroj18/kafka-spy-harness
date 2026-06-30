# Evidence-Based Testing: Discriminator Inference → Schema Inference → AsyncAPI 3.0

This document traces the complete pipeline for all six Kafka topics — from raw event inputs in `input/events-cache/` through kafka-spy's discriminator inference, through JSON schema inference, to the final merged AsyncAPI 3.0 spec — and verifies each step against the requirements in `testing_plan.md`.

---

## Test Run Parameters

- **Sample size**: 500 events per event type
- **Total events**: 7,500 (15 types × 500)
- **Kafka-spy runs**: 6 (3 with `--discriminator eventType`, 3 without)
- **Merged spec**: `output/asyncapi-specs/all-kafka-events.yaml`
- **Source schemas**: `output/inferred-schemas/<topic>/*.json`
- **Metadata files**: `output/inferred-schemas/<topic>/metadata.json`

---

## Part 1: Discriminator Inference Scenarios

These four scenarios exercise the four discriminator result types that appear across the six topics.

---

### Scenario 1: Explicit Discriminator Bypasses Probe Phase (`order-events`, `payment-events`, `user-events`)

**Use-case**: When `--discriminator eventType` is provided on the command line, kafka-spy skips the probe phase entirely and uses the given field directly. This is the baseline for topics whose discriminator is already known.

**Topics covered**: `order-events`, `payment-events`, `user-events`

**Command pattern**:
```bash
java -jar specmatic.jar kafka-spy \
  --broker localhost:9092 \
  --topic order-events \
  --discriminator eventType \
  --sample-size 500 \
  --offset beginning \
  output/inferred-schemas/order-events/
```

**Evidence — metadata** (`output/inferred-schemas/order-events/metadata.json`):
```json
{
  "discriminatorResultType": "EXPLICIT_SINGLE",
  "discriminatorField": "eventType"
}
```

Same result for `payment-events` and `user-events`:
```json
{ "discriminatorResultType": "EXPLICIT_SINGLE", "discriminatorField": "eventType" }
```

**Schema file names produced**:

| Topic | Schema files |
|---|---|
| `order-events` | `ORDER_CREATED.json`, `ORDER_SHIPPED.json`, `ORDER_CANCELLED.json` |
| `payment-events` | `PAYMENT_INITIATED.json`, `PAYMENT_COMPLETED.json`, `PAYMENT_FAILED.json` |
| `user-events` | `USER_REGISTERED.json`, `USER_UPDATED.json`, `USER_DELETED.json` |

Each file name matches the value of the `eventType` field in the corresponding input events — exactly as required.

**Verdict**: ✅ PASS — Explicit discriminator bypasses probe, routes messages correctly, metadata records `EXPLICIT_SINGLE` + `eventType`.

---

### Scenario 2: Auto-Inferred Discriminator — Non-Standard Field Name (`inferred-events`)

**Use-case**: kafka-spy runs **without** `--discriminator`. It runs a probe phase, and the inference algorithm identifies `action` (not `eventType`) as the discriminator. This proves the auto-detection works on fields with non-standard names.

**Command**:
```bash
java -jar specmatic.jar kafka-spy \
  --broker localhost:9092 \
  --topic inferred-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size 500 \
  --offset beginning \
  output/inferred-schemas/inferred-events/
```

**Input** (`input/events-cache/ITEM_ADDED.json`, first record):
```json
{
  "action": "ITEM_ADDED",
  "itemId": "fac77fcd-f577-4504-b8f7-39cb04ac8673",
  "name": "Gadget",
  "quantity": 25,
  "category": "electronics"
}
```

Three distinct `action` values appear across the probe records:
- `ITEM_ADDED` (from `input/events-cache/ITEM_ADDED.json`)
- `ITEM_REMOVED` (from `input/events-cache/ITEM_REMOVED.json`)
- `INVENTORY_ADJUSTED` (from `input/events-cache/INVENTORY_ADJUSTED.json`)

The `action` field is present in every record, has low cardinality (3 values), and perfectly separates the three clusters — making it the clear winner in the inference algorithm's scoring.

**Evidence — metadata** (`output/inferred-schemas/inferred-events/metadata.json`):
```json
{
  "discriminatorResultType": "EXPLICIT_SINGLE",
  "discriminatorField": "action"
}
```

**Evidence — inferred schema** (`output/inferred-schemas/inferred-events/ITEM_ADDED.json`):
```json
{
  "type": "object",
  "properties": {
    "action": { "type": "string", "enum": ["ITEM_ADDED"] },
    "itemId": { "type": "string" },
    "name":   { "type": "string" },
    "quantity": { "type": "integer" },
    "category": { "type": "string", "enum": ["clothing", "electronics", "food", "sports"] }
  },
  "required": ["action", "category", "itemId", "name", "quantity"]
}
```

The schema file is correctly named `ITEM_ADDED.json` — matching the `action` field value, not `eventType`.

**Verdict**: ✅ PASS — Probe phase correctly auto-detects `action` as `ExplicitSingle` discriminator. No `--discriminator` flag required.

---

### Scenario 3: No Variants Detected — Single Schema Named After Topic (`untyped-events`)

**Use-case**: All messages on `untyped-events` have identical structure (no type-discriminating field). The probe phase finds only one structural cluster and returns `NO_VARIANTS`. kafka-spy falls through to `SingleType` mode and writes one schema file named after the topic itself.

**Command**:
```bash
java -jar specmatic.jar kafka-spy \
  --broker localhost:9092 \
  --topic untyped-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size 500 \
  --offset beginning \
  output/inferred-schemas/untyped-events/
```

**Input** (`input/events-cache/HEARTBEAT.json`, two sample records):
```json
{ "serviceId": "svc-001", "timestamp": "2025-04-07T02:06:36Z", "healthy": false, "uptimeSeconds": 20710 }
{ "serviceId": "svc-002", "timestamp": "2024-10-29T16:14:52Z", "healthy": true,  "uptimeSeconds": 57167 }
```

All 500 HEARTBEAT records share the same four top-level fields: `serviceId`, `timestamp`, `healthy`, `uptimeSeconds`. No field exists that could separate them into clusters.

**Evidence — metadata** (`output/inferred-schemas/untyped-events/metadata.json`):
```json
{
  "discriminatorResultType": "SINGLE_TYPE"
}
```

**Evidence — schema file produced** (`output/inferred-schemas/untyped-events/untyped-events.json`):
```json
{
  "type": "object",
  "properties": {
    "serviceId":     { "type": "string" },
    "timestamp":     { "type": "string" },
    "healthy":       { "type": "boolean" },
    "uptimeSeconds": { "type": "integer" }
  },
  "required": ["healthy", "serviceId", "timestamp", "uptimeSeconds"]
}
```

Key observations:
- The file is named `untyped-events.json` (the topic name), **not** `default.json` or `HEARTBEAT.json`
- There is exactly **one** schema file in the directory
- `serviceId` is `type: string` — not an enum, despite values like `svc-001`…`svc-010` (correctly treated as open-ended identifiers)

**Verdict**: ✅ PASS — `NO_VARIANTS` correctly detected, `SINGLE_TYPE` written to metadata, single schema named after topic.

---

### Scenario 4: Structural Clustering — No Shared Discriminator Field (`shape-events`)

**Use-case**: Two structurally distinct event shapes (card payment and bank transfer) are interleaved on the same topic with no shared string-token field that could act as a discriminator. The probe phase returns `IMPLICIT_SHAPE`, and kafka-spy separates clusters by which top-level fields are present.

**Command**:
```bash
java -jar specmatic.jar kafka-spy \
  --broker localhost:9092 \
  --topic shape-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size 500 \
  --offset beginning \
  output/inferred-schemas/shape-events/
```

**Input — CARD_PAYMENT** (`input/events-cache/CARD_PAYMENT.json`, first record):
```json
{ "cardNumber": "7379146750848918", "expiry": "12/26", "cvv": "943", "amount": 515.16 }
```

**Input — BANK_TRANSFER** (`input/events-cache/BANK_TRANSFER.json`, first record):
```json
{ "routingNumber": "062971823", "accountNumber": "75243346", "bankName": "First National", "amount": 3022.13 }
```

The only shared field is `amount`. There is no `type`, `action`, `eventType`, or any other string-token field. The two shapes are separated entirely by field presence:
- Card shape signature: `cardNumber`, `cvv`, `expiry`
- Bank shape signature: `accountNumber`, `bankName`, `routingNumber`

**Evidence — metadata** (`output/inferred-schemas/shape-events/metadata.json`):
```json
{
  "discriminatorResultType": "IMPLICIT_SHAPE",
  "clusterSignatures": [
    { "schemaFile": "accountNumber.json", "signaturePaths": ["accountNumber", "bankName", "routingNumber"] },
    { "schemaFile": "cardNumber.json",    "signaturePaths": ["cardNumber", "cvv", "expiry"] }
  ]
}
```

**Evidence — inferred schema for card cluster** (`output/inferred-schemas/shape-events/cardNumber.json`):
```json
{
  "type": "object",
  "properties": {
    "cardNumber": { "type": "string" },
    "expiry":     { "type": "string" },
    "cvv":        { "type": "string" },
    "amount":     { "type": "number" }
  },
  "required": ["amount", "cardNumber", "cvv", "expiry"]
}
```

**Evidence — inferred schema for bank cluster** (`output/inferred-schemas/shape-events/accountNumber.json`):
```json
{
  "type": "object",
  "properties": {
    "routingNumber":  { "type": "string" },
    "accountNumber":  { "type": "string" },
    "bankName":       { "type": "string", "enum": ["City Trust", "Coastal Bank", "First National", "Heritage Savings", "Union Federal"] },
    "amount":         { "type": "number" }
  },
  "required": ["accountNumber", "amount", "bankName", "routingNumber"]
}
```

Schema files are named after the first alphabetically-sorted entry in each cluster's signature path list: `accountNumber.json` for the bank cluster, `cardNumber.json` for the card cluster.

**Verdict**: ✅ PASS — `IMPLICIT_SHAPE` correctly identified. Two structurally distinct schemas produced with no discriminator field required.

---

## Part 2: Schema Inference Quality Scenarios

These scenarios verify the JSON schema inferrer makes the right decisions about field types, enums, and required lists.

---

### Scenario 5: Single-Value Enum on Discriminator Field

**Use-case**: After routing messages by discriminator value, each schema file sees only one value for the discriminator field. The inferrer should produce a single-element `enum`, which `kafka-asyncapi` then converts to `const`.

**Evidence** (`output/inferred-schemas/inferred-events/ITEM_ADDED.json`):
```json
"action": { "type": "string", "enum": ["ITEM_ADDED"] }
```

**After `kafka-asyncapi` processing** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
action:
  type: string
  const: ITEM_ADDED
```

Same pattern for all 9 `eventType`-discriminated messages (e.g. ORDER_CREATED):
```json
"eventType": { "type": "string", "enum": ["ORDER_CREATED"] }
```
→
```yaml
eventType:
  type: string
  const: ORDER_CREATED
```

**Verdict**: ✅ PASS — Single-value `enum` on the discriminator field correctly converted to `const` in the spec.

---

### Scenario 6: Bounded Enum Correctly Retained (`category` in ITEM_ADDED)

**Use-case**: The `category` field takes exactly 4 values across all 500 events. The inferrer should detect this as a closed enum.

**Input** (all 500 `ITEM_ADDED` events use only these values):
```
electronics, clothing, food, sports
```

**Evidence — inferred schema**:
```json
"category": { "type": "string", "enum": ["clothing", "electronics", "food", "sports"] }
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
category:
  type: string
  enum:
    - clothing
    - electronics
    - food
    - sports
```

All 4 enum values present, alphabetically sorted.

**Verdict**: ✅ PASS — Bounded 4-value enum correctly inferred and retained in the spec.

---

### Scenario 7: Bounded Enum Correctly Retained (`reason` in ITEM_REMOVED)

**Input** (all 500 `ITEM_REMOVED` events):
```
out-of-stock, discontinued, damaged, recalled
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
reason:
  type: string
  enum:
    - damaged
    - discontinued
    - out-of-stock
    - recalled
```

**Verdict**: ✅ PASS — 4-value bounded enum correctly retained.

---

### Scenario 8: High-Cardinality String Field NOT Over-Constrained (`adjustedBy`)

**Use-case**: The `adjustedBy` field holds email addresses that vary across events. It should remain `type: string`, never inferred as an enum.

**Input** (distinct `adjustedBy` values in `input/events-cache/INVENTORY_ADJUSTED.json`):
```
admin@example.com, ops@warehouse.com, manager@supply.com, stock@fulfillment.com,
supervisor@warehouse.com, lead@ops.com, coordinator@supply.com, analyst@logistics.com,
planner@inventory.com, director@supply.com, controller@warehouse.com, auditor@ops.com
```
→ 12+ distinct values across 500 events — well above the enum threshold.

**Evidence — inferred schema**:
```json
"adjustedBy": { "type": "string" }
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
adjustedBy:
  type: string
```

**Verdict**: ✅ PASS — No enum generated for high-cardinality free-text field.

---

### Scenario 9: High-Cardinality String Field NOT Over-Constrained (`errorMessage` in PAYMENT_FAILED)

**Use-case**: Free-text error messages should never become enums.

**Evidence — inferred schema** (`output/inferred-schemas/payment-events/PAYMENT_FAILED.json`):
```json
"errorMessage": { "type": "string" }
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
errorMessage:
  type: string
```

**Verdict**: ✅ PASS — `errorMessage` correctly inferred as open-ended string.

---

### Scenario 10: Nested Object + Array Inference (`items` in ORDER_CREATED)

**Use-case**: The `items` field is an array of objects with their own typed properties. The inferrer must recursively handle nested structures.

**Input** (`input/events-cache/ORDER_CREATED.json`, `items` field):
```json
"items": [{ "productId": "PROD-0017", "quantity": 8, "unitPrice": 252.46 }]
```

**Evidence — inferred schema** (`output/inferred-schemas/order-events/ORDER_CREATED.json`):
```json
"items": {
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "productId":  { "type": "string" },
      "quantity":   { "type": "integer" },
      "unitPrice":  { "type": "number" }
    },
    "required": ["productId", "quantity", "unitPrice"]
  }
}
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
items:
  type: array
  items:
    type: object
    properties:
      productId:
        type: string
      quantity:
        type: integer
      unitPrice:
        type: number
    required:
      - productId
      - quantity
      - unitPrice
```

**Verdict**: ✅ PASS — Nested array of objects correctly inferred with proper types and required lists.

---

### Scenario 11: Array of Strings with Bounded Enum (`changedFields` in USER_UPDATED)

**Use-case**: `changedFields` is a `string[]` where each element is one of a fixed set of field names. The inferrer should detect the array item type as a bounded enum.

**Input** (`input/events-cache/USER_UPDATED.json`, `changedFields` values observed):
```
["name"], ["email"], ["country"], ["name", "email"], ["email", "country"], etc.
```
All elements are drawn from only: `name`, `email`, `country`.

**Evidence — inferred schema**:
```json
"changedFields": {
  "type": "array",
  "items": { "type": "string", "enum": ["country", "email", "name"] }
}
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
changedFields:
  type: array
  items:
    type: string
    enum:
      - country
      - email
      - name
```

**Verdict**: ✅ PASS — Array item enum correctly inferred for bounded string elements.

---

### Scenario 12: Boolean Field Correctly Typed (`healthy` in HEARTBEAT)

**Input**: `healthy` alternates between `true` and `false` across 500 events.

**Evidence — inferred schema**:
```json
"healthy": { "type": "boolean" }
```

**Final output** (`output/asyncapi-specs/all-kafka-events.yaml`):
```yaml
healthy:
  type: boolean
```

**Verdict**: ✅ PASS — Boolean field correctly typed (not string, not enum).

---

### Scenario 13: Integer Field Correctly Typed (`uptimeSeconds`, `quantity`, `delta`)

Three distinct integer fields across different topics:

| Field | Type in inferred schema | Type in spec |
|---|---|---|
| `uptimeSeconds` (HEARTBEAT) | `integer` | `integer` |
| `quantity` (ITEM_ADDED) | `integer` | `integer` |
| `delta` (INVENTORY_ADJUSTED) | `integer` | `integer` |

`delta` is notable: it ranges from −50 to +50 including negative values. The inferrer correctly identifies it as `integer` regardless of sign.

**Verdict**: ✅ PASS — Integer fields typed correctly across all three topics.

---

## Part 3: AsyncAPI 3.0 Generation Scenarios

---

### Scenario 14: `$schema` Key Stripped from All Payloads

**Use-case**: Every inferred JSON Schema file contains `"$schema": "http://json-schema.org/draft-07/schema#"` as a meta-annotation. This key is not valid in AsyncAPI payload schemas and must be removed.

**Evidence — present in inferred schema** (`output/inferred-schemas/inferred-events/ITEM_ADDED.json`, last line):
```json
"$schema": "http://json-schema.org/draft-07/schema#"
```

**Evidence — absent from spec**:
```bash
$ grep '$schema' output/asyncapi-specs/all-kafka-events.yaml
(no output)
```

**Verdict**: ✅ PASS — `$schema` does not appear in the generated spec.

---

### Scenario 15: `const` on Discriminator Field for EXPLICIT_SINGLE Topics

**Use-case**: For topics where the discriminator result is `EXPLICIT_SINGLE`, `kafka-asyncapi` must convert the single-element enum on the discriminator field to `const`.

**Evidence** — all 12 explicitly-discriminated messages in the merged spec:

| Message | Field | Value in spec |
|---|---|---|
| ORDER_CREATED | `eventType` | `const: ORDER_CREATED` |
| ORDER_SHIPPED | `eventType` | `const: ORDER_SHIPPED` |
| ORDER_CANCELLED | `eventType` | `const: ORDER_CANCELLED` |
| PAYMENT_INITIATED | `eventType` | `const: PAYMENT_INITIATED` |
| PAYMENT_COMPLETED | `eventType` | `const: PAYMENT_COMPLETED` |
| PAYMENT_FAILED | `eventType` | `const: PAYMENT_FAILED` |
| USER_REGISTERED | `eventType` | `const: USER_REGISTERED` |
| USER_UPDATED | `eventType` | `const: USER_UPDATED` |
| USER_DELETED | `eventType` | `const: USER_DELETED` |
| ITEM_ADDED | `action` | `const: ITEM_ADDED` |
| ITEM_REMOVED | `action` | `const: ITEM_REMOVED` |
| INVENTORY_ADJUSTED | `action` | `const: INVENTORY_ADJUSTED` |

**Verdict**: ✅ PASS — All 12 EXPLICIT_SINGLE messages have `const` on their discriminator field.

---

### Scenario 16: No `const` for SINGLE_TYPE Topics (`untyped-events`)

**Use-case**: For `SINGLE_TYPE` topics there is no discriminator field, so no `const` should appear anywhere in the message payload.

**Evidence** (`output/asyncapi-specs/all-kafka-events.yaml`, `untyped-events` message):
```yaml
untyped-events:
  name: untyped-events
  contentType: application/json
  payload:
    type: object
    properties:
      serviceId:
        type: string
      timestamp:
        type: string
      healthy:
        type: boolean
      uptimeSeconds:
        type: integer
    required:
      - healthy
      - serviceId
      - timestamp
      - uptimeSeconds
```

No `const` on any field. `serviceId` is plain `type: string` (not an enum of `svc-001`…`svc-010`).

**Verdict**: ✅ PASS — No `const` in SINGLE_TYPE payload.

---

### Scenario 17: No `const` for IMPLICIT_SHAPE Topics (`shape-events`)

**Use-case**: For `IMPLICIT_SHAPE` topics there is no shared discriminator field, so neither cluster schema should have a `const`.

**Evidence** (`output/asyncapi-specs/all-kafka-events.yaml`, `cardNumber` and `accountNumber` messages):
```yaml
cardNumber:
  payload:
    properties:
      cardNumber: { type: string }
      expiry:     { type: string }
      cvv:        { type: string }
      amount:     { type: number }

accountNumber:
  payload:
    properties:
      routingNumber: { type: string }
      accountNumber: { type: string }
      bankName:
        type: string
        enum: [City Trust, Coastal Bank, First National, Heritage Savings, Union Federal]
      amount: { type: number }
```

No `const` on any field in either schema.

**Verdict**: ✅ PASS — No `const` in IMPLICIT_SHAPE payloads.

---

### Scenario 18: Topic Names Converted to PascalCase for Channel Keys

**Use-case**: Channel keys in AsyncAPI 3.0 must be identifiers. kafka-asyncapi-merged converts kebab-case topic names to PascalCase by splitting on `-` and capitalising each segment.

| Topic (address) | Channel key | Status |
|---|---|---|
| `order-events` | `OrderEvents` | ✅ |
| `payment-events` | `PaymentEvents` | ✅ |
| `user-events` | `UserEvents` | ✅ |
| `inferred-events` | `InferredEvents` | ✅ |
| `untyped-events` | `UntypedEvents` | ✅ |
| `shape-events` | `ShapeEvents` | ✅ |

**Verdict**: ✅ PASS — All 6 channel keys correctly PascalCased.

---

### Scenario 19: Operation Key Naming (`send` + PascalCase)

**Use-case**: Each channel gets one operation with key `send<PascalCaseTopic>`.

| Operation key | Status |
|---|---|
| `sendOrderEvents` | ✅ |
| `sendPaymentEvents` | ✅ |
| `sendUserEvents` | ✅ |
| `sendInferredEvents` | ✅ |
| `sendUntypedEvents` | ✅ |
| `sendShapeEvents` | ✅ |

**Verdict**: ✅ PASS — All 6 operation keys match `send<PascalCase>`.

---

### Scenario 20: All 15 Event Types Present in Merged Spec

**Use-case**: The merged spec must contain one message entry per event type across all 6 topics.

| Message key | Source topic | Present |
|---|---|---|
| `ORDER_CREATED` | order-events | ✅ |
| `ORDER_SHIPPED` | order-events | ✅ |
| `ORDER_CANCELLED` | order-events | ✅ |
| `PAYMENT_INITIATED` | payment-events | ✅ |
| `PAYMENT_COMPLETED` | payment-events | ✅ |
| `PAYMENT_FAILED` | payment-events | ✅ |
| `USER_REGISTERED` | user-events | ✅ |
| `USER_UPDATED` | user-events | ✅ |
| `USER_DELETED` | user-events | ✅ |
| `ITEM_ADDED` | inferred-events | ✅ |
| `ITEM_REMOVED` | inferred-events | ✅ |
| `INVENTORY_ADJUSTED` | inferred-events | ✅ |
| `untyped-events` | untyped-events | ✅ |
| `cardNumber` | shape-events | ✅ |
| `accountNumber` | shape-events | ✅ |

**Verdict**: ✅ PASS — All 15 messages present. No duplicates.

---

### Scenario 21: AsyncAPI 3.0 Structural Compliance

**Use-case**: The merged spec must contain all required AsyncAPI 3.0 top-level blocks with correct values.

| Block | Actual value | Status |
|---|---|---|
| `asyncapi` | `3.0.0` | ✅ |
| `info.title` | `All Kafka Events API` | ✅ |
| `info.version` | `1.0.0` | ✅ |
| `servers.kafka.host` | `localhost:9092` | ✅ |
| `servers.kafka.protocol` | `kafka` | ✅ |
| `channels` count | 6 | ✅ |
| `operations` count | 6 | ✅ |
| `components.messages` count | 15 | ✅ |

**Verdict**: ✅ PASS — Fully compliant AsyncAPI 3.0 structure.

---

## Part 4: Metadata Round-Trip Scenario

### Scenario 22: `metadata.json` Persisted and Read Back by `kafka-asyncapi`

**Use-case**: After kafka-spy writes `metadata.json`, the downstream `kafka-asyncapi` command reads it to determine the discriminator — so `--discriminator` does not need to be repeated on the command line.

**Evidence — all 6 metadata files written correctly**:

| Topic | `discriminatorResultType` | Extra field |
|---|---|---|
| `order-events` | `EXPLICIT_SINGLE` | `discriminatorField: "eventType"` |
| `payment-events` | `EXPLICIT_SINGLE` | `discriminatorField: "eventType"` |
| `user-events` | `EXPLICIT_SINGLE` | `discriminatorField: "eventType"` |
| `inferred-events` | `EXPLICIT_SINGLE` | `discriminatorField: "action"` |
| `untyped-events` | `SINGLE_TYPE` | _(none)_ |
| `shape-events` | `IMPLICIT_SHAPE` | `clusterSignatures` array (2 entries) |

**Evidence — kafka-asyncapi for `inferred-events` run without `--discriminator`**:
- Config file (`config/inferred-events.yaml`) has no `discriminator` key
- Yet the spec correctly has `const: ITEM_ADDED`, `const: ITEM_REMOVED`, `const: INVENTORY_ADJUSTED`
- This proves the command read `discriminatorField: "action"` from `metadata.json` and applied it

**Verdict**: ✅ PASS — `metadata.json` round-trip works. Downstream commands do not need the discriminator repeated.

---

## Summary

| # | Scenario | Topic(s) | Status |
|---|---|---|---|
| 1 | Explicit `--discriminator` bypasses probe | order, payment, user | ✅ PASS |
| 2 | Auto-inferred `ExplicitSingle` on non-standard field (`action`) | inferred-events | ✅ PASS |
| 3 | `NO_VARIANTS` → single schema named after topic | untyped-events | ✅ PASS |
| 4 | `IMPLICIT_SHAPE` — structural clustering, no shared field | shape-events | ✅ PASS |
| 5 | Single-value `enum` on discriminator → `const` | all EXPLICIT_SINGLE | ✅ PASS |
| 6 | Bounded enum retained (`category`, 4 values) | inferred-events | ✅ PASS |
| 7 | Bounded enum retained (`reason`, 4 values) | inferred-events | ✅ PASS |
| 8 | High-cardinality string not over-constrained (`adjustedBy`) | inferred-events | ✅ PASS |
| 9 | High-cardinality string not over-constrained (`errorMessage`) | payment-events | ✅ PASS |
| 10 | Nested object + array correctly inferred (`items`) | order-events | ✅ PASS |
| 11 | Array of bounded strings (`changedFields`) | user-events | ✅ PASS |
| 12 | Boolean field correctly typed (`healthy`) | untyped-events | ✅ PASS |
| 13 | Integer field correctly typed (`uptimeSeconds`, `quantity`, `delta`) | multiple | ✅ PASS |
| 14 | `$schema` stripped from all payloads | all | ✅ PASS |
| 15 | `const` on discriminator field (EXPLICIT_SINGLE topics) | 4 topics | ✅ PASS |
| 16 | No `const` for SINGLE_TYPE topic | untyped-events | ✅ PASS |
| 17 | No `const` for IMPLICIT_SHAPE topic | shape-events | ✅ PASS |
| 18 | Topic names PascalCased for channel keys | all | ✅ PASS |
| 19 | Operation key naming (`send<PascalCase>`) | all | ✅ PASS |
| 20 | All 15 event types present in merged spec | all | ✅ PASS |
| 21 | AsyncAPI 3.0 structural compliance | merged spec | ✅ PASS |
| 22 | `metadata.json` round-trip (`kafka-spy` → `kafka-asyncapi`) | all | ✅ PASS |

**All 22 scenarios pass.** The discriminator inference, schema inference, and AsyncAPI generation pipeline is correct across all four result types (`EXPLICIT_SINGLE`, `SINGLE_TYPE`, `IMPLICIT_SHAPE`, and explicit-override) and across all 6 Kafka topics.
