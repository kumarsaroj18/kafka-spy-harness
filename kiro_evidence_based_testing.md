# Evidence-Based Testing: kafka-spy → AsyncAPI 3.0 Spec Generation

This document demonstrates that the generated `asyncapi-specs/all-kafka-events.yaml` correctly implements all requirements by tracing **input events → inferred schema → final spec output** for each use-case.

---

## Test Run Parameters

- **Sample size**: 500 events per event type
- **Total events**: 7,500 (15 types × 500)
- **Generated spec**: `asyncapi-specs/all-kafka-events.yaml`
- **Source schemas**: `inferred-schemas/<topic>/*.json`
- **Requirements doc**: `specmatic/kafka-spy-stage2-implementation-plan.md`

---

## Scenario 1: Auto-Inferred Explicit Discriminator (ExplicitSingle)

**Use-case**: kafka-spy runs WITHOUT `--discriminator` on `inferred-events` topic and auto-detects the `action` field as the discriminator.

**Requirement** (stage-2 plan §4.4.3): *"If property key == discriminatorField AND property schema has key 'enum' AND the enum list has exactly one element → replace enum with const"*

### Evidence

**Input** (`events-cache/ITEM_ADDED.json`, first record):
```json
{
  "action": "ITEM_ADDED",
  "itemId": "fac77fcd-f577-4504-b8f7-39cb04ac8673",
  "name": "Gadget",
  "quantity": 25,
  "category": "electronics"
}
```

**Intermediate — metadata** (`inferred-schemas/inferred-events/metadata.json`):
```json
{
  "discriminatorResultType": "EXPLICIT_SINGLE",
  "discriminatorField": "action"
}
```

**Intermediate — inferred schema** (`inferred-schemas/inferred-events/ITEM_ADDED.json`):
```json
"action": {
  "type": "string",
  "enum": ["ITEM_ADDED"]
}
```

**Final output** (`all-kafka-events.yaml`, ITEM_ADDED message):
```yaml
action:
  type: string
  const: ITEM_ADDED
```

**Verdict**: ✅ PASS — Single-value `enum` on discriminator field correctly converted to `const`.

---

## Scenario 2: High-Cardinality String Fields NOT Over-Constrained (`adjustedBy`, `name`)

**Use-case**: Free-text fields with more than 10 distinct values should remain `type: string` (no enum). This tests the `ENUM_CARDINALITY_THRESHOLD = 10` rule defined in `JsonSchemaInferrer.kt`.

**Requirement**: When `distinct_values.size > 10` → plain `type: string`. The threshold is hardcoded at line 17 of `specmatic/kafka-spy/src/main/kotlin/io/specmatic/kafkaspy/JsonSchemaInferrer.kt`.

### Evidence

| Field | File | Distinct Values | Count | Exceeds threshold (>10)? | Spec Output |
|---|---|---|---|---|---|
| `adjustedBy` | `INVENTORY_ADJUSTED.json` | admin@example.com, ops@warehouse.com, manager@supply.com, stock@fulfillment.com, supervisor@warehouse.com, lead@ops.com, coordinator@supply.com, analyst@logistics.com, planner@inventory.com, director@supply.com, controller@warehouse.com, auditor@ops.com | **12** | Yes | `type: string` ✅ |
| `name` | `ITEM_ADDED.json` | Gadget, Hinge, Gizmo, Gear, Wrench, Cog, Rivet, Doohickey, Bolt, Thingamajig, Clamp, Pulley, Sprocket, Widget, Lever | **15** | Yes | `type: string` ✅ |

**Verdict**: ✅ PASS — Both fields output as plain `type: string` with no enum.

**Previous behaviour (bug)**: With only 100 events, `adjustedBy` had 4 distinct values and `name` had 10 — both ≤ 10, so the inferrer incorrectly created enums.

---

## Scenario 3: Boundary Condition — Exactly 10 Distinct Values (`USER_REGISTERED.country`)

**Use-case**: A field with exactly 10 distinct values sits at the threshold boundary. Per the rule `distinct.size <= ENUM_CARDINALITY_THRESHOLD` (where threshold = 10), exactly 10 values should STILL be inferred as an enum.

**Requirement**: `10 <= 10` → enum is created. This tests the `<=` boundary of the threshold.

### Evidence

**Input** (`events-cache/USER_REGISTERED.json`, distinct `country` values):
```
AU, BR, CA, DE, FR, GB, IN, JP, MX, US
```
→ **Exactly 10 distinct values**

**Final output** (`all-kafka-events.yaml`, USER_REGISTERED message):
```yaml
country:
  type: string
  enum:
    - AU
    - BR
    - CA
    - DE
    - FR
    - GB
    - IN
    - JP
    - MX
    - US
```

**Verdict**: ✅ PASS — Exactly 10 values → enum IS created (boundary `<=` confirmed).

**Contrast with Scenario 2**: `adjustedBy` has 12 values (> 10) → no enum. `country` has 10 values (= 10) → enum. This confirms the boundary is inclusive (`<=`).

---

## Scenario 4: Legitimate Bounded Enum Correctly Retained (`category`)

**Use-case**: The `category` field has exactly 4 possible values that genuinely represent a closed set.

**Requirement**: Low-cardinality fields with stable bounded value sets should be inferred as enums.

### Evidence

**Input** (`events-cache/ITEM_ADDED.json` — all 500 events use only):
```
electronics, clothing, food, sports
```
→ **Exactly 4 values, no others across 500 events**

**Final output** (`all-kafka-events.yaml`, ITEM_ADDED message):
```yaml
category:
  type: string
  enum:
    - clothing
    - electronics
    - food
    - sports
```

**Verdict**: ✅ PASS — Legitimate enum correctly retained with all 4 values.

---

## Scenario 5: SINGLE_TYPE — No Discriminator, No Const (`untyped-events`)

**Use-case**: All messages on `untyped-events` topic have identical structure (HEARTBEAT). No discriminator exists. The spec should have no `const` field.

**Requirement** (README): *"NoVariants → SingleType: All messages share one structure — single event type, no discriminator needed"*

### Evidence

**Input** (`events-cache/HEARTBEAT.json`, sample):
```json
{
  "serviceId": "svc-001",
  "timestamp": "2026-04-25T02:26:50Z",
  "healthy": false,
  "uptimeSeconds": 57167
}
```

**Intermediate — metadata** (`inferred-schemas/untyped-events/metadata.json`):
```json
{
  "discriminatorResultType": "SINGLE_TYPE"
}
```

**Final output** (`all-kafka-events.yaml`, untyped-events message):
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

**Verdict**: ✅ PASS — No `const` on any field. `serviceId` is plain `type: string` (not an enum).

**Previous behaviour (bug)**: The old run produced `serviceId: enum: [svc-001, ..., svc-010]`.

---

## Scenario 6: IMPLICIT_SHAPE — Structural Clustering (`shape-events`)

**Use-case**: Two structurally different event shapes (CARD_PAYMENT and BANK_TRANSFER) share no common discriminator field. kafka-spy uses structural clustering to separate them.

**Requirement** (README): *"ImplicitShape: No single field works but event types differ by which top-level fields are present"*

### Evidence

**Input — CARD_PAYMENT** (`events-cache/CARD_PAYMENT.json`, first record):
```json
{
  "cardNumber": "7774361352274607",
  "expiry": "06/27",
  "cvv": "682",
  "amount": 706.99
}
```

**Input — BANK_TRANSFER** (`events-cache/BANK_TRANSFER.json`, first record):
```json
{
  "routingNumber": "352404950",
  "accountNumber": "0421494265",
  "bankName": "Coastal Bank",
  "amount": 6981.58
}
```

**Intermediate — metadata** (`inferred-schemas/shape-events/metadata.json`):
```json
{
  "discriminatorResultType": "IMPLICIT_SHAPE",
  "clusterSignatures": [
    {
      "schemaFile": "accountNumber.json",
      "signaturePaths": ["accountNumber", "bankName", "routingNumber"]
    },
    {
      "schemaFile": "cardNumber.json",
      "signaturePaths": ["cardNumber", "cvv", "expiry"]
    }
  ]
}
```

**Final output** (`all-kafka-events.yaml`, ShapeEvents channel):
```yaml
ShapeEvents:
  address: shape-events
  messages:
    accountNumber:
      $ref: "#/components/messages/accountNumber"
    cardNumber:
      $ref: "#/components/messages/cardNumber"
```

Neither `accountNumber` nor `cardNumber` message payloads contain a `const` property.

**Verdict**: ✅ PASS — Two structural clusters correctly identified and named. No discriminator/const applied.

---

## Scenario 7: `$schema` Key Stripped from All Payloads

**Use-case**: Inferred JSON Schema files contain `"$schema": "http://json-schema.org/draft-07/schema#"` but AsyncAPI payloads must not include this key.

**Requirement** (stage-2 plan §4.4.1): *"Strip the $schema key (JSON Schema meta, not valid in AsyncAPI payload)"*

### Evidence

**Intermediate** (`inferred-schemas/inferred-events/ITEM_ADDED.json`, last line):
```json
"$schema": "http://json-schema.org/draft-07/schema#"
```

**Final output**: Search for `$schema` in `all-kafka-events.yaml`:
```
$ grep '$schema' asyncapi-specs/all-kafka-events.yaml
(no results)
```

**Verdict**: ✅ PASS — `$schema` does not appear anywhere in the generated spec.

---

## Scenario 8: Per-Field Independent Inference (PAYMENT_FAILED)

**Use-case**: Two string fields in the same message get different inference outcomes — `errorCode` (4 values → enum) vs `errorMessage` (>10 values → plain string). This proves the inferrer decides independently per field, not per message.

**Requirement**: Each field's cardinality is evaluated independently against the threshold.

### Evidence

**Input** (`events-cache/PAYMENT_FAILED.json`, sample):
```json
{
  "eventType": "PAYMENT_FAILED",
  "paymentId": "...",
  "errorCode": "CARD_DECLINED",
  "errorMessage": "Card was declined by the issuer",
  "failedAt": "2025-..."
}
```

**Distinct values observed across 500 events**:

| Field | Distinct values | Count | ≤ 10? |
|---|---|---|---|
| `errorCode` | CARD_DECLINED, FRAUD_BLOCKED, INSUFFICIENT_FUNDS, TIMEOUT | **4** | Yes |
| `errorMessage` | (many varied messages — 500 events with >10 unique strings) | **>10** | No |

**Final output** (`all-kafka-events.yaml`, PAYMENT_FAILED):
```yaml
errorCode:
  type: string
  enum:
    - CARD_DECLINED
    - FRAUD_BLOCKED
    - INSUFFICIENT_FUNDS
    - TIMEOUT
errorMessage:
  type: string
```

**Verdict**: ✅ PASS — Same message, two fields, different outcomes: `errorCode` gets enum (4 ≤ 10), `errorMessage` gets plain string (>10). Inference is per-field.

---

## Scenario 9: PascalCase Channel Key Conversion

**Use-case**: Topic names (kebab-case) must be converted to PascalCase for channel keys.

**Requirement** (stage-2 plan §4.4.2): *"Splits the topic name on - and _, capitalises the first letter of each segment, joins them"*

### Evidence

| Topic (address) | Channel Key (actual) | Expected | Status |
|---|---|---|---|
| `inferred-events` | `InferredEvents` | `InferredEvents` | ✅ |
| `order-events` | `OrderEvents` | `OrderEvents` | ✅ |
| `payment-events` | `PaymentEvents` | `PaymentEvents` | ✅ |
| `shape-events` | `ShapeEvents` | `ShapeEvents` | ✅ |
| `untyped-events` | `UntypedEvents` | `UntypedEvents` | ✅ |
| `user-events` | `UserEvents` | `UserEvents` | ✅ |

**Verdict**: ✅ PASS — All 6 channel keys correctly PascalCased.

---

## Scenario 10: Operation Key Naming (`send` + PascalCase)

**Use-case**: Operation keys must follow the pattern `<action><PascalCaseTopic>`.

**Requirement** (stage-2 plan §4.4.1 example): *"sendOrderEvents"*

### Evidence

| Operation Key (actual) | Expected | Status |
|---|---|---|
| `sendInferredEvents` | `sendInferredEvents` | ✅ |
| `sendOrderEvents` | `sendOrderEvents` | ✅ |
| `sendPaymentEvents` | `sendPaymentEvents` | ✅ |
| `sendShapeEvents` | `sendShapeEvents` | ✅ |
| `sendUntypedEvents` | `sendUntypedEvents` | ✅ |
| `sendUserEvents` | `sendUserEvents` | ✅ |

**Verdict**: ✅ PASS — All 6 operation keys match `send<PascalCase>`.

---

## Scenario 11: Explicit Discriminator Topics — `eventType` const

**Use-case**: Topics run with explicit `--discriminator eventType` (`order-events`, `payment-events`, `user-events`) should have `const` on `eventType`.

### Evidence

**Intermediate — metadata** (`inferred-schemas/order-events/metadata.json`):
```json
{
  "discriminatorResultType": "EXPLICIT_SINGLE",
  "discriminatorField": "eventType"
}
```

**Final output** (`all-kafka-events.yaml`, ORDER_CREATED):
```yaml
eventType:
  type: string
  const: ORDER_CREATED
```

**Also verified for**: ORDER_SHIPPED (`const: ORDER_SHIPPED`), ORDER_CANCELLED (`const: ORDER_CANCELLED`), PAYMENT_COMPLETED, PAYMENT_FAILED, PAYMENT_INITIATED, USER_REGISTERED, USER_UPDATED, USER_DELETED — all 9 messages have `const` on `eventType`.

**Verdict**: ✅ PASS — All explicit-discriminator topics have `const` on `eventType`.

---

## Scenario 12: Non-String Type Inference (boolean, integer, number)

**Use-case**: The inferrer must correctly detect non-string primitive types — not everything is a string.

**Requirement** (`JsonSchemaInferrer.kt` `toJsonType`): Booleans → `boolean`, Int/Long → `integer`, Float/Double → `number`.

### Evidence

**Input** (`events-cache/HEARTBEAT.json`, sample):
```json
{
  "serviceId": "svc-001",
  "timestamp": "2026-04-25T02:26:50Z",
  "healthy": false,
  "uptimeSeconds": 57167
}
```

**Input** (`events-cache/CARD_PAYMENT.json`, sample):
```json
{
  "cardNumber": "7774361352274607",
  "expiry": "06/27",
  "cvv": "682",
  "amount": 706.99
}
```

**Final output** — type mapping:

| Field | Input value example | Inferred type | Status |
|---|---|---|---|
| `healthy` | `false` | `type: boolean` | ✅ |
| `uptimeSeconds` | `57167` | `type: integer` | ✅ |
| `amount` (cardNumber) | `706.99` | `type: number` | ✅ |
| `serviceId` | `"svc-001"` | `type: string` | ✅ |

**Verdict**: ✅ PASS — All four JSON primitive types (string, boolean, integer, number) correctly inferred from input data.

---

## Scenario 13: Nested Object Schema (ORDER_CREATED items array)

**Use-case**: Complex nested structures (array of objects) should be correctly inferred.

### Evidence

**Input** (`events-cache/ORDER_CREATED.json`, items field):
```json
"items": [
  {"productId": "PROD-0006", "quantity": 9, "unitPrice": 66.95}
]
```

**Final output** (`all-kafka-events.yaml`, ORDER_CREATED):
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

**Verdict**: ✅ PASS — Nested array of objects correctly inferred with proper types and required fields.

---

## Scenario 14: All 15 Event Types Present in Spec

**Use-case**: Every event type from all 15 cache files must have a corresponding message in the spec.

### Evidence

| Event Type | Topic | Present in `components.messages`? |
|---|---|---|
| ORDER_CREATED | order-events | ✅ |
| ORDER_SHIPPED | order-events | ✅ |
| ORDER_CANCELLED | order-events | ✅ |
| PAYMENT_COMPLETED | payment-events | ✅ |
| PAYMENT_FAILED | payment-events | ✅ |
| PAYMENT_INITIATED | payment-events | ✅ |
| USER_REGISTERED | user-events | ✅ |
| USER_UPDATED | user-events | ✅ |
| USER_DELETED | user-events | ✅ |
| ITEM_ADDED | inferred-events | ✅ |
| ITEM_REMOVED | inferred-events | ✅ |
| INVENTORY_ADJUSTED | inferred-events | ✅ |
| untyped-events | untyped-events | ✅ |
| cardNumber | shape-events | ✅ |
| accountNumber | shape-events | ✅ |

**Verdict**: ✅ PASS — All 15 event types accounted for.

---

## Scenario 15: AsyncAPI 3.0 Structural Compliance

**Use-case**: The spec must contain all required AsyncAPI 3.0 top-level blocks.

### Evidence

| Block | Present? | Value |
|---|---|---|
| `asyncapi` | ✅ | `3.0.0` |
| `info.title` | ✅ | `All Kafka Events API` |
| `info.version` | ✅ | `1.0.0` |
| `servers.kafka.host` | ✅ | `localhost:9092` |
| `servers.kafka.protocol` | ✅ | `kafka` |
| `channels` (count) | ✅ | 6 channels |
| `operations` (count) | ✅ | 6 operations |
| `components.messages` (count) | ✅ | 15 messages |

**Verdict**: ✅ PASS — Fully compliant AsyncAPI 3.0 structure.

---

## Summary

| # | Scenario | Status |
|---|---|---|
| 1 | Auto-inferred discriminator → `const` | ✅ PASS |
| 2 | High-cardinality strings not over-constrained | ✅ PASS |
| 3 | Boundary condition: exactly 10 values → enum | ✅ PASS |
| 4 | `category` bounded enum retained | ✅ PASS |
| 5 | SINGLE_TYPE — no const, no serviceId enum | ✅ PASS |
| 6 | IMPLICIT_SHAPE — structural clusters | ✅ PASS |
| 7 | `$schema` stripped | ✅ PASS |
| 8 | Per-field independent inference (enum vs string) | ✅ PASS |
| 9 | PascalCase channel keys | ✅ PASS |
| 10 | Operation key naming | ✅ PASS |
| 11 | Explicit discriminator `eventType` → const | ✅ PASS |
| 12 | Non-string type inference (boolean/integer/number) | ✅ PASS |
| 13 | Nested object schema (items array) | ✅ PASS |
| 14 | All 15 event types present | ✅ PASS |
| 15 | AsyncAPI 3.0 structural compliance | ✅ PASS |

**All 15 scenarios pass.** The generated spec is accurate and conforms to requirements.
