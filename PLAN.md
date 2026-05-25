# Kafka Spy Manual Test Harness — Implementation Plan

> **Working directory**: `/Users/saroj/workspace/github-personal/kafka-spy-manual-test/`
> **Sibling repo**: `/Users/saroj/workspace/github-personal/specmatic/`

---

## Goal

End-to-end manual test harness that:
1. Runs a Spring Boot Kotlin **producer** (REST API) publishing to three Kafka topics
2. Runs a Spring Boot Kotlin **consumer** simulating a real app (unaware of kafka-spy)
3. Uses the **Anthropic SDK (claude-opus-4-7)** to generate random, realistic payloads and POST them to the producer
4. Runs **`specmatic kafka-spy`** to infer JSON schemas from the observed Kafka traffic
5. **Validates** inferred schemas against ground-truth schemas
6. Generates an **HTML report** with per-event-type PASS / WARNING / FAIL results

---

## Architecture

```
run-test.sh  (./run-test.sh <sample-size>)
     │
     ├─► docker-compose up            Kafka (cp-kafka:7.6.0) on localhost:9092
     │
     ├─► consumer/bootRun :8082       Listens on 3 topics, group=main-consumer-group
     │                                Completely unaware of kafka-spy
     ├─► producer/bootRun :8081       Exposes POST /api/events/bulk
     │
     ├─► scripts/generate-and-publish.py
     │       Claude API → random payloads → POST to producer → Kafka
     │
     ├─► specmatic kafka-spy (×3)     Unique group.id, --offset beginning, infers schemas
     │       → inferred-schemas/{topic}/{EVENT_TYPE}.json
     │
     └─► scripts/validate-and-report.py
             schemas/ vs inferred-schemas/ → reports/report.html
```

---

## Topics and Event Types

| Topic            | Event Types                                                  |
|------------------|--------------------------------------------------------------|
| `order-events`   | ORDER_CREATED, ORDER_SHIPPED, ORDER_CANCELLED                |
| `payment-events` | PAYMENT_INITIATED, PAYMENT_COMPLETED, PAYMENT_FAILED         |
| `user-events`    | USER_REGISTERED, USER_UPDATED, USER_DELETED                  |

Discriminator field on all topics: **`eventType`**

---

## Final Directory Structure

```
kafka-spy-manual-test/
├── PLAN.md                          ← this file
├── docker-compose.yml               ← Confluent cp-kafka:7.6.0 + ZooKeeper
├── run-test.sh                      ← main orchestration script
├── producer/                        ← Spring Boot Kotlin, port 8081
│   ├── settings.gradle.kts
│   ├── build.gradle.kts
│   └── src/main/
│       ├── kotlin/com/example/producer/
│       │   ├── ProducerApplication.kt
│       │   ├── controller/EventController.kt       # POST /api/events, POST /api/events/bulk
│       │   ├── service/EventProducerService.kt     # routes eventType → Kafka topic
│       │   └── model/
│       │       ├── OrderEvents.kt
│       │       ├── PaymentEvents.kt
│       │       └── UserEvents.kt
│       └── resources/application.yml
├── consumer/                        ← Spring Boot Kotlin, port 8082
│   ├── settings.gradle.kts
│   ├── build.gradle.kts
│   └── src/main/
│       ├── kotlin/com/example/consumer/
│       │   ├── ConsumerApplication.kt
│       │   ├── controller/StatusController.kt      # GET /api/status (received counts)
│       │   └── listener/
│       │       ├── OrderEventListener.kt
│       │       ├── PaymentEventListener.kt
│       │       └── UserEventListener.kt
│       └── resources/application.yml
├── schemas/                         ← ground-truth JSON Schema Draft-07 (9 files)
│   ├── order-events/
│   │   ├── ORDER_CREATED.json
│   │   ├── ORDER_SHIPPED.json
│   │   └── ORDER_CANCELLED.json
│   ├── payment-events/
│   │   ├── PAYMENT_INITIATED.json
│   │   ├── PAYMENT_COMPLETED.json
│   │   └── PAYMENT_FAILED.json
│   └── user-events/
│       ├── USER_REGISTERED.json
│       ├── USER_UPDATED.json
│       └── USER_DELETED.json
├── scripts/
│   ├── generate-and-publish.py      ← Claude API → payloads → POST to producer
│   └── validate-and-report.py       ← schema diff → HTML report
├── inferred-schemas/                ← kafka-spy output (git-ignored)
└── reports/                         ← HTML report output (git-ignored)
```

---

## Implementation Steps

### Step 1 — `docker-compose.yml`

Confluent stack (ZooKeeper + Kafka) on localhost:9092.

```yaml
version: "3.8"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports: ["2181:2181"]

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

---

### Step 2 — Producer App (`producer/`)

**Port**: 8081  
**Dependencies**: spring-boot-starter-web, spring-kafka, jackson-module-kotlin

#### `build.gradle.kts`
```kotlin
plugins {
    id("org.springframework.boot") version "3.3.0"
    id("io.spring.dependency-management") version "1.1.5"
    kotlin("jvm") version "1.9.24"
    kotlin("plugin.spring") version "1.9.24"
}
group = "com.example"
version = "0.0.1-SNAPSHOT"

repositories { mavenCentral() }

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
}
```

#### `application.yml`
```yaml
server:
  port: 8081
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
```

#### Data Models

**`model/OrderEvents.kt`**
```kotlin
package com.example.producer.model

data class OrderItem(val productId: String, val quantity: Int, val unitPrice: Double)

data class OrderCreatedEvent(
    val eventType: String = "ORDER_CREATED",
    val orderId: String,
    val customerId: String,
    val items: List<OrderItem>,
    val totalAmount: Double,
    val currency: String,        // USD | EUR | GBP
    val createdAt: String
)

data class OrderShippedEvent(
    val eventType: String = "ORDER_SHIPPED",
    val orderId: String,
    val trackingNumber: String,
    val carrier: String,         // FEDEX | UPS | DHL | USPS
    val shippedAt: String,
    val estimatedDelivery: String
)

data class OrderCancelledEvent(
    val eventType: String = "ORDER_CANCELLED",
    val orderId: String,
    val reason: String,          // CUSTOMER_REQUEST | OUT_OF_STOCK | PAYMENT_FAILED | FRAUD_DETECTED
    val cancelledAt: String,
    val refundAmount: Double
)
```

**`model/PaymentEvents.kt`**
```kotlin
package com.example.producer.model

data class PaymentInitiatedEvent(
    val eventType: String = "PAYMENT_INITIATED",
    val paymentId: String,
    val orderId: String,
    val amount: Double,
    val currency: String,        // USD | EUR | GBP
    val method: String,          // CREDIT_CARD | DEBIT_CARD | PAYPAL | BANK_TRANSFER
    val initiatedAt: String
)

data class PaymentCompletedEvent(
    val eventType: String = "PAYMENT_COMPLETED",
    val paymentId: String,
    val transactionId: String,
    val amount: Double,
    val completedAt: String
)

data class PaymentFailedEvent(
    val eventType: String = "PAYMENT_FAILED",
    val paymentId: String,
    val errorCode: String,       // INSUFFICIENT_FUNDS | CARD_DECLINED | TIMEOUT | FRAUD_BLOCKED
    val errorMessage: String,
    val failedAt: String
)
```

**`model/UserEvents.kt`**
```kotlin
package com.example.producer.model

data class UserRegisteredEvent(
    val eventType: String = "USER_REGISTERED",
    val userId: String,
    val email: String,
    val name: String,
    val country: String,         // ISO 3166-1 alpha-2
    val registeredAt: String
)

data class UserUpdatedEvent(
    val eventType: String = "USER_UPDATED",
    val userId: String,
    val changedFields: List<String>,
    val updatedAt: String
)

data class UserDeletedEvent(
    val eventType: String = "USER_DELETED",
    val userId: String,
    val reason: String,          // USER_REQUEST | GDPR_ERASURE | ADMIN_ACTION
    val deletedAt: String
)
```

#### REST Controller (`controller/EventController.kt`)
```kotlin
package com.example.producer.controller

import com.example.producer.service.EventProducerService
import com.fasterxml.jackson.databind.JsonNode
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/events")
class EventController(private val service: EventProducerService) {

    @PostMapping
    fun publish(@RequestBody event: JsonNode): Map<String, String> {
        val eventType = event.get("eventType")?.asText()
            ?: return mapOf("status" to "error", "message" to "missing eventType field")
        service.publish(eventType, event)
        return mapOf("status" to "ok", "eventType" to eventType)
    }

    @PostMapping("/bulk")
    fun publishBulk(@RequestBody events: List<JsonNode>): Map<String, Any> {
        var count = 0
        events.forEach { event ->
            val eventType = event.get("eventType")?.asText() ?: return@forEach
            service.publish(eventType, event)
            count++
        }
        return mapOf("status" to "ok", "published" to count)
    }
}
```

#### Producer Service (`service/EventProducerService.kt`)
```kotlin
package com.example.producer.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service

@Service
class EventProducerService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {
    companion object {
        private val TOPIC_MAP = mapOf(
            "ORDER_CREATED"       to "order-events",
            "ORDER_SHIPPED"       to "order-events",
            "ORDER_CANCELLED"     to "order-events",
            "PAYMENT_INITIATED"   to "payment-events",
            "PAYMENT_COMPLETED"   to "payment-events",
            "PAYMENT_FAILED"      to "payment-events",
            "USER_REGISTERED"     to "user-events",
            "USER_UPDATED"        to "user-events",
            "USER_DELETED"        to "user-events",
        )
    }

    fun publish(eventType: String, event: JsonNode) {
        val topic = TOPIC_MAP[eventType]
            ?: throw IllegalArgumentException("Unknown eventType: $eventType")
        kafkaTemplate.send(topic, eventType, objectMapper.writeValueAsString(event))
    }
}
```

#### `ProducerApplication.kt`
```kotlin
package com.example.producer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
fun main(args: Array<String>) { runApplication<ProducerApplication>(*args) }
```

#### `settings.gradle.kts`
```kotlin
rootProject.name = "producer"
```

---

### Step 3 — Consumer App (`consumer/`)

**Port**: 8082  
**Consumer group**: `main-consumer-group` (completely separate from kafka-spy)

#### `application.yml`
```yaml
server:
  port: 8082
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: main-consumer-group
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
```

#### Listeners

**`listener/OrderEventListener.kt`**
```kotlin
package com.example.consumer.listener

import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class OrderEventListener {
    private val log = LoggerFactory.getLogger(javaClass)
    val count = AtomicInteger(0)

    @KafkaListener(topics = ["order-events"])
    fun onMessage(message: String) {
        count.incrementAndGet()
        log.info("order-events: {}", message.take(120))
    }
}
```

Repeat the same pattern for:
- `PaymentEventListener` — topic `payment-events`
- `UserEventListener` — topic `user-events`

#### Status Endpoint (`controller/StatusController.kt`)
```kotlin
package com.example.consumer.controller

import com.example.consumer.listener.*
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/status")
class StatusController(
    private val orderListener: OrderEventListener,
    private val paymentListener: PaymentEventListener,
    private val userListener: UserEventListener
) {
    @GetMapping
    fun status() = mapOf(
        "status" to "up",
        "received" to mapOf(
            "order-events"   to orderListener.count.get(),
            "payment-events" to paymentListener.count.get(),
            "user-events"    to userListener.count.get(),
        )
    )
}
```

#### `ConsumerApplication.kt`
```kotlin
package com.example.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
fun main(args: Array<String>) { runApplication<ConsumerApplication>(*args) }
```

Consumer `build.gradle.kts` and `settings.gradle.kts` are identical to producer except `rootProject.name = "consumer"` and no actuator needed (the `/api/status` endpoint serves as health check).

---

### Step 4 — Ground-Truth JSON Schemas (`schemas/`)

9 files, JSON Schema Draft-07. These represent the producer's contract and are compared against kafka-spy output.

**`schemas/order-events/ORDER_CREATED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "orderId", "customerId", "items", "totalAmount", "currency", "createdAt"],
  "properties": {
    "eventType":   { "type": "string", "const": "ORDER_CREATED" },
    "orderId":     { "type": "string" },
    "customerId":  { "type": "string" },
    "items": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "required": ["productId", "quantity", "unitPrice"],
        "properties": {
          "productId":  { "type": "string" },
          "quantity":   { "type": "integer", "minimum": 1 },
          "unitPrice":  { "type": "number", "minimum": 0 }
        }
      }
    },
    "totalAmount": { "type": "number", "minimum": 0 },
    "currency":    { "type": "string", "enum": ["USD", "EUR", "GBP"] },
    "createdAt":   { "type": "string" }
  }
}
```

**`schemas/order-events/ORDER_SHIPPED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "orderId", "trackingNumber", "carrier", "shippedAt", "estimatedDelivery"],
  "properties": {
    "eventType":         { "type": "string", "const": "ORDER_SHIPPED" },
    "orderId":           { "type": "string" },
    "trackingNumber":    { "type": "string" },
    "carrier":           { "type": "string", "enum": ["FEDEX", "UPS", "DHL", "USPS"] },
    "shippedAt":         { "type": "string" },
    "estimatedDelivery": { "type": "string" }
  }
}
```

**`schemas/order-events/ORDER_CANCELLED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "orderId", "reason", "cancelledAt", "refundAmount"],
  "properties": {
    "eventType":    { "type": "string", "const": "ORDER_CANCELLED" },
    "orderId":      { "type": "string" },
    "reason":       { "type": "string", "enum": ["CUSTOMER_REQUEST", "OUT_OF_STOCK", "PAYMENT_FAILED", "FRAUD_DETECTED"] },
    "cancelledAt":  { "type": "string" },
    "refundAmount": { "type": "number", "minimum": 0 }
  }
}
```

**`schemas/payment-events/PAYMENT_INITIATED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "paymentId", "orderId", "amount", "currency", "method", "initiatedAt"],
  "properties": {
    "eventType":   { "type": "string", "const": "PAYMENT_INITIATED" },
    "paymentId":   { "type": "string" },
    "orderId":     { "type": "string" },
    "amount":      { "type": "number", "minimum": 0 },
    "currency":    { "type": "string", "enum": ["USD", "EUR", "GBP"] },
    "method":      { "type": "string", "enum": ["CREDIT_CARD", "DEBIT_CARD", "PAYPAL", "BANK_TRANSFER"] },
    "initiatedAt": { "type": "string" }
  }
}
```

**`schemas/payment-events/PAYMENT_COMPLETED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "paymentId", "transactionId", "amount", "completedAt"],
  "properties": {
    "eventType":     { "type": "string", "const": "PAYMENT_COMPLETED" },
    "paymentId":     { "type": "string" },
    "transactionId": { "type": "string" },
    "amount":        { "type": "number", "minimum": 0 },
    "completedAt":   { "type": "string" }
  }
}
```

**`schemas/payment-events/PAYMENT_FAILED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "paymentId", "errorCode", "errorMessage", "failedAt"],
  "properties": {
    "eventType":    { "type": "string", "const": "PAYMENT_FAILED" },
    "paymentId":    { "type": "string" },
    "errorCode":    { "type": "string", "enum": ["INSUFFICIENT_FUNDS", "CARD_DECLINED", "TIMEOUT", "FRAUD_BLOCKED"] },
    "errorMessage": { "type": "string" },
    "failedAt":     { "type": "string" }
  }
}
```

**`schemas/user-events/USER_REGISTERED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "userId", "email", "name", "country", "registeredAt"],
  "properties": {
    "eventType":    { "type": "string", "const": "USER_REGISTERED" },
    "userId":       { "type": "string" },
    "email":        { "type": "string" },
    "name":         { "type": "string" },
    "country":      { "type": "string" },
    "registeredAt": { "type": "string" }
  }
}
```

**`schemas/user-events/USER_UPDATED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "userId", "changedFields", "updatedAt"],
  "properties": {
    "eventType":     { "type": "string", "const": "USER_UPDATED" },
    "userId":        { "type": "string" },
    "changedFields": { "type": "array", "items": { "type": "string" } },
    "updatedAt":     { "type": "string" }
  }
}
```

**`schemas/user-events/USER_DELETED.json`**
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "required": ["eventType", "userId", "reason", "deletedAt"],
  "properties": {
    "eventType": { "type": "string", "const": "USER_DELETED" },
    "userId":    { "type": "string" },
    "reason":    { "type": "string", "enum": ["USER_REQUEST", "GDPR_ERASURE", "ADMIN_ACTION"] },
    "deletedAt": { "type": "string" }
  }
}
```

---

### Step 5 — AI Payload Generator (`scripts/generate-and-publish.py`)

Uses Anthropic SDK with `claude-opus-4-7` to generate random, varied payloads per event type, then POSTs them to the producer's `/api/events/bulk` endpoint.

Key design:
- Generates in batches of up to 50 per Claude call (stays within token limits)
- Instructs Claude to vary enum values across the batch (important for kafka-spy enum detection)
- Strips markdown code fences from Claude response before JSON parsing
- 0.2s sleep between requests to avoid rate-limit bursts

```python
#!/usr/bin/env python3
"""
Usage:
  python generate-and-publish.py --sample-size 100 --producer-url http://localhost:8081
"""
import argparse, json, time, sys
import anthropic
import urllib.request

EVENT_TYPES = [
    "ORDER_CREATED", "ORDER_SHIPPED", "ORDER_CANCELLED",
    "PAYMENT_INITIATED", "PAYMENT_COMPLETED", "PAYMENT_FAILED",
    "USER_REGISTERED", "USER_UPDATED", "USER_DELETED",
]

SCHEMAS = {
    "ORDER_CREATED":     "orderId(uuid), customerId(uuid), items(list of {productId, quantity int, unitPrice decimal}), totalAmount(decimal), currency(USD|EUR|GBP), createdAt(ISO-8601)",
    "ORDER_SHIPPED":     "orderId(uuid), trackingNumber(string), carrier(FEDEX|UPS|DHL|USPS), shippedAt(ISO-8601), estimatedDelivery(ISO-8601)",
    "ORDER_CANCELLED":   "orderId(uuid), reason(CUSTOMER_REQUEST|OUT_OF_STOCK|PAYMENT_FAILED|FRAUD_DETECTED), cancelledAt(ISO-8601), refundAmount(decimal)",
    "PAYMENT_INITIATED": "paymentId(uuid), orderId(uuid), amount(decimal), currency(USD|EUR|GBP), method(CREDIT_CARD|DEBIT_CARD|PAYPAL|BANK_TRANSFER), initiatedAt(ISO-8601)",
    "PAYMENT_COMPLETED": "paymentId(uuid), transactionId(string), amount(decimal), completedAt(ISO-8601)",
    "PAYMENT_FAILED":    "paymentId(uuid), errorCode(INSUFFICIENT_FUNDS|CARD_DECLINED|TIMEOUT|FRAUD_BLOCKED), errorMessage(string), failedAt(ISO-8601)",
    "USER_REGISTERED":   "userId(uuid), email(email address), name(full name), country(ISO alpha-2), registeredAt(ISO-8601)",
    "USER_UPDATED":      "userId(uuid), changedFields(list of field names e.g. [name, email]), updatedAt(ISO-8601)",
    "USER_DELETED":      "userId(uuid), reason(USER_REQUEST|GDPR_ERASURE|ADMIN_ACTION), deletedAt(ISO-8601)",
}

def generate_batch(client, event_type, count):
    prompt = f"""Generate {count} realistic and varied JSON objects for Kafka event type "{event_type}".
Each object must have:
- "eventType": "{event_type}"
- Fields: {SCHEMAS[event_type]}
Requirements:
- Use realistic UUIDs (format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx)
- Vary enum values across the batch — do NOT always use the same value
- Vary numeric amounts realistically
- Use ISO-8601 timestamps from 2024-2026
- Return ONLY a valid JSON array with exactly {count} objects, no extra text"""

    message = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}]
    )
    text = message.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text)

def post_bulk(producer_url, events):
    payload = json.dumps(events).encode("utf-8")
    req = urllib.request.Request(
        f"{producer_url}/api/events/bulk",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--producer-url", default="http://localhost:8081")
    args = parser.parse_args()

    client = anthropic.Anthropic()
    batch_size = min(args.sample_size, 50)

    for event_type in EVENT_TYPES:
        print(f"  Generating {args.sample_size} × {event_type} ...", end=" ", flush=True)
        total_published, remaining = 0, args.sample_size
        while remaining > 0:
            chunk = min(batch_size, remaining)
            events = generate_batch(client, event_type, chunk)
            result = post_bulk(args.producer_url, events)
            total_published += result.get("published", 0)
            remaining -= chunk
            time.sleep(0.2)
        print(f"published {total_published}")

    print("Done. All events published to Kafka.")

if __name__ == "__main__":
    main()
```

---

### Step 6 — Validation & Report (`scripts/validate-and-report.py`)

#### Validation Rules

| Check | Severity | Condition |
|-------|----------|-----------|
| Schema file exists | **FAIL** | kafka-spy produced no file for this event type |
| Required fields covered | **FAIL** | Ground-truth required field not marked required in inferred |
| Property types match | **FAIL** | `type` mismatch on shared property |
| Enum completeness | **WARNING** | Known enum value absent from inferred (may need larger sample) |
| No missing properties | **WARNING** | Ground-truth property absent from inferred |
| No phantom properties | **WARNING** | Inferred property not in ground-truth |

Report exits with code 1 if any FAIL is present (CI-compatible).

```python
#!/usr/bin/env python3
"""
Usage:
  python validate-and-report.py \
    --known-schemas ../schemas \
    --inferred-schemas ../inferred-schemas \
    --output ../reports/report.html \
    --sample-size 100
"""
import argparse, json, os, datetime, sys
from pathlib import Path
from dataclasses import dataclass, field

@dataclass
class Finding:
    level: str    # FAIL | WARNING | INFO
    message: str

@dataclass
class EventResult:
    topic: str
    event_type: str
    status: str   # PASS | WARNING | FAIL
    findings: list = field(default_factory=list)
    known_schema: dict = field(default_factory=dict)
    inferred_schema: dict = field(default_factory=dict)

def compare_schemas(known, inferred):
    findings = []
    known_props = known.get("properties", {})
    inferred_props = inferred.get("properties", {})
    known_required = set(known.get("required", []))
    inferred_required = set(inferred.get("required", []))

    for f in (known_required - inferred_required):
        findings.append(Finding("FAIL", f"Required field '{f}' not marked required in inferred schema"))

    for prop, schema in known_props.items():
        if prop not in inferred_props:
            findings.append(Finding("WARNING", f"Property '{prop}' from ground truth absent in inferred schema"))
            continue
        inf_prop = inferred_props[prop]
        known_type, inferred_type = schema.get("type"), inf_prop.get("type")
        if known_type and inferred_type and known_type != inferred_type:
            findings.append(Finding("FAIL", f"Property '{prop}' type mismatch: expected '{known_type}', got '{inferred_type}'"))
        if "enum" in schema:
            missing_enum = set(schema["enum"]) - set(inf_prop.get("enum", []))
            if missing_enum:
                findings.append(Finding("WARNING", f"Property '{prop}' missing enum values: {missing_enum}"))

    for prop in inferred_props:
        if prop not in known_props:
            findings.append(Finding("WARNING", f"Inferred schema has unexpected property '{prop}'"))

    return findings

def validate_all(known_root, inferred_root):
    results = []
    for topic_dir in sorted(known_root.iterdir()):
        if not topic_dir.is_dir():
            continue
        topic = topic_dir.name
        for schema_file in sorted(topic_dir.glob("*.json")):
            event_type = schema_file.stem
            known = json.loads(schema_file.read_text())
            inferred_path = inferred_root / topic / schema_file.name
            if not inferred_path.exists():
                results.append(EventResult(
                    topic=topic, event_type=event_type, status="FAIL",
                    findings=[Finding("FAIL", "kafka-spy produced no schema file for this event type")],
                    known_schema=known
                ))
                continue
            inferred = json.loads(inferred_path.read_text())
            findings = compare_schemas(known, inferred)
            if any(f.level == "FAIL" for f in findings):
                status = "FAIL"
            elif any(f.level == "WARNING" for f in findings):
                status = "WARNING"
            else:
                status = "PASS"
            results.append(EventResult(topic=topic, event_type=event_type, status=status,
                                       findings=findings, known_schema=known, inferred_schema=inferred))
    return results

def render_html(results, sample_size, output):
    # Full HTML report with summary stats table + per-event-type details section
    # showing side-by-side ground-truth vs inferred schema, findings color-coded by level.
    # See kafka-spy-test-plan.md in specmatic repo for complete render_html implementation.
    pass  # implement per the full template in Step 6 of kafka-spy-test-plan.md

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--known-schemas",    default="../schemas",             type=Path)
    parser.add_argument("--inferred-schemas", default="../inferred-schemas",    type=Path)
    parser.add_argument("--output",           default="../reports/report.html", type=Path)
    parser.add_argument("--sample-size",      default=100, type=int)
    args = parser.parse_args()

    results = validate_all(args.known_schemas, args.inferred_schemas)
    render_html(results, args.sample_size, args.output)

    fail_count = sum(1 for r in results if r.status == "FAIL")
    sys.exit(1 if fail_count > 0 else 0)

if __name__ == "__main__":
    main()
```

> The full `render_html` HTML template is in `kafka-spy-test-plan.md` (Step 6) in the sibling specmatic repo.

---

### Step 7 — Orchestration Script (`run-test.sh`)

```bash
#!/usr/bin/env bash
set -euo pipefail

SAMPLE_SIZE=${1:-100}
PRODUCER_URL="http://localhost:8081"
CONSUMER_URL="http://localhost:8082"
SPECMATIC_REPO="$(cd "$(dirname "$0")/.." && pwd)/specmatic"
SPECMATIC_JAR="$(ls "$SPECMATIC_REPO"/application/build/libs/specmatic-executable-*-all-unobfuscated.jar | tail -1)"

echo "=== Kafka Spy Manual Test (sample-size=$SAMPLE_SIZE) ==="

# [1] Kafka
echo "[1/7] Starting Kafka..."
docker-compose up -d
sleep 12

# [2] Consumer
echo "[2/7] Starting consumer (port 8082)..."
(cd consumer && ./gradlew bootRun --quiet > /tmp/consumer.log 2>&1) &
CONSUMER_PID=$!
until curl -sf "$CONSUMER_URL/api/status" > /dev/null 2>&1; do sleep 2; done
echo "      Consumer ready."

# [3] Producer
echo "[3/7] Starting producer (port 8081)..."
(cd producer && ./gradlew bootRun --quiet > /tmp/producer.log 2>&1) &
PRODUCER_PID=$!
until curl -sf "$PRODUCER_URL/actuator/health" > /dev/null 2>&1; do sleep 2; done
echo "      Producer ready."

# [4] Generate events
echo "[4/7] Generating $SAMPLE_SIZE events per type via Claude API..."
python3 scripts/generate-and-publish.py \
  --sample-size "$SAMPLE_SIZE" \
  --producer-url "$PRODUCER_URL"

# [5] kafka-spy
echo "[5/7] Running kafka-spy on all three topics..."
rm -rf inferred-schemas

for TOPIC in order-events payment-events user-events; do
  echo "      Spying on $TOPIC ..."
  java -jar "$SPECMATIC_JAR" kafka-spy \
    --broker    localhost:9092 \
    --topic     "$TOPIC" \
    --discriminator eventType \
    --sample-size   "$SAMPLE_SIZE" \
    --offset    beginning \
    "inferred-schemas/$TOPIC/"
done

# [6] Validate and report
echo "[6/7] Validating schemas and generating HTML report..."
python3 scripts/validate-and-report.py \
  --known-schemas    schemas/ \
  --inferred-schemas inferred-schemas/ \
  --output           reports/report.html \
  --sample-size      "$SAMPLE_SIZE"

# [7] Consumer stats
echo "[7/7] Consumer received:"
curl -s "$CONSUMER_URL/api/status" | python3 -m json.tool

# Cleanup
kill "$CONSUMER_PID" "$PRODUCER_PID" 2>/dev/null || true
docker-compose down

echo ""
echo "=== Report: reports/report.html ==="
```

---

## Implementation Order

1. `docker-compose.yml`
2. `schemas/` — all 9 ground-truth JSON files
3. `producer/` — Gradle project, models, service, controller
4. `consumer/` — Gradle project, listeners, status controller
5. `scripts/generate-and-publish.py`
6. `scripts/validate-and-report.py` (full `render_html`)
7. `run-test.sh` + `.gitignore`

---

## Validation Rules Summary

| Check | Severity |
|-------|----------|
| kafka-spy produced a schema file | FAIL if missing |
| All ground-truth required fields also required in inferred | FAIL |
| Property types match | FAIL |
| All known enum values present in inferred | WARNING |
| No ground-truth properties absent from inferred | WARNING |
| No unexpected properties in inferred | WARNING |

Exit code 1 if any FAIL (CI-ready).

---

## Prerequisites

- Docker + Docker Compose
- JDK 17+
- Python 3.10+ with `anthropic` package: `pip install anthropic`
- `ANTHROPIC_API_KEY` set in environment
- specmatic jar built from sibling repo:
  ```bash
  cd /Users/saroj/workspace/github-personal/specmatic
  ./gradlew :application:shadowJar
  # → application/build/libs/specmatic-executable-<version>-all-unobfuscated.jar
  ```

## Key Design Decisions

1. **Consumer isolation** — group id `main-consumer-group` is completely separate from kafka-spy's internal UUID group id; the consumer is never disrupted.
2. **Offset strategy** — kafka-spy runs `--offset beginning` after all messages are published so it reads the full dataset in one pass and stops at `--sample-size` per event type.
3. **AI payload variation** — Claude is explicitly instructed to vary enum values across the batch to ensure kafka-spy sees enough distinct values for enum detection.
4. **Validation severity** — missing required fields and type mismatches are FAIL (broken contract); missing enum values and extra/absent properties are WARNING (common with small samples).
5. **HTML report CI gate** — exits with code 1 on any FAIL, making it pluggable into CI pipelines later.
