# generate-events

Generate realistic, varied Kafka event payloads for all 9 event types and publish them to the local producer.
Events are cached to disk so subsequent runs skip generation entirely.

**Usage**: `/generate-events [sample-size] [producer-url] [--fresh]`

- `sample-size` — number of events per event type (default: `100`)
- `producer-url` — producer base URL (default: `http://localhost:8081`)
- `--fresh` — discard any existing cache files and regenerate all events from scratch

Parse `$ARGUMENTS`:
- Check if `--fresh` appears anywhere in the argument string → set FRESH=true, remove it before further parsing
- First remaining token = sample-size (integer, default `100`)
- Second remaining token = producer-url (default `http://localhost:8081`)

Set:
- `REPO_ROOT` = the directory containing `docker-compose.yml` (find with `find` or use the project root)
- `CACHE_DIR` = `{REPO_ROOT}/events-cache`

---

## Tools

Event generation and publishing are handled by two scripts in `{REPO_ROOT}/scripts/`:

| Script | Purpose | Invocation |
|--------|---------|-----------|
| `scripts/generate_events.py` | Generate events for **one** event type and write/append to its cache file | See "Cache behaviour" below |
| `scripts/publish_events.py` | Read all 9 cache files and POST events to the producer interleaved by topic | See "Publishing" below |

Never write inline Python at runtime — always call these scripts.

---

## Cache behaviour

For each of the 9 event types there is a cache file at `{CACHE_DIR}/{EVENT_TYPE}.json`
containing a JSON array of event objects.

Determine the events to publish for each event type using this decision tree:

### If `--fresh` is set

Delete all cache files, then for each event type run:

```bash
python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {sample_size} {CACHE_DIR}
```

This creates a fresh cache file with `sample_size` events.

### If cache file does not exist

```bash
python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {sample_size} {CACHE_DIR}
```

The script creates `{CACHE_DIR}/{EVENT_TYPE}.json` and prints:
`📝 {EVENT_TYPE}: generated {sample_size} events (cache total: {sample_size})`

### If cache file exists AND cached count ≥ sample_size

No generation needed — `publish_events.py` reads the first `sample_size` entries directly.
Print: `✅ {EVENT_TYPE}: using {sample_size}/{cached_count} cached events`

### If cache file exists AND cached count < sample_size

Print a warning:
```
⚠  {EVENT_TYPE}: cache has {cached_count} events but {sample_size} requested.
   Options:
     [A] Append {needed} more events to the cache file (total will be {sample_size})
     [B] Recreate the cache file with {sample_size} fresh events
```
**Pause and ask the user to choose A or B**, then:

- **A (Append)**: run the generator with `--append` to top up the cache file:
  ```bash
  python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {needed} {CACHE_DIR} --append
  ```
  (`needed` = `sample_size − cached_count`)

- **B (Recreate)**: delete the cache file, then run the generator without `--append`:
  ```bash
  rm {CACHE_DIR}/{EVENT_TYPE}.json
  python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {sample_size} {CACHE_DIR}
  ```

---

## Publishing

Once all 9 cache files are ready, publish everything in a single call:

```bash
python3 {REPO_ROOT}/scripts/publish_events.py {sample_size} {CACHE_DIR} {producer_url}
```

The script:
- Reads the first `sample_size` entries from each of the 9 cache files
- POSTs them to `{producer_url}/api/events/bulk` in round-robin batches of ≤ 50, interleaved by topic:
  - **order-events**: ORDER_CREATED → ORDER_SHIPPED → ORDER_CANCELLED
  - **payment-events**: PAYMENT_INITIATED → PAYMENT_COMPLETED → PAYMENT_FAILED
  - **user-events**: USER_REGISTERED → USER_UPDATED → USER_DELETED
- Prints `✓ {EVENT_TYPE}: published {n}` after each type completes
- Exits with code 1 on any POST failure

---

## Event Type Schemas (reference)

### ORDER_CREATED (topic: order-events)
```json
{
  "eventType": "ORDER_CREATED",
  "orderId": "<uuid>",
  "customerId": "<uuid>",
  "items": [
    { "productId": "<string>", "quantity": <int ≥1>, "unitPrice": <decimal> }
  ],
  "totalAmount": <decimal ≥0>,
  "currency": "<USD|EUR|GBP>",
  "createdAt": "<ISO-8601>"
}
```

### ORDER_SHIPPED (topic: order-events)
```json
{
  "eventType": "ORDER_SHIPPED",
  "orderId": "<uuid>",
  "trackingNumber": "<string>",
  "carrier": "<FEDEX|UPS|DHL|USPS>",
  "shippedAt": "<ISO-8601>",
  "estimatedDelivery": "<ISO-8601>"
}
```

### ORDER_CANCELLED (topic: order-events)
```json
{
  "eventType": "ORDER_CANCELLED",
  "orderId": "<uuid>",
  "reason": "<CUSTOMER_REQUEST|OUT_OF_STOCK|PAYMENT_FAILED|FRAUD_DETECTED>",
  "cancelledAt": "<ISO-8601>",
  "refundAmount": <decimal ≥0>
}
```

### PAYMENT_INITIATED (topic: payment-events)
```json
{
  "eventType": "PAYMENT_INITIATED",
  "paymentId": "<uuid>",
  "orderId": "<uuid>",
  "amount": <decimal ≥0>,
  "currency": "<USD|EUR|GBP>",
  "method": "<CREDIT_CARD|DEBIT_CARD|PAYPAL|BANK_TRANSFER>",
  "initiatedAt": "<ISO-8601>"
}
```

### PAYMENT_COMPLETED (topic: payment-events)
```json
{
  "eventType": "PAYMENT_COMPLETED",
  "paymentId": "<uuid>",
  "transactionId": "<string>",
  "amount": <decimal ≥0>,
  "completedAt": "<ISO-8601>"
}
```

### PAYMENT_FAILED (topic: payment-events)
```json
{
  "eventType": "PAYMENT_FAILED",
  "paymentId": "<uuid>",
  "errorCode": "<INSUFFICIENT_FUNDS|CARD_DECLINED|TIMEOUT|FRAUD_BLOCKED>",
  "errorMessage": "<human-readable string>",
  "failedAt": "<ISO-8601>"
}
```

### USER_REGISTERED (topic: user-events)
```json
{
  "eventType": "USER_REGISTERED",
  "userId": "<uuid>",
  "email": "<email>",
  "name": "<full name>",
  "country": "<ISO 3166-1 alpha-2>",
  "registeredAt": "<ISO-8601>"
}
```

### USER_UPDATED (topic: user-events)
```json
{
  "eventType": "USER_UPDATED",
  "userId": "<uuid>",
  "changedFields": ["<field1>", "<field2>"],
  "updatedAt": "<ISO-8601>"
}
```

### USER_DELETED (topic: user-events)
```json
{
  "eventType": "USER_DELETED",
  "userId": "<uuid>",
  "reason": "<USER_REQUEST|GDPR_ERASURE|ADMIN_ACTION>",
  "deletedAt": "<ISO-8601>"
}
```
