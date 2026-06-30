# run-kafka-spy-test

End-to-end Kafka Spy test: starts infrastructure, generates (or loads cached) events, runs kafka-spy,
generates AsyncAPI 3.0 specs, and optionally validates schemas and opens the HTML report.

**Usage**: `/run-kafka-spy-test [sample-size] [--fresh] [--report]`

- `sample-size` — events per event type (default: `100`)
- `--fresh` — discard any existing event cache files and regenerate all events from scratch
- `--report` — run the JSON schema validator and generate `reports/report.html` (skipped by default)

Parse `$ARGUMENTS`:
- Check if `--fresh` appears anywhere in the argument string → set FRESH=true, remove it before further parsing
- Check if `--report` appears anywhere in the argument string → set REPORT=true, remove it before further parsing
- First remaining token = sample-size (integer, default `100`)

Set these variables for the whole run:
- `SAMPLE_SIZE` = parsed sample-size
- `FRESH` = true if `--fresh` was present, false otherwise
- `REPORT` = true if `--report` was present, false otherwise
- `PRODUCER_URL` = `http://localhost:8081`
- `CONSUMER_URL` = `http://localhost:8082`
- `REPO_ROOT` = absolute path of the project root (find it: the directory containing `docker-compose.yml` and `run-test.sh`)
- `CACHE_DIR` = `{REPO_ROOT}/events-cache`
- `ASYNCAPI_DIR` = `{REPO_ROOT}/asyncapi-specs`
- `SPECMATIC_JAR` = result of: `ls {REPO_ROOT}/../specmatic/application/build/libs/specmatic-executable-*-all-unobfuscated.jar | tail -1`

> **Tip — rebuild JAR after source changes:** If kafka-spy or application source was modified since the JAR was built, rebuild with:
> ```bash
> cd {REPO_ROOT}/../specmatic && ./gradlew :specmatic-executable:unobfuscatedShadowJar
> ```

Print a header: `=== Kafka Spy Manual Test (sample-size=N) ===`  
If `--fresh` was set, also print: `🔄 --fresh: event cache will be discarded and regenerated`

---

## Step 1 — Start Kafka

```bash
cd {REPO_ROOT}
docker-compose up -d
```

Then sleep 12 seconds to let Kafka fully initialise.

---

## Step 2 — Start Consumer (port 8082)

```bash
cd {REPO_ROOT}/consumer
./gradlew bootRun --quiet > /tmp/consumer.log 2>&1 &
```

Poll until ready (retry every 2 s, no timeout):
```bash
curl -sf http://localhost:8082/api/status
```

Print `Consumer ready.` when it responds.

---

## Step 3 — Start Producer (port 8081)

```bash
cd {REPO_ROOT}/producer
./gradlew bootRun --quiet > /tmp/producer.log 2>&1 &
```

Poll until ready (retry every 2 s):
```bash
curl -sf http://localhost:8081/actuator/health
```

Print `Producer ready.` when it responds.

---

## Step 4 — Load or generate events and publish

Events for each type are cached in `{CACHE_DIR}/{EVENT_TYPE}.json` (JSON arrays).
Use `scripts/generate_events.py` to create or top up cache files, and
`scripts/publish_events.py` to publish them. Never write inline Python at runtime.

### Cache decision (per event type)

All 15 event types:
```
ORDER_CREATED ORDER_SHIPPED ORDER_CANCELLED
PAYMENT_INITIATED PAYMENT_COMPLETED PAYMENT_FAILED
USER_REGISTERED USER_UPDATED USER_DELETED
ITEM_ADDED ITEM_REMOVED INVENTORY_ADJUSTED
HEARTBEAT
CARD_PAYMENT BANK_TRANSFER
```

#### If `--fresh` is set

Delete all cache files, then generate fresh events for all 15 types:

```bash
rm -rf {CACHE_DIR}
mkdir -p {CACHE_DIR}
for EVENT_TYPE in ORDER_CREATED ORDER_SHIPPED ORDER_CANCELLED \
                  PAYMENT_INITIATED PAYMENT_COMPLETED PAYMENT_FAILED \
                  USER_REGISTERED USER_UPDATED USER_DELETED \
                  ITEM_ADDED ITEM_REMOVED INVENTORY_ADJUSTED \
                  HEARTBEAT \
                  CARD_PAYMENT BANK_TRANSFER; do
  python3 {REPO_ROOT}/scripts/generate_events.py $EVENT_TYPE {SAMPLE_SIZE} {CACHE_DIR}
done
```

Print: `📝 {EVENT_TYPE}: generated {SAMPLE_SIZE} events (cache recreated)` (the script prints this automatically)

#### If cache file does not exist

```bash
python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {SAMPLE_SIZE} {CACHE_DIR}
```

The script creates the cache file and prints:
`📝 {EVENT_TYPE}: generated {SAMPLE_SIZE} events (cache total: {SAMPLE_SIZE})`

#### If cache file exists AND cached count ≥ SAMPLE_SIZE

No generation needed — `publish_events.py` reads the first `SAMPLE_SIZE` entries directly.
Print: `✅ {EVENT_TYPE}: using {SAMPLE_SIZE}/{cached_count} cached events`

#### If cache file exists AND cached count < SAMPLE_SIZE

Print:
```
⚠  {EVENT_TYPE}: cache has {cached_count} events but {SAMPLE_SIZE} requested.
   Options:
     [A] Append {needed} more events to the cache file (total will be {SAMPLE_SIZE})
     [B] Recreate the cache file with {SAMPLE_SIZE} fresh events
```
**Pause and ask the user to choose A or B**, then:

- **A (Append)** — top up the existing cache file:
  ```bash
  python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {needed} {CACHE_DIR} --append
  ```
  (`needed` = `SAMPLE_SIZE − cached_count`)

- **B (Recreate)** — delete and regenerate:
  ```bash
  rm {CACHE_DIR}/{EVENT_TYPE}.json
  python3 {REPO_ROOT}/scripts/generate_events.py {EVENT_TYPE} {SAMPLE_SIZE} {CACHE_DIR}
  ```

### Publishing

Once all 15 cache files are ready, publish everything in a single call:

```bash
python3 {REPO_ROOT}/scripts/publish_events.py {SAMPLE_SIZE} {CACHE_DIR} {PRODUCER_URL}
```

The script POSTs events interleaved by topic in round-robin batches of ≤ 50:
- **order-events**: ORDER_CREATED → ORDER_SHIPPED → ORDER_CANCELLED (via `/api/events/bulk`)
- **payment-events**: PAYMENT_INITIATED → PAYMENT_COMPLETED → PAYMENT_FAILED (via `/api/events/bulk`)
- **user-events**: USER_REGISTERED → USER_UPDATED → USER_DELETED (via `/api/events/bulk`)
- **inferred-events**: ITEM_ADDED → ITEM_REMOVED → INVENTORY_ADJUSTED (via `/api/events/bulk`, action field routing)
- **untyped-events**: HEARTBEAT (via `/api/events/raw?topic=untyped-events`)
- **shape-events**: CARD_PAYMENT → BANK_TRANSFER (via `/api/events/raw?topic=shape-events`)

It prints `✓ {EVENT_TYPE}: published N` after each type and exits with code 1 on any POST failure.

---

## Step 5 — Run kafka-spy (×6 topics)

```bash
rm -rf {REPO_ROOT}/inferred-schemas
```

**IMPORTANT — output root:** Always pass `"{REPO_ROOT}/inferred-schemas/"` as the output root
(NOT `inferred-schemas/$TOPIC/`). kafka-spy always appends `<topic-name>/` to the output root, so
using the root directly produces the correct flat layout (`inferred-schemas/order-events/*.json`)
that `kafka-asyncapi-merged` and the validators expect.

### Existing 3 topics (explicit discriminator — probe phase skipped → EXPLICIT_SINGLE in metadata.json)

```bash
for TOPIC in order-events payment-events user-events; do
  echo "Spying on $TOPIC (explicit discriminator) ..."
  java -jar {SPECMATIC_JAR} kafka-spy \
    --broker    localhost:9092 \
    --topic     $TOPIC \
    --discriminator eventType \
    --sample-size   {SAMPLE_SIZE} \
    --offset    beginning \
    "{REPO_ROOT}/inferred-schemas/"
done
```

### New 3 topics (no discriminator — auto-inference runs)

```bash
echo "Spying on inferred-events (expected: EXPLICIT_SINGLE / action) ..."
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic inferred-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/"

echo "Spying on untyped-events (expected: SINGLE_TYPE) ..."
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic untyped-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/"

echo "Spying on shape-events (expected: IMPLICIT_SHAPE) ..."
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic shape-events \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas/"
```

---

## Step 5b — Topic auto-discovery smoke test

Write to `auto-discovered-schemas/` (NOT inside `inferred-schemas/`) to avoid polluting
the metadata and asyncapi validators which scan all subdirectories of `inferred-schemas/`.

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
```

Count discovered topic directories:
```bash
DISCOVERED=$(ls -d {REPO_ROOT}/auto-discovered-schemas/*/ 2>/dev/null | wc -l | tr -d ' ')
if [ "$DISCOVERED" -ge 6 ]; then
  echo "Auto-discovery PASS: $DISCOVERED topics discovered"
  AUTO_DISCOVERY_PASS=true
else
  echo "Auto-discovery FAIL: expected >= 6 topics, got $DISCOVERED"
  AUTO_DISCOVERY_PASS=false
fi
```

---

## Step 5c — Validate metadata.json

```bash
echo "Validating metadata.json for all 6 topics ..."
python3 {REPO_ROOT}/scripts/validate-metadata.py \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas
METADATA_EXIT=$?
```

Capture the exit code. If non-zero, print "Metadata validation FAILED" and continue (do not abort).

---

## Step 6 — Generate merged AsyncAPI 3.0 spec

```bash
rm -rf {REPO_ROOT}/asyncapi-specs
mkdir -p {REPO_ROOT}/asyncapi-specs

echo "Generating merged AsyncAPI spec ..."
java -jar {SPECMATIC_JAR} kafka-asyncapi-merged \
  --config     "{REPO_ROOT}/config/merged.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs" \
  "{REPO_ROOT}/inferred-schemas/"
```

This writes `{ASYNCAPI_DIR}/all-kafka-events.yaml` — a single AsyncAPI 3.0 document covering
all 6 topics. `kafka-asyncapi-merged` reads `metadata.json` from each topic subdirectory to
determine discriminator handling: `EXPLICIT_SINGLE` triggers enum→const conversion on the
discriminator field; all other result types emit schemas as-is.

---

## Step 7 — Validate merged AsyncAPI spec

```bash
python3 {REPO_ROOT}/scripts/validate-asyncapi.py \
  --merged-spec          {REPO_ROOT}/asyncapi-specs/all-kafka-events.yaml \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas
ASYNCAPI_EXIT=$?
```

Capture the exit code. Note whether it is 0 (PASS) or 1 (FAILs present).
If it fails, print the output and continue to Step 8 (do not abort the run).

---

## Step 8 — Validate schemas and generate HTML report _(only if `--report` was passed)_

Skip this step entirely if `REPORT` is false.

If `REPORT` is true:
```bash
mkdir -p {REPO_ROOT}/reports
python3 {REPO_ROOT}/scripts/validate-and-report.py \
  --known-schemas    {REPO_ROOT}/schemas/ \
  --inferred-schemas {REPO_ROOT}/inferred-schemas/ \
  --output           {REPO_ROOT}/reports/report.html \
  --sample-size      {SAMPLE_SIZE}
REPORT_EXIT=$?
```

Capture the exit code. Note whether it is 0 (all PASS) or 1 (FAILs present).

---

## Step 9 — Consumer stats

```bash
curl -s http://localhost:8082/api/status
```

Pretty-print the JSON and show it. All 6 topic counts should be > 0.

---

## Cleanup

Stop the consumer and producer background processes (the ones started in steps 2 and 3):
```bash
pkill -f "consumer.*bootRun" 2>/dev/null || true
pkill -f "producer.*bootRun" 2>/dev/null || true
docker-compose -f {REPO_ROOT}/docker-compose.yml down
```

---

---

## Edge-case run pattern (for topics in `inferred-schemas-edge/`)

Edge-case topics (EC1–EC8) write to a separate output root so the original 6-topic regression in `inferred-schemas/` stays untouched and `validate-asyncapi.py`'s topic-count check doesn't fail.

### Spy one edge topic

```bash
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker localhost:9092 \
  --topic <edge-topic> \
  --probe-count 60 \
  --probe-duration-ms 30000 \
  --sample-size {SAMPLE_SIZE} \
  --offset beginning \
  "{REPO_ROOT}/inferred-schemas-edge/"
```

All edge topics use inference (no `--discriminator` flag) except EC7's `generic-events` which also uses inference.

### Publish only the edge topic (per-topic filter)

```bash
python3 {REPO_ROOT}/scripts/publish_events.py {SAMPLE_SIZE} {CACHE_DIR} {PRODUCER_URL} \
  --topics <edge-topic>
```

For EC5 small-N runs (total=2, 3, 4, large-N), vary `SAMPLE_SIZE` and pass `--topics small-sample-events`.

### Validate edge topics

```bash
# Metadata (WARN-skips topics not in EXPECTED; FAILs on missing EXPECTED topics):
python3 {REPO_ROOT}/scripts/validate-metadata.py \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas-edge

# Merged AsyncAPI spec (adjust --expected-topics to match how many edge topics were run):
java -jar {SPECMATIC_JAR} kafka-asyncapi-merged \
  --config     "{REPO_ROOT}/config/merged.yaml" \
  --output-dir "{REPO_ROOT}/asyncapi-specs-edge" \
  "{REPO_ROOT}/inferred-schemas-edge/"

python3 {REPO_ROOT}/scripts/validate-asyncapi.py \
  --merged-spec          {REPO_ROOT}/asyncapi-specs-edge/all-kafka-events.yaml \
  --inferred-schemas-dir {REPO_ROOT}/inferred-schemas-edge \
  --expected-topics <N>
```

**EC3-literal and EC4 are negotiate-class** — leave them out of `EXPECTED` in `validate-metadata.py` and do not add them to the asyncapi validation until D1 is resolved.

---

## Final summary

Always print:
```
=== AsyncAPI specs: {REPO_ROOT}/asyncapi-specs/ ===
Auto-discovery: PASS ✅  (or FAIL ❌ based on Step 5b result)
Metadata:       PASS ✅  (or FAIL ❌ if Step 5c exit code was 1)
AsyncAPI:       PASS ✅  (or FAIL ❌ if Step 7 exit code was 1)
```

If `REPORT` is true, also print:
```
=== Report: {REPO_ROOT}/reports/report.html ===
Schema:   PASS ✅  (or FAIL ❌ if Step 8 exit code was 1)
```

Then, only if `REPORT` is true, open the report in the browser:
```bash
open {REPO_ROOT}/reports/report.html
```
