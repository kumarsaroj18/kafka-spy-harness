# run-kafka-spy-test

End-to-end Kafka Spy test: starts infrastructure, generates (or loads cached) events, runs kafka-spy,
validates schemas, opens the HTML report.

**Usage**: `/run-kafka-spy-test [sample-size] [--fresh]`

- `sample-size` — events per event type (default: `100`)
- `--fresh` — discard any existing event cache files and regenerate all events from scratch

Parse `$ARGUMENTS`:
- Check if `--fresh` appears anywhere in the argument string → set FRESH=true, remove it before further parsing
- First remaining token = sample-size (integer, default `100`)

Set these variables for the whole run:
- `SAMPLE_SIZE` = parsed sample-size
- `FRESH` = true if `--fresh` was present, false otherwise
- `PRODUCER_URL` = `http://localhost:8081`
- `CONSUMER_URL` = `http://localhost:8082`
- `REPO_ROOT` = absolute path of the project root (find it: the directory containing `docker-compose.yml` and `run-test.sh`)
- `CACHE_DIR` = `{REPO_ROOT}/events-cache`
- `SPECMATIC_JAR` = result of: `ls {REPO_ROOT}/../specmatic/application/build/libs/specmatic-executable-*-all-unobfuscated.jar | tail -1`

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

#### If `--fresh` is set

Delete all cache files, then generate fresh events for all 9 types:

```bash
rm -rf {CACHE_DIR}
mkdir -p {CACHE_DIR}
for EVENT_TYPE in ORDER_CREATED ORDER_SHIPPED ORDER_CANCELLED \
                  PAYMENT_INITIATED PAYMENT_COMPLETED PAYMENT_FAILED \
                  USER_REGISTERED USER_UPDATED USER_DELETED; do
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

Once all 9 cache files are ready, publish everything in a single call:

```bash
python3 {REPO_ROOT}/scripts/publish_events.py {SAMPLE_SIZE} {CACHE_DIR} {PRODUCER_URL}
```

The script POSTs events interleaved by topic in round-robin batches of ≤ 50:
- **order-events**: ORDER_CREATED → ORDER_SHIPPED → ORDER_CANCELLED
- **payment-events**: PAYMENT_INITIATED → PAYMENT_COMPLETED → PAYMENT_FAILED
- **user-events**: USER_REGISTERED → USER_UPDATED → USER_DELETED

It prints `✓ {EVENT_TYPE}: published N` after each type and exits with code 1 on any POST failure.

---

## Step 5 — Run kafka-spy (×3 topics)

```bash
rm -rf {REPO_ROOT}/inferred-schemas
```

For each topic in `order-events payment-events user-events`:
```bash
java -jar {SPECMATIC_JAR} kafka-spy \
  --broker    localhost:9092 \
  --topic     {TOPIC} \
  --discriminator eventType \
  --sample-size   {SAMPLE_SIZE} \
  --offset    beginning \
  "{REPO_ROOT}/inferred-schemas/{TOPIC}/"
```

Print `Spying on {TOPIC} ...` before each run.

---

## Step 6 — Validate schemas and generate HTML report

```bash
mkdir -p {REPO_ROOT}/reports
python3 {REPO_ROOT}/scripts/validate-and-report.py \
  --known-schemas    {REPO_ROOT}/schemas/ \
  --inferred-schemas {REPO_ROOT}/inferred-schemas/ \
  --output           {REPO_ROOT}/reports/report.html \
  --sample-size      {SAMPLE_SIZE}
```

Capture the exit code. Note whether it is 0 (all PASS) or 1 (FAILs present).

---

## Step 7 — Consumer stats

```bash
curl -s http://localhost:8082/api/status
```

Pretty-print the JSON and show it.

---

## Cleanup

Stop the consumer and producer background processes (the ones started in steps 2 and 3):
```bash
pkill -f "consumer.*bootRun" 2>/dev/null || true
pkill -f "producer.*bootRun" 2>/dev/null || true
docker-compose -f {REPO_ROOT}/docker-compose.yml down
```

---

## Final summary

Print:
```
=== Report: {REPO_ROOT}/reports/report.html ===
Status: PASS ✅  (or FAIL ❌ if exit code was 1)
```

Then open the report in the browser:
```bash
open {REPO_ROOT}/reports/report.html
```
