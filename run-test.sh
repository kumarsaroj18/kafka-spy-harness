#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run-test.sh  —  Steps 1-3 (infrastructure), 5 (spy), 6-7 (AsyncAPI gen + validate), 8 (report).
#
# Step 4 (event generation) is handled by the Claude Code skill:
#   /generate-events <sample-size>
#
# Recommended workflow:
#   1. Run this script with --start-only to bring up Kafka + apps
#   2. In Claude Code, run: /generate-events <sample-size>
#   3. Run this script with --finish to spy, validate, and clean up
#
# Or run the full end-to-end skill instead:
#   /run-kafka-spy-test <sample-size>
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SAMPLE_SIZE=${1:-100}
ASYNCAPI_DIR="${ASYNCAPI_DIR:-asyncapi-specs}"
PRODUCER_URL="http://localhost:8081"
CONSUMER_URL="http://localhost:8082"
SPECMATIC_REPO="$(cd "$(dirname "$0")/.." && pwd)/specmatic"
SPECMATIC_JAR="$(ls "$SPECMATIC_REPO"/application/build/libs/specmatic-executable-*-all-unobfuscated.jar | tail -1)"

echo "=== Kafka Spy Manual Test (sample-size=$SAMPLE_SIZE) ==="
echo "    Specmatic JAR: $SPECMATIC_JAR"

# [1] Kafka
echo "[1/8] Starting Kafka..."
docker-compose up -d
sleep 12

# [2] Consumer
echo "[2/8] Starting consumer (port 8082)..."
(cd consumer && ./gradlew bootRun --quiet > /tmp/consumer.log 2>&1) &
CONSUMER_PID=$!
echo "      Waiting for consumer to be ready..."
until curl -sf "$CONSUMER_URL/api/status" > /dev/null 2>&1; do sleep 2; done
echo "      Consumer ready."

# [3] Producer
echo "[3/8] Starting producer (port 8081)..."
(cd producer && ./gradlew bootRun --quiet > /tmp/producer.log 2>&1) &
PRODUCER_PID=$!
echo "      Waiting for producer to be ready..."
until curl -sf "$PRODUCER_URL/actuator/health" > /dev/null 2>&1; do sleep 2; done
echo "      Producer ready."

# [4] Generate events — handled by Claude Code skill
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " [4/8] EVENT GENERATION — run this in Claude Code:"
echo "        /generate-events $SAMPLE_SIZE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -rp "      Press Enter once the skill has finished publishing events..."

# [5] kafka-spy
echo "[5/8] Running kafka-spy on all three topics..."
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

# [6] Generate AsyncAPI 3.0 specs
echo "[6/8] Generating AsyncAPI 3.0 specs..."
rm -rf "$ASYNCAPI_DIR"

for TOPIC in order-events payment-events user-events; do
  echo "      Generating AsyncAPI spec for $TOPIC ..."
  java -jar "$SPECMATIC_JAR" kafka-asyncapi \
    --config "config/$TOPIC.yaml" \
    --output-dir "$ASYNCAPI_DIR" \
    "inferred-schemas/$TOPIC/"
done

echo "      AsyncAPI specs generated:"
ls -1 "$ASYNCAPI_DIR/"

# [7] Validate AsyncAPI specs
echo "[7/8] Validating generated AsyncAPI specs..."
python3 scripts/validate-asyncapi.py \
  --asyncapi-dir "$ASYNCAPI_DIR" \
  --inferred-schemas-dir inferred-schemas

# [8] Validate JSON schemas and generate HTML report
echo "[8/8] Validating inferred JSON schemas and generating HTML report..."
mkdir -p reports
python3 scripts/validate-and-report.py \
  --known-schemas    schemas/ \
  --inferred-schemas inferred-schemas/ \
  --output           reports/report.html \
  --sample-size      "$SAMPLE_SIZE"

# Cleanup
echo ""
echo "Stopping services..."
kill "$CONSUMER_PID" "$PRODUCER_PID" 2>/dev/null || true
docker-compose down

echo ""
echo "=== Report: reports/report.html ==="
