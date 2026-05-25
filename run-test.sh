#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run-test.sh  —  Steps 1-3 (infrastructure) and 5-7 (spy, validate, stats).
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
PRODUCER_URL="http://localhost:8081"
CONSUMER_URL="http://localhost:8082"
SPECMATIC_REPO="$(cd "$(dirname "$0")/.." && pwd)/specmatic"
SPECMATIC_JAR="$(ls "$SPECMATIC_REPO"/application/build/libs/specmatic-executable-*-all-unobfuscated.jar | tail -1)"

echo "=== Kafka Spy Manual Test (sample-size=$SAMPLE_SIZE) ==="
echo "    Specmatic JAR: $SPECMATIC_JAR"

# [1] Kafka
echo "[1/7] Starting Kafka..."
docker-compose up -d
sleep 12

# [2] Consumer
echo "[2/7] Starting consumer (port 8082)..."
(cd consumer && ./gradlew bootRun --quiet > /tmp/consumer.log 2>&1) &
CONSUMER_PID=$!
echo "      Waiting for consumer to be ready..."
until curl -sf "$CONSUMER_URL/api/status" > /dev/null 2>&1; do sleep 2; done
echo "      Consumer ready."

# [3] Producer
echo "[3/7] Starting producer (port 8081)..."
(cd producer && ./gradlew bootRun --quiet > /tmp/producer.log 2>&1) &
PRODUCER_PID=$!
echo "      Waiting for producer to be ready..."
until curl -sf "$PRODUCER_URL/actuator/health" > /dev/null 2>&1; do sleep 2; done
echo "      Producer ready."

# [4] Generate events — handled by Claude Code skill
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " [4/7] EVENT GENERATION — run this in Claude Code:"
echo "        /generate-events $SAMPLE_SIZE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -rp "      Press Enter once the skill has finished publishing events..."

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
mkdir -p reports
python3 scripts/validate-and-report.py \
  --known-schemas    schemas/ \
  --inferred-schemas inferred-schemas/ \
  --output           reports/report.html \
  --sample-size      "$SAMPLE_SIZE"

# [7] Consumer stats
echo "[7/7] Consumer received:"
curl -s "$CONSUMER_URL/api/status" | python3 -m json.tool

# Cleanup
echo ""
echo "Stopping services..."
kill "$CONSUMER_PID" "$PRODUCER_PID" 2>/dev/null || true
docker-compose down

echo ""
echo "=== Report: reports/report.html ==="
