#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-}"
if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario>"
    echo ""
    echo "Scenarios:"
    for f in Config.*.toml; do
        name="${f#Config.}"
        name="${name%.toml}"
        echo "  $name"
    done
    # Special scenarios that reuse existing configs
    echo "  webhook-hmac-401"
    exit 1
fi

cd "$(dirname "$0")"

BRIA_E2E_BASE_URL="${BRIA_E2E_BASE_URL:-http://localhost:4000/v1}"
BRIA_E2E_API_KEY="${BRIA_API_KEY:-e2e-secret}"
BRIA_E2E_WEBHOOK_SECRET="${BRIA_E2E_WEBHOOK_SECRET:-test-secret-42}"

# ── Config mapping: a few scenarios reuse existing TOML files ──
# webhook-hmac-401 reuses the webhook-pg config (HMAC is already set up there)
case "$SCENARIO" in
    webhook-hmac-401) CONFIG_BASE="webhook-pg" ;;
    *)                CONFIG_BASE="$SCENARIO" ;;
esac

CONFIG_FILE="Config.${CONFIG_BASE}.toml"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: $CONFIG_FILE not found"
    exit 1
fi

rm -f Config.toml
ln -sf "$CONFIG_FILE" Config.toml

DOCKER_COMPOSE="docker compose -p e2e-${SCENARIO}"

cleanup() {
    $DOCKER_COMPOSE down -v --remove-orphans > /dev/null 2>&1 || true
    rm -f Config.toml
    rm -rf tmp
}
trap cleanup EXIT

rm -rf tmp
mkdir -p tmp/bria/source
chmod -R 777 tmp/bria

# ── Pre-create source tables before bria starts ──
case "$SCENARIO" in
    sqlite-file)
        sqlite3 tmp/bria/source.db "
            CREATE TABLE IF NOT EXISTS bria_jobs (
                id TEXT PRIMARY KEY,
                payload TEXT NOT NULL DEFAULT '{}',
                status TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );
        "
        ;;
esac

echo "=== Scenario: $SCENARIO ==="
echo "=== Config:  $CONFIG_FILE ==="

echo "=== Building images ==="
$DOCKER_COMPOSE build --quiet > /dev/null 2>&1

echo "=== Starting services ==="
$DOCKER_COMPOSE up -d --quiet-pull > /dev/null 2>&1

echo "=== Waiting for bria healthy ==="
for i in $(seq 1 60); do
    if curl -sS "${BRIA_E2E_BASE_URL}/ping" 2>/dev/null | grep -q pong; then
        break
    fi
    sleep 1
done

# Wait for amqp-helper if needed (AMQP scenarios)
case "$SCENARIO" in
    queue-file|http-queue)
        for i in $(seq 1 30); do
            if $DOCKER_COMPOSE exec -T amqp-helper python3 -c 'import pika' 2>/dev/null; then
                break
            fi
            sleep 1
        done
        ;;
esac

# ── Truncate result files for cron-file to avoid stale data ──
case "$SCENARIO" in
    cron-file)
        $DOCKER_COMPOSE exec -T bria sh -c '> /tmp/bria/results.jsonl' 2>/dev/null || true
        ;;
esac

echo "=== Trigger: $SCENARIO ==="

JOB_ID="e2e-${SCENARIO}-$(date +%s)"
PIPELINE=""

# ─────────────────────────────────────────────────────────────────────────────
# Helper: HMAC computation with Python (portable across platforms)
# ─────────────────────────────────────────────────────────────────────────────
hmac_sha256() {
    local secret="$1"
    local body="$2"
    python3 -c "
import hmac, hashlib, sys
print(hmac.new(sys.argv[1].encode(), sys.argv[2].encode(), hashlib.sha256).hexdigest())
" "$secret" "$body"
}

# ─────────────────────────────────────────────────────────────────────────────
# Structured Python assertion helpers (replaces grep-based verification)
# Each reads RESULT from stdin and asserts specific JSON fields.
# ─────────────────────────────────────────────────────────────────────────────

# Assert a file-sink (JSONL) result: find the job and check status + optional exit_code
assert_jsonl_status() {
    local expected_status="$1"
    local job_id="$2"
    local expected_exit_code="${3:-}"
    python3 -c "
import json, sys
lines = [l.strip() for l in sys.stdin.read().splitlines() if l.strip()]
expected_status = sys.argv[1]
job_id = sys.argv[2]
expected_ec = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None
for line in lines:
    r = json.loads(line)
    j = r.get('job', {})
    if j.get('id') == job_id or r.get('pipeline_id') == job_id:
        assert r.get('status') == expected_status, f'Expected status={expected_status} got {r.get(\"status\")!r}'
        if expected_ec is not None:
            steps = r.get('steps', {})
            for sid, s in steps.items():
                if s.get('exit_code') == expected_ec:
                    sys.exit(0)
            sys.exit(1)
        sys.exit(0)
sys.exit(1)
" "$expected_status" "$job_id" "$expected_exit_code"
}

# Assert JSONL contains at least one result for a pipeline; check status
assert_jsonl_pipeline() {
    local expected_pipeline="$1"
    local expected_status="${2:-success}"
    python3 -c "
import json, sys
lines = [l.strip() for l in sys.stdin.read().splitlines() if l.strip()]
pipeline = sys.argv[1]
expected_status = sys.argv[2]
assert len(lines) >= 1, 'No results found'
found = False
for line in lines:
    r = json.loads(line)
    if r.get('pipeline_id') == pipeline:
        assert r.get('status') == expected_status, f'Expected status={expected_status} got {r.get(\"status\")!r}'
        found = True
assert found, f'No result for pipeline {pipeline!r}'
sys.exit(0)
" "$expected_pipeline" "$expected_status"
}

# Assert PG-sink result (psql -t -A output: status|stdout)
assert_pg_status() {
    local expected_status="$1"
    python3 -c "
import sys
line = sys.stdin.read().strip()
if not line: sys.exit(1)
parts = line.split('|', 1)
status = parts[0].strip()
assert status == sys.argv[1], f'Expected status={sys.argv[1]!r} got {status!r}'
sys.exit(0)
" "$expected_status"
}

# Assert SQLite-sink result (sqlite3 output: status|stdout)
assert_sqlite_status() {
    local expected_status="$1"
    python3 -c "
import sys
line = sys.stdin.read().strip()
if not line: sys.exit(1)
parts = line.split('|', 1)
status = parts[0].strip()
assert status == sys.argv[1], f'Expected status={sys.argv[1]!r} got {status!r}'
sys.exit(0)
" "$expected_status"
}

# Assert SSE output contains expected JSON data for the job
assert_sse_ok() {
    local scenario="$1"
    python3 -c "
import json, sys
text = sys.stdin.read()
scenario = sys.argv[1]
# SSE format: each event has a 'data:' line. We look for our job data.
for line in text.splitlines():
    line = line.strip()
    if line.startswith('data:'):
        data = line[5:].strip()
        try:
            r = json.loads(data)
            # Check the result has our pipeline pattern
            pid = r.get('pipeline_id', '')
            if pid and scenario in pid:
                assert r.get('status') == 'success', f'Expected success got {r.get(\"status\")!r}'
                sys.exit(0)
        except json.JSONDecodeError:
            continue
sys.exit(1)
" "$scenario"
}

# Assert AMQP output contains expected result
assert_amqp_ok() {
    local scenario="$1"
    python3 -c "
import json, sys
text = sys.stdin.read()
scenario = sys.argv[1]
amqp_msgs = json.loads(text)
assert len(amqp_msgs) > 0, 'No AMQP messages received'
for msg in amqp_msgs:
    body = json.loads(msg.get('body', '{}'))
    j = body.get('job', {})
    if j.get('id', '').startswith('e2e-' + scenario):
        assert body.get('status') == 'success', f'Expected success got {body.get(\"status\")!r}'
        sys.exit(0)
sys.exit(1)
" "$scenario"
}

# Assert HTTP response code matches expected
assert_http_code() {
    local expected_code="$1"
    python3 -c "
import sys
code = sys.stdin.read().strip()
assert code == sys.argv[1], f'Expected HTTP {sys.argv[1]} got {code}'
sys.exit(0)
" "$expected_code"
}

# ─────────────────────────────────────────────────────────────────────────────
# Trigger and verification per scenario
# ─────────────────────────────────────────────────────────────────────────────

case "$SCENARIO" in

    # ── Happy-path HTTP-POST scenarios ──
    http-pg|http-file|http-sqlite|http-queue|http-webhook|http-pg-recovery)
        if [ "$SCENARIO" = "http-queue" ]; then
            # Give bria a short post-health initialization window to declare AMQP topology.
            sleep 1
            $DOCKER_COMPOSE exec -T amqp-helper python3 /scripts/amqp-helper.py consume bria result.success 30 > tmp/amqp.out 2>/dev/null &
            CONSUME_PID=$!
            sleep 1
        fi
        echo "Trigger via HTTP..."
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        if [ "$SCENARIO" = "http-pg-recovery" ]; then
            echo "Waiting for job to enter running state before restart..."
            for i in $(seq 1 20); do
                STATE=$($DOCKER_COMPOSE exec -T postgres psql -U bria -d bria -t -A -c \
                    "SELECT state FROM bria_job_state WHERE job_id = '$JOB_ID' LIMIT 1;" 2>/dev/null || true)
                if [ "$STATE" = "running" ]; then
                    break
                fi
                sleep 1
            done
            echo "Restarting bria to exercise PG recovery path..."
            $DOCKER_COMPOSE restart bria > /dev/null
            for i in $(seq 1 60); do
                if curl -sS "${BRIA_E2E_BASE_URL}/ping" 2>/dev/null | grep -q pong; then
                    break
                fi
                sleep 1
            done
        fi
        ;;

    # ── SSE scenario: wait for keepalive before triggering ──
    http-sse)
        echo "Starting SSE subscriber..."
        curl -sSN -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" "${BRIA_E2E_BASE_URL}/sse" > tmp/sse.out 2>/dev/null &
        SSE_PID=$!
        # Wait for the SSE keepalive comment to confirm connection is established
        echo "Waiting for SSE connection (keepalive)..."
        for i in $(seq 1 15); do
            if grep -q "keepalive" tmp/sse.out 2>/dev/null; then
                echo "SSE connected after ${i}s"
                break
            fi
            sleep 1
        done
        echo "Trigger via HTTP..."
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        ;;

    # ── Webhook with HMAC ──
    webhook-pg)
        echo "Trigger via webhook with HMAC..."
        SECRET="${BRIA_E2E_WEBHOOK_SECRET}"
        BODY="{\"id\":\"$JOB_ID\",\"message\":\"hello webhook\"}"
        HMAC=$(hmac_sha256 "$SECRET" "$BODY")
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/hooks" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -H "X-Bria-Signature: $HMAC" \
            -d "$BODY"
        echo ""
        ;;

    # ── File-source scenarios ──
    file-file|file-sqlite)
        echo "Trigger via file source..."
        $DOCKER_COMPOSE exec -T bria mkdir -p /tmp/bria/source
        $DOCKER_COMPOSE exec -T bria sh -c "printf '%s\n' '{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}' > /tmp/bria/source/test.jsonl"
        ;;

    # ── Cron scenario ──
    cron-file)
        echo "Waiting for cron tick (schedule every 5s)..."
        ;;

    # ── PG-source scenario ──
    pg-pg)
        $DOCKER_COMPOSE exec -T postgres psql -U bria -d bria -c "
            INSERT INTO bria_jobs (id, payload, status) VALUES
            ('$JOB_ID', '{\"id\":\"$JOB_ID\",\"message\":\"hello pg-pg\"}', NULL)
            ON CONFLICT (id) DO NOTHING;" > /dev/null 2>&1
        ;;

    # ── SQLite-source scenario ──
    sqlite-file)
        sqlite3 tmp/bria/source.db \
            "INSERT OR REPLACE INTO bria_jobs (id, payload, status) VALUES ('$JOB_ID', '{\"id\":\"$JOB_ID\",\"message\":\"hello sqlite-file\"}', NULL);"
        ;;

    # ── AMQP-source scenario ──
    queue-file)
        echo "Trigger via AMQP..."
        echo "{\"id\":\"$JOB_ID\",\"message\":\"hello queue-file\"}" | \
            $DOCKER_COMPOSE exec -T amqp-helper python3 /scripts/amqp-helper.py publish bria job.submit
        ;;

    # ── Failure path: non-zero exit task ──
    http-nonzero)
        echo "Trigger via HTTP (expecting task failure)..."
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        ;;

    # ── Failure path: 413 Payload Too Large ──
    http-413)
        echo "Trigger via HTTP with oversized body (max_body_bytes=10)..."
        HTTP_CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d '{"id":"test-413","message":"this body is way too long for the tiny limit"}')
        echo "HTTP status: $HTTP_CODE"
        echo "$HTTP_CODE" | assert_http_code "413" && OK=1
        # 413 scenario exits early (no pipeline to poll)
        if [ "${OK:-0}" -eq 1 ]; then
            echo ""
            echo "=== PASS: $SCENARIO ==="
            exit 0
        else
            echo ""
            echo "=== FAIL: $SCENARIO ==="
            exit 1
        fi
        ;;

    # ── Failure path: bad webhook HMAC (401) ──
    webhook-hmac-401)
        echo "Trigger via webhook with WRONG HMAC (expecting 401)..."
        SECRET="${BRIA_E2E_WEBHOOK_SECRET}"
        BODY="{\"id\":\"$JOB_ID\",\"message\":\"hello webhook\"}"
        WRONG_HMAC="0000000000000000000000000000000000000000000000000000000000000000"
        HTTP_CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${BRIA_E2E_BASE_URL}/hooks" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -H "X-Bria-Signature: $WRONG_HMAC" \
            -d "$BODY")
        echo "HTTP status: $HTTP_CODE"
        echo "$HTTP_CODE" | assert_http_code "401" && OK=1
        # 401 scenario exits early (no pipeline to poll)
        if [ "${OK:-0}" -eq 1 ]; then
            echo ""
            echo "=== PASS: $SCENARIO ==="
            exit 0
        else
            echo ""
            echo "=== FAIL: $SCENARIO ==="
            exit 1
        fi
        ;;

    # ── Failure path: cancellation via DELETE ──
    http-cancel)
        echo "Trigger via HTTP then cancel..."
        # First submit a blocker job to fill the concurrency=1 slot,
        # so the target job queues behind it and can be cancelled at the semaphore.
        BLOCKER_ID="e2e-cancel-blocker-$(date +%s)"
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$BLOCKER_ID\",\"message\":\"blocker\"}"
        echo ""
        sleep 1
        # Now submit the target job (will wait at semaphore behind blocker)
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        sleep 1
        echo "Cancelling job $JOB_ID..."
        CANCEL_RESP=$(curl -sS -X DELETE "${BRIA_E2E_BASE_URL}/jobs/$JOB_ID" \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}")
        echo "Cancel response: $CANCEL_RESP"
        ;;

    # ── Failure path: condition step evaluating to false (fail action) ──
    http-condition-false)
        echo "Trigger via HTTP (expecting condition to fail pipeline)..."
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        ;;

    *)
        echo "ERROR: Unknown scenario: $SCENARIO"
        exit 1
        ;;
esac

# ─────────────────────────────────────────────────────────────────────────────
# Polling-based verification (replaces fixed sleep 6)
# ─────────────────────────────────────────────────────────────────────────────

echo "=== Waiting for pipeline (polling)..."
OK=0

case "$SCENARIO" in

    # ── PG-sink scenarios ──
    http-pg|webhook-pg|pg-pg)
        for i in $(seq 1 30); do
            RESULT=$($DOCKER_COMPOSE exec -T postgres psql -U bria -d bria -t -A -c \
                "SELECT status, stdout FROM bria_results WHERE job_id = '$JOB_ID' LIMIT 1;" 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_pg_status success; then
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    # ── Cron scenario: verify by pipeline_id (job IDs are auto-generated) ──
    cron-file)
        for i in $(seq 1 45); do
            RESULT=$($DOCKER_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_pipeline cron-file-pipeline success; then
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    # ── File-sink scenarios (JSONL) ──
    file-file|http-file|queue-file|sqlite-file|http-pg-recovery)
        for i in $(seq 1 30); do
            RESULT=$($DOCKER_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_status success "$JOB_ID"; then
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    # ── SQLite-sink scenarios ──
    file-sqlite|http-sqlite)
        for i in $(seq 1 30); do
            RESULT=$($DOCKER_COMPOSE exec -T bria sh -c "sqlite3 /tmp/bria/results.db 'SELECT status, stdout FROM results LIMIT 1;'" 2>/dev/null || \
                     sqlite3 tmp/bria/results.db 'SELECT status, stdout FROM results LIMIT 1;' 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_sqlite_status success; then
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    # ── SSE scenario ──
    http-sse)
        # Poll for SSE data to arrive after triggering
        for i in $(seq 1 30); do
            if grep -q '"status":"success"' tmp/sse.out 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill $SSE_PID 2>/dev/null || true
        wait $SSE_PID 2>/dev/null || true
        RESULT=$(cat tmp/sse.out 2>/dev/null || true)
        echo "SSE: $RESULT"
        if echo "$RESULT" | assert_sse_ok "$SCENARIO"; then
            OK=1
        fi
        ;;

    # ── AMQP sink scenario ──
    http-queue)
        wait $CONSUME_PID 2>/dev/null || true
        RESULT=$(cat tmp/amqp.out 2>/dev/null || true)
        echo "AMQP: $RESULT"
        if echo "$RESULT" | assert_amqp_ok "$SCENARIO"; then
            OK=1
        fi
        ;;

    # ── Webhook sink scenario ──
    http-webhook)
        RESULT=$($DOCKER_COMPOSE logs --no-log-prefix webhook-echo 2>/dev/null || true)
        echo "Webhook: $RESULT"
        if echo "$RESULT" | python3 -c "
import json, sys
text = sys.stdin.read()
scenario = sys.argv[1]
for line in text.splitlines():
    line = line.strip()
    if not line: continue
    try:
        r = json.loads(line)
        body = json.loads(r.get('body', '{}'))
        j = body.get('job', {})
        if j.get('id', '').startswith('e2e-' + scenario):
            assert body.get('status') == 'success', f'Expected success got {body.get(\"status\")!r}'
            sys.exit(0)
    except (json.JSONDecodeError, KeyError):
        continue
sys.exit(1)
" "$SCENARIO"; then
            OK=1
        fi
        ;;

    # ── Failure: non-zero exit ──
    http-nonzero)
        for i in $(seq 1 30); do
            RESULT=$($DOCKER_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_status failure "$JOB_ID"; then
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    # ── Failure: cancellation ──
    http-cancel)
        # Verify the cancellation was recorded in PG state store.
        # The target job waits at the concurrency=1 semaphore behind a blocker.
        # Once the blocker finishes (sleep 15), the target acquires the semaphore,
        # sees the cancellation signal, and records "cancelled".
        for i in $(seq 1 40); do
            STATE=$($DOCKER_COMPOSE exec -T postgres psql -U bria -d bria -t -A -c \
                "SELECT state FROM bria_job_state WHERE job_id = '$JOB_ID' LIMIT 1;" 2>/dev/null || true)
            if [ -n "$STATE" ] && echo "$STATE" | grep -q cancelled; then
                echo "Cancellation confirmed in state store"
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    # ── Failure: condition false (fail action) ──
    http-condition-false)
        for i in $(seq 1 30); do
            RESULT=$($DOCKER_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_status failure "$JOB_ID"; then
                OK=1
                break
            fi
            sleep 1
        done
        ;;

    *)
        echo "ERROR: Unknown verify for $SCENARIO"
        ;;
esac

if [ "$OK" -eq 1 ]; then
    echo ""
    echo "=== PASS: $SCENARIO ==="
    exit 0
else
    echo ""
    echo "=== FAIL: $SCENARIO ==="
    exit 1
fi
