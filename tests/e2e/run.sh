#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Usage:
#   ./run.sh <scenario>          run one scenario (expects infra already up)
#   ./run.sh --all               build once, start infra, run every scenario
#   ./run.sh --infra-up          bring infra up and wait for healthy
#   ./run.sh --infra-down        tear infra down
# ---------------------------------------------------------------------------

cd "$(dirname "$0")"

BRIA_E2E_BASE_URL="${BRIA_E2E_BASE_URL:-http://localhost:4000/v1}"
BRIA_E2E_API_KEY="${BRIA_API_KEY:-e2e-secret}"
BRIA_E2E_WEBHOOK_SECRET="${BRIA_E2E_WEBHOOK_SECRET:-test-secret-42}"

INFRA_COMPOSE="docker compose -p e2e-infra -f docker-compose.infra.yml"
BRIA_COMPOSE="docker compose -p e2e-bria"   # uses docker-compose.yml (bria only)

# ── Pre-built image tag used by bria service ──────────────────────────────────
BRIA_IMAGE="bria:e2e"

# ── All scenarios in run order ────────────────────────────────────────────────
HAPPY_PATH_SCENARIOS=(
    http-pg file-file file-sqlite http-file http-sqlite http-sse
    webhook-pg cron-file pg-pg sqlite-file
    queue-file http-queue http-webhook http-pg-recovery
)
FAILURE_SCENARIOS=(
    http-nonzero http-413 http-cancel http-condition-false webhook-hmac-401
)
ALL_SCENARIOS=("${HAPPY_PATH_SCENARIOS[@]}" "${FAILURE_SCENARIOS[@]}")

# ── --infra-up ────────────────────────────────────────────────────────────────
if [ "${1:-}" = "--infra-up" ]; then
    echo "=== Starting shared infra ==="
    $INFRA_COMPOSE up -d --quiet-pull
    echo "=== Waiting for postgres ==="
    for i in $(seq 1 30); do
        if $INFRA_COMPOSE exec -T postgres pg_isready -U bria -d bria > /dev/null 2>&1; then break; fi
        sleep 1
    done
    echo "=== Waiting for rabbitmq ==="
    for i in $(seq 1 30); do
        if $INFRA_COMPOSE exec -T rabbitmq rabbitmq-diagnostics -q ping > /dev/null 2>&1; then break; fi
        sleep 1
    done
    echo "=== Waiting for amqp-helper ==="
    for i in $(seq 1 30); do
        if $INFRA_COMPOSE exec -T amqp-helper python3 -c 'import pika' > /dev/null 2>&1; then break; fi
        sleep 1
    done
    echo "=== Infra ready ==="
    exit 0
fi

# ── --infra-down ──────────────────────────────────────────────────────────────
if [ "${1:-}" = "--infra-down" ]; then
    echo "=== Tearing down shared infra ==="
    $INFRA_COMPOSE down -v --remove-orphans
    docker network rm e2e-net > /dev/null 2>&1 || true
    exit 0
fi

# ── --all ─────────────────────────────────────────────────────────────────────
if [ "${1:-}" = "--all" ]; then
    echo "=== Building bria:e2e image ==="
    docker build -q -t "$BRIA_IMAGE" ../.. -f ../../Dockerfile

    # Bring infra up (idempotent)
    "$0" --infra-up

    PASS=()
    FAIL=()
    for s in "${ALL_SCENARIOS[@]}"; do
        if "$0" "$s"; then
            PASS+=("$s")
        else
            FAIL+=("$s")
        fi
    done

    "$0" --infra-down

    echo ""
    echo "=== Results ==="
    echo "PASS (${#PASS[@]}): ${PASS[*]:-none}"
    echo "FAIL (${#FAIL[@]}): ${FAIL[*]:-none}"
    [ "${#FAIL[@]}" -eq 0 ]
    exit $?
fi

# ── Single-scenario mode ──────────────────────────────────────────────────────
SCENARIO="${1:-}"
if [ -z "$SCENARIO" ]; then
    echo "Usage: $0 <scenario> | --all | --infra-up | --infra-down"
    echo ""
    echo "Scenarios:"
    for f in Config.*.toml; do
        name="${f#Config.}"; name="${name%.toml}"; echo "  $name"
    done
    echo "  webhook-hmac-401"
    exit 1
fi

# Config mapping: a few scenarios reuse existing TOML files
case "$SCENARIO" in
    webhook-hmac-401) CONFIG_BASE="webhook-pg" ;;
    *)                CONFIG_BASE="$SCENARIO"  ;;
esac

CONFIG_FILE="Config.${CONFIG_BASE}.toml"
if [ ! -f "$CONFIG_FILE" ]; then
    echo "ERROR: $CONFIG_FILE not found"
    exit 1
fi

# ── Per-scenario cleanup: stop bria and wipe tmp ──────────────────────────────
cleanup_scenario() {
    $BRIA_COMPOSE down -v --remove-orphans > /dev/null 2>&1 || true
    rm -rf Config.toml
    rm -rf tmp
}
trap cleanup_scenario EXIT

# ── Reset PG result tables between scenarios ──────────────────────────────────
reset_pg() {
    $INFRA_COMPOSE exec -T postgres psql -U bria -d bria -c \
        "TRUNCATE bria_results; TRUNCATE bria_jobs; DELETE FROM bria_job_state;" \
        > /dev/null 2>&1 || true
}

rm -rf tmp
mkdir -p tmp/bria/source
chmod -R 777 tmp/bria

# ── Pre-create source tables before bria starts ───────────────────────────────
# Use a one-shot container with the same volume mount to avoid
# file ownership/locking issues on Linux CI.
case "$SCENARIO" in
    sqlite-file)
        cat > tmp/source_setup.sql << 'SQLEOF'
CREATE TABLE IF NOT EXISTS bria_jobs (
    id TEXT PRIMARY KEY,
    payload TEXT NOT NULL DEFAULT '{}',
    status TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
SQLEOF
        docker run --rm --entrypoint sh \
            -v "$(pwd)/tmp/bria:/tmp/bria" \
            -v "$(pwd)/tmp/source_setup.sql:/setup.sql:ro" \
            "$BRIA_IMAGE" -c 'sqlite3 /tmp/bria/source.db < /setup.sql' 2>/dev/null || true
        rm -f tmp/source_setup.sql
        ;;
esac

ln -sf "$CONFIG_FILE" Config.toml

echo "=== Scenario: $SCENARIO ==="
echo "=== Config:  $CONFIG_FILE ==="

# Reset shared state so previous scenario results don't bleed in
reset_pg

# ── Start bria with new config ────────────────────────────────────────────────
echo "=== Starting bria ==="
$BRIA_COMPOSE up -d

echo "=== Waiting for bria healthy ==="
for i in $(seq 1 20); do
    if curl -sS "${BRIA_E2E_BASE_URL}/ping" 2>/dev/null | grep -q pong; then
        echo "Bria ready after ${i}s"
        break
    fi
    sleep 1
done

# Wait for amqp-helper pika import if needed (already installed; this is near-instant)
case "$SCENARIO" in
    queue-file|http-queue)
        for i in $(seq 1 10); do
            if $INFRA_COMPOSE exec -T amqp-helper python3 -c 'import pika' 2>/dev/null; then break; fi
            sleep 1
        done
        ;;
esac

# Truncate stale result files for cron
case "$SCENARIO" in
    cron-file)
        $BRIA_COMPOSE exec -T bria sh -c '> /tmp/bria/results.jsonl' 2>/dev/null || true
        ;;
esac

echo "=== Trigger: $SCENARIO ==="

JOB_ID="e2e-${SCENARIO}-$(date +%s)"

# ─────────────────────────────────────────────────────────────────────────────
# Helpers (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────
hmac_sha256() {
    local secret="$1" body="$2"
    python3 -c "
import hmac, hashlib, sys
print(hmac.new(sys.argv[1].encode(), sys.argv[2].encode(), hashlib.sha256).hexdigest())
" "$secret" "$body"
}

assert_jsonl_status() {
    local expected_status="$1" job_id="$2" expected_exit_code="${3:-}"
    python3 -c "
import json, sys
lines = [l.strip() for l in sys.stdin.read().splitlines() if l.strip()]
expected_status = sys.argv[1]; job_id = sys.argv[2]
expected_ec = int(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None
for line in lines:
    r = json.loads(line); j = r.get('job', {})
    if j.get('id') == job_id or r.get('pipeline_id') == job_id:
        assert r.get('status') == expected_status, f'Expected status={expected_status} got {r.get(\"status\")!r}'
        if expected_ec is not None:
            steps = r.get('steps', {})
            for sid, s in steps.items():
                if s.get('exit_code') == expected_ec: sys.exit(0)
            sys.exit(1)
        sys.exit(0)
sys.exit(1)
" "$expected_status" "$job_id" "$expected_exit_code"
}

assert_jsonl_pipeline() {
    local expected_pipeline="$1" expected_status="${2:-success}"
    python3 -c "
import json, sys
lines = [l.strip() for l in sys.stdin.read().splitlines() if l.strip()]
pipeline = sys.argv[1]; expected_status = sys.argv[2]
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

assert_pg_status() {
    local expected_status="$1"
    python3 -c "
import sys
line = sys.stdin.read().strip()
if not line: sys.exit(1)
status = line.split('|', 1)[0].strip()
assert status == sys.argv[1], f'Expected status={sys.argv[1]!r} got {status!r}'
sys.exit(0)
" "$expected_status"
}

assert_sqlite_status() {
    local expected_status="$1"
    python3 -c "
import sys
line = sys.stdin.read().strip()
if not line: sys.exit(1)
status = line.split('|', 1)[0].strip()
assert status == sys.argv[1], f'Expected status={sys.argv[1]!r} got {status!r}'
sys.exit(0)
" "$expected_status"
}

assert_sse_ok() {
    local scenario="$1"
    python3 -c "
import json, sys
text = sys.stdin.read(); scenario = sys.argv[1]
for line in text.splitlines():
    line = line.strip()
    if line.startswith('data:'):
        data = line[5:].strip()
        try:
            r = json.loads(data)
            pid = r.get('pipeline_id', '')
            if pid and scenario in pid:
                assert r.get('status') == 'success', f'Expected success got {r.get(\"status\")!r}'
                sys.exit(0)
        except json.JSONDecodeError:
            continue
sys.exit(1)
" "$scenario"
}

assert_amqp_ok() {
    local scenario="$1"
    python3 -c "
import json, sys
text = sys.stdin.read(); scenario = sys.argv[1]
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
# Trigger
# ─────────────────────────────────────────────────────────────────────────────
case "$SCENARIO" in

    http-pg|http-file|http-sqlite|http-queue|http-webhook|http-pg-recovery)
        if [ "$SCENARIO" = "http-queue" ]; then
            sleep 1
            $INFRA_COMPOSE exec -T amqp-helper python3 /scripts/amqp-helper.py consume bria result.success 30 > tmp/amqp.out 2>/dev/null &
            CONSUME_PID=$!
            sleep 1
        fi
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        if [ "$SCENARIO" = "http-pg-recovery" ]; then
            echo "Waiting for job to enter running state before restart..."
            for i in $(seq 1 20); do
                STATE=$($INFRA_COMPOSE exec -T postgres psql -U bria -d bria -t -A -c \
                    "SELECT state FROM bria_job_state WHERE job_id = '$JOB_ID' LIMIT 1;" 2>/dev/null || true)
                if [ "$STATE" = "running" ]; then break; fi
                sleep 1
            done
            echo "Restarting bria to exercise PG recovery path..."
            $BRIA_COMPOSE restart bria > /dev/null
            for i in $(seq 1 20); do
                if curl -sS "${BRIA_E2E_BASE_URL}/ping" 2>/dev/null | grep -q pong; then break; fi
                sleep 1
            done
        fi
        ;;

    http-sse)
        curl -sSN -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" "${BRIA_E2E_BASE_URL}/sse" > tmp/sse.out 2>/dev/null &
        SSE_PID=$!
        for i in $(seq 1 10); do
            if grep -q "keepalive" tmp/sse.out 2>/dev/null; then break; fi
            sleep 1
        done
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        ;;

    webhook-pg)
        BODY="{\"id\":\"$JOB_ID\",\"message\":\"hello webhook\"}"
        HMAC=$(hmac_sha256 "${BRIA_E2E_WEBHOOK_SECRET}" "$BODY")
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/hooks" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -H "X-Bria-Signature: $HMAC" \
            -d "$BODY"
        echo ""
        ;;

    file-file|file-sqlite)
        $BRIA_COMPOSE exec -T bria mkdir -p /tmp/bria/source
        $BRIA_COMPOSE exec -T bria sh -c "printf '%s\n' '{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}' > /tmp/bria/source/test.jsonl"
        ;;

    cron-file)
        echo "Waiting for cron tick (schedule every 5s)..."
        ;;

    pg-pg)
        $INFRA_COMPOSE exec -T postgres psql -U bria -d bria -c "
            INSERT INTO bria_jobs (id, payload, status) VALUES
            ('$JOB_ID', '{\"id\":\"$JOB_ID\",\"message\":\"hello pg-pg\"}', NULL)
            ON CONFLICT (id) DO NOTHING;" > /dev/null 2>&1
        ;;

    sqlite-file)
        $BRIA_COMPOSE exec -T bria sqlite3 /tmp/bria/source.db \
            "INSERT OR REPLACE INTO bria_jobs (id, payload, status) VALUES ('$JOB_ID', '{\"id\":\"$JOB_ID\",\"message\":\"hello sqlite-file\"}', NULL);"
        ;;

    queue-file)
        echo "{\"id\":\"$JOB_ID\",\"message\":\"hello queue-file\"}" | \
            $INFRA_COMPOSE exec -T amqp-helper python3 /scripts/amqp-helper.py publish bria job.submit
        ;;

    http-nonzero)
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        ;;

    http-413)
        HTTP_CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d '{"id":"test-413","message":"this body is way too long for the tiny limit"}')
        echo "HTTP status: $HTTP_CODE"
        echo "$HTTP_CODE" | assert_http_code "413" && OK=1
        if [ "${OK:-0}" -eq 1 ]; then echo ""; echo "=== PASS: $SCENARIO ==="; exit 0
        else echo ""; echo "=== FAIL: $SCENARIO ==="; exit 1; fi
        ;;

    webhook-hmac-401)
        BODY="{\"id\":\"$JOB_ID\",\"message\":\"hello webhook\"}"
        HTTP_CODE=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "${BRIA_E2E_BASE_URL}/hooks" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -H "X-Bria-Signature: 0000000000000000000000000000000000000000000000000000000000000000" \
            -d "$BODY")
        echo "HTTP status: $HTTP_CODE"
        echo "$HTTP_CODE" | assert_http_code "401" && OK=1
        if [ "${OK:-0}" -eq 1 ]; then echo ""; echo "=== PASS: $SCENARIO ==="; exit 0
        else echo ""; echo "=== FAIL: $SCENARIO ==="; exit 1; fi
        ;;

    http-cancel)
        BLOCKER_ID="e2e-cancel-blocker-$(date +%s)"
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$BLOCKER_ID\",\"message\":\"blocker\"}"
        echo ""
        sleep 1
        curl -sS -X POST "${BRIA_E2E_BASE_URL}/jobs" \
            -H 'content-type: application/json' \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}" \
            -d "{\"id\":\"$JOB_ID\",\"message\":\"hello $SCENARIO\"}"
        echo ""
        sleep 1
        CANCEL_RESP=$(curl -sS -X DELETE "${BRIA_E2E_BASE_URL}/jobs/$JOB_ID" \
            -H "x-bria-api-key: ${BRIA_E2E_API_KEY}")
        echo "Cancel response: $CANCEL_RESP"
        ;;

    http-condition-false)
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
# Verification (polling timeouts tightened — bria is already warm)
# ─────────────────────────────────────────────────────────────────────────────
echo "=== Waiting for pipeline result ==="
OK=0

case "$SCENARIO" in

    http-pg|webhook-pg|pg-pg)
        for i in $(seq 1 15); do
            RESULT=$($INFRA_COMPOSE exec -T postgres psql -U bria -d bria -t -A -c \
                "SELECT status, stdout FROM bria_results WHERE job_id = '$JOB_ID' LIMIT 1;" 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_pg_status success; then OK=1; break; fi
            sleep 1
        done
        ;;

    cron-file)
        # cron fires every 5s so we need up to ~10s after bria start
        for i in $(seq 1 20); do
            RESULT=$($BRIA_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_pipeline cron-file-pipeline success; then OK=1; break; fi
            sleep 1
        done
        ;;

    file-file|http-file|queue-file|sqlite-file|http-pg-recovery)
        for i in $(seq 1 15); do
            RESULT=$($BRIA_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_status success "$JOB_ID"; then OK=1; break; fi
            sleep 1
        done
        ;;

    file-sqlite|http-sqlite)
        for i in $(seq 1 15); do
            RESULT=$($BRIA_COMPOSE exec -T bria sh -c "sqlite3 /tmp/bria/results.db 'SELECT status, stdout FROM results LIMIT 1;'" 2>/dev/null || \
                     sqlite3 tmp/bria/results.db 'SELECT status, stdout FROM results LIMIT 1;' 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_sqlite_status success; then OK=1; break; fi
            sleep 1
        done
        ;;

    http-sse)
        for i in $(seq 1 15); do
            if grep -q '"status":"success"' tmp/sse.out 2>/dev/null; then break; fi
            sleep 1
        done
        kill $SSE_PID 2>/dev/null || true
        wait $SSE_PID 2>/dev/null || true
        RESULT=$(cat tmp/sse.out 2>/dev/null || true)
        if echo "$RESULT" | assert_sse_ok "$SCENARIO"; then OK=1; fi
        ;;

    http-queue)
        wait $CONSUME_PID 2>/dev/null || true
        RESULT=$(cat tmp/amqp.out 2>/dev/null || true)
        if echo "$RESULT" | assert_amqp_ok "$SCENARIO"; then OK=1; fi
        ;;

    http-webhook)
        # Logs come from the infra container now
        for i in $(seq 1 15); do
            RESULT=$($INFRA_COMPOSE logs --no-log-prefix webhook-echo 2>/dev/null || true)
            if echo "$RESULT" | python3 -c "
import json, sys
text = sys.stdin.read(); scenario = sys.argv[1]
for line in text.splitlines():
    line = line.strip()
    if not line: continue
    try:
        r = json.loads(line)
        body = json.loads(r.get('body', '{}'))
        j = body.get('job', {})
        if j.get('id', '').startswith('e2e-' + scenario):
            assert body.get('status') == 'success'
            sys.exit(0)
    except (json.JSONDecodeError, KeyError):
        continue
sys.exit(1)
" "$SCENARIO" 2>/dev/null; then OK=1; break; fi
            sleep 1
        done
        ;;

    http-nonzero)
        for i in $(seq 1 15); do
            RESULT=$($BRIA_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_status failure "$JOB_ID"; then OK=1; break; fi
            sleep 1
        done
        ;;

    http-cancel)
        for i in $(seq 1 20); do
            STATE=$($INFRA_COMPOSE exec -T postgres psql -U bria -d bria -t -A -c \
                "SELECT state FROM bria_job_state WHERE job_id = '$JOB_ID' LIMIT 1;" 2>/dev/null || true)
            if [ -n "$STATE" ] && echo "$STATE" | grep -q cancelled; then
                echo "Cancellation confirmed"
                OK=1; break
            fi
            sleep 1
        done
        ;;

    http-condition-false)
        for i in $(seq 1 15); do
            RESULT=$($BRIA_COMPOSE exec -T bria cat /tmp/bria/results.jsonl 2>/dev/null || cat tmp/bria/results.jsonl 2>/dev/null || true)
            if [ -n "$RESULT" ] && echo "$RESULT" | assert_jsonl_status failure "$JOB_ID"; then OK=1; break; fi
            sleep 1
        done
        ;;

    *)
        echo "ERROR: Unknown verify for $SCENARIO"
        ;;
esac

if [ "$OK" -eq 1 ]; then
    echo ""; echo "=== PASS: $SCENARIO ==="; exit 0
else
    echo ""; echo "=== FAIL: $SCENARIO ==="; exit 1
fi
