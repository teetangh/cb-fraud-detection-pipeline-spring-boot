#!/usr/bin/env bash
#
# T10 — full end-to-end smoke test against the LIVE docker-compose stack.
#
# Spec §12 is explicit: the build is not done until T1-T10 pass against the real
# stack, not only against Testcontainers-isolated tests. This is the difference
# between "the components work in isolation" and "the system works", and it is
# the script a human runs to demo it.
#
#   docker compose up -d && ./scripts/smoke-test.sh
#
# Exits non-zero if any scenario fails. Prints a scenario-by-scenario summary.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -f "${REPO_ROOT}/.env" ] && { set -a; . "${REPO_ROOT}/.env"; set +a; }

MOCK="http://localhost:${MOCK_PAYMENT_PORT:-8080}"
GATEWAY="http://localhost:${GATEWAY_PORT:-8081}"
INGESTION="http://localhost:${INGESTION_PORT:-8082}"
SCORING="http://localhost:${SCORING_PORT:-8084}"
CB_QUERY="http://localhost:${CB_QUERY_PORT:-8093}"
CB_AUTH="${CB_USER:-Administrator}:${CB_PASSWORD:-password}"

RED='\033[0;31m'; GRN='\033[0;32m'; YEL='\033[0;33m'; DIM='\033[2m'; NC='\033[0m'
PASS=0; FAIL=0
declare -a RESULTS

pass() { printf "  ${GRN}✓${NC} %s\n" "$1"; RESULTS+=("PASS|$1"); PASS=$((PASS+1)); }
fail() { printf "  ${RED}✗${NC} %s\n" "$1"; [ -n "${2:-}" ] && printf "      ${DIM}%s${NC}\n" "$2"; RESULTS+=("FAIL|$1"); FAIL=$((FAIL+1)); }
info() { printf "  ${DIM}%s${NC}\n" "$1"; }
head2() { printf "\n${YEL}%s${NC}\n" "$1"; }

n1ql() { curl -s -u "${CB_AUTH}" "${CB_QUERY}/query/service" \
           --data-urlencode "statement=$1" --data-urlencode "scan_consistency=request_plus"; }

# ── 0. Wait for the stack ────────────────────────────────────────────────────
head2 "0. Stack readiness"
for svc_url in "mock-payment-api ${MOCK}" "gateway ${GATEWAY}" "ingestion ${INGESTION}" "scoring ${SCORING}"; do
  name="${svc_url%% *}"; url="${svc_url##* }"
  ok=0
  for _ in $(seq 1 60); do
    if curl -sf "${url}/actuator/health/readiness" 2>/dev/null | grep -q '"status":"UP"'; then ok=1; break; fi
    sleep 2
  done
  [ "$ok" = 1 ] && pass "${name} ready" || fail "${name} not ready after 120s" "check: docker compose logs ${name}"
done
[ "$FAIL" -gt 0 ] && { printf "\n${RED}Stack is not up — aborting.${NC}\n"; exit 1; }

# ── 1. Infrastructure ────────────────────────────────────────────────────────
head2 "1. Infrastructure (spec §7, §8)"
TOPICS=$(docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh \
           --bootstrap-server localhost:9092 --list 2>/dev/null | grep -c '^fraud\.')
[ "${TOPICS}" = "9" ] && pass "9 Kafka topics exist" || fail "expected 9 topics, found ${TOPICS}"

for t in raw enriched scored decisioned actioned; do
  P=$(docker exec fraud-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --describe --topic "fraud.transactions.${t}" 2>/dev/null \
      | grep -oP 'PartitionCount: \K[0-9]+' | head -1)
  [ "${P}" = "6" ] || fail "fraud.transactions.${t} has ${P} partitions, expected 6"
done
pass "transaction topics have 6 partitions (customerId ordering — ADR-0012)"

RULES=$(n1ql 'SELECT RAW COUNT(*) FROM `fraud-detection`.`intelligence`.`fraud-rules` WHERE enabled = true' \
        | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ "${RULES}" = "8" ] && pass "8 seed rules queryable via N1QL" || fail "expected 8 enabled rules, found ${RULES:-0}"

# ── 2. T1 — clean allow path ─────────────────────────────────────────────────
head2 "2. T1 — clean allow path"
T1_TXN="txn-smoke-clean-$$-$RANDOM"
T1_CUST="cust-smoke-clean-$$-$RANDOM"
T1=$(curl -s -X POST "${MOCK}/payments/initiate" -H 'Content-Type: application/json' \
  -d "{\"transactionId\":\"${T1_TXN}\",\"customerId\":\"${T1_CUST}\",\"merchantId\":\"merch-1\",
       \"merchantCategoryCode\":\"5411\",\"amount\":150.00,\"currency\":\"INR\",
       \"countryCode\":\"IN\",\"deviceId\":\"dev-smoke-1\",\"paymentMethod\":\"CARD\"}")

T1_DECISION=$(jq -r '.fraudDecision // "none"' <<<"${T1}" 2>/dev/null)
T1_SCORE=$(jq -r '.riskScore // -1' <<<"${T1}" 2>/dev/null)
T1_SOURCE=$(jq -r '.resolvedBy // "none"' <<<"${T1}" 2>/dev/null)
T1_CORR=$(jq -r '.correlationId // ""' <<<"${T1}" 2>/dev/null)

[ "${T1_DECISION}" = "ALLOW" ] && pass "decision is ALLOW" || fail "decision was ${T1_DECISION}, expected ALLOW" "${T1}"
{ [ "${T1_SCORE}" -ge 0 ] && [ "${T1_SCORE}" -lt 30 ]; } \
  && pass "riskScore ${T1_SCORE} < 30" || fail "riskScore ${T1_SCORE}, expected < 30"

# THE assertion. Without it this passes even if the pipeline never answered and
# the gateway returned its default — which for a clean transaction could still
# have looked like a plausible result.
[ "${T1_SOURCE}" = "PIPELINE" ] \
  && pass "resolvedBy=PIPELINE (real decision, not the timeout default)" \
  || fail "resolvedBy=${T1_SOURCE}, expected PIPELINE" \
          "TIMEOUT_DEFAULT here means the Pub/Sub wakeup did not arrive within 150ms — see ADR-0003"

# ── 3. T2 — velocity escalation ──────────────────────────────────────────────
head2 "3. T2 — velocity-triggered escalation"
T2_CUST="cust-smoke-vel-$$-$RANDOM"
for i in $(seq 1 6); do
  T2=$(curl -s -X POST "${MOCK}/payments/initiate" -H 'Content-Type: application/json' \
    -d "{\"transactionId\":\"txn-smoke-vel-$$-${i}\",\"customerId\":\"${T2_CUST}\",
         \"merchantId\":\"merch-1\",\"merchantCategoryCode\":\"5411\",\"amount\":100.00,
         \"currency\":\"INR\",\"countryCode\":\"IN\",\"deviceId\":\"dev-smoke-vel\",
         \"paymentMethod\":\"CARD\"}")
done
T2_SCORE=$(jq -r '.riskScore // -1' <<<"${T2}" 2>/dev/null)
T2_DECISION=$(jq -r '.fraudDecision // "none"' <<<"${T2}" 2>/dev/null)
T2_TXN="txn-smoke-vel-$$-6"

# Assert against the DECISION RECORD, not the HTTP response: the response may
# legitimately be the timeout default under load, while the stored decision is
# always the real one.
sleep 3
T2_RULES=$(n1ql "SELECT RAW d.triggeredRules FROM \`fraud-detection\`.\`audit\`.\`decisions\` d WHERE d.transactionId = '${T2_TXN}'")
if grep -q 'VELOCITY_1M' <<<"${T2_RULES}"; then
  pass "VELOCITY_1M in triggeredRules on the 6th transaction"
else
  fail "VELOCITY_1M not triggered on the 6th transaction" "$(head -c 300 <<<"${T2_RULES}")"
fi
T2_STORED_SCORE=$(n1ql "SELECT RAW d.riskScore FROM \`fraud-detection\`.\`audit\`.\`decisions\` d WHERE d.transactionId = '${T2_TXN}'" \
                  | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ -n "${T2_STORED_SCORE}" ] && [ "${T2_STORED_SCORE}" -gt 0 ] \
  && pass "stored riskScore ${T2_STORED_SCORE} > 0 (derived from the live ruleset)" \
  || fail "stored riskScore was ${T2_STORED_SCORE:-none}"
info "HTTP response: decision=${T2_DECISION} score=${T2_SCORE}"

# ── 4. T3 — duplicate idempotency ────────────────────────────────────────────
head2 "4. T3 — duplicate request idempotency"
T3_TXN="txn-smoke-dup-$$-$RANDOM"
T3_CUST="cust-smoke-dup-$$-$RANDOM"
BODY="{\"transactionId\":\"${T3_TXN}\",\"customerId\":\"${T3_CUST}\",\"merchantId\":\"merch-1\",
       \"merchantCategoryCode\":\"5411\",\"amount\":99.00,\"currency\":\"INR\",
       \"countryCode\":\"IN\",\"deviceId\":\"dev-smoke-dup\",\"paymentMethod\":\"CARD\"}"
curl -s -X POST "${MOCK}/payments/initiate" -H 'Content-Type: application/json' -d "${BODY}" >/dev/null
curl -s -X POST "${MOCK}/payments/initiate" -H 'Content-Type: application/json' -d "${BODY}" >/dev/null
sleep 2
DUPES=$(n1ql "SELECT RAW COUNT(*) FROM \`fraud-detection\`.\`transactions\`.\`raw-transactions\` t WHERE t.transactionId = '${T3_TXN}'" \
        | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ "${DUPES}" = "1" ] \
  && pass "exactly ONE transaction record for a duplicated transactionId" \
  || fail "found ${DUPES:-0} records, expected exactly 1" "upsert() instead of insert()? (§9.1)"

OUTBOX=$(n1ql "SELECT RAW COUNT(*) FROM \`fraud-detection\`.\`transactions\`.\`outbox\` o WHERE o.payload.transactionId = '${T3_TXN}'" \
         | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ "${OUTBOX}" = "1" ] \
  && pass "exactly ONE outbox row (no double publish)" \
  || fail "found ${OUTBOX:-0} outbox rows, expected 1"

# ── 5. Explainability ────────────────────────────────────────────────────────
head2 "5. Explainability (spec §1, §11)"
if [ -n "${T1_CORR}" ] && [ "${T1_CORR}" != "null" ]; then
  SERVICES=$(docker compose logs --no-color 2>/dev/null | grep -F "${T1_CORR}" \
             | grep -oE 'fraud-(mock-payment-api|gateway|ingestion|enrichment|scoring|decision|audit)' \
             | sort -u | wc -l)
  [ "${SERVICES}" -ge 4 ] \
    && pass "correlationId traces across ${SERVICES} services in the logs" \
    || fail "correlationId found in only ${SERVICES} services" \
            "docker compose logs | grep ${T1_CORR}"
  info "trace: docker compose logs | grep ${T1_CORR}"
else
  fail "no correlationId returned to the caller"
fi

AUDIT=$(n1ql "SELECT RAW COUNT(*) FROM \`fraud-detection\`.\`audit\`.\`audit-ledger\` a WHERE a.transactionId = '${T1_TXN}'" \
        | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ -n "${AUDIT}" ] && [ "${AUDIT}" -ge 1 ] \
  && pass "append-only ledger has ${AUDIT} entr(y|ies) for the T1 transaction" \
  || fail "no audit-ledger entry for ${T1_TXN}"

# ── 6. Latency ───────────────────────────────────────────────────────────────
head2 "6. Latency (spec §11)"
LAT=$(curl -s "${MOCK}/actuator/prometheus" 2>/dev/null | grep -E '^payment_fraud_latency_seconds' | head -4)
if [ -n "${LAT}" ]; then
  COUNT=$(grep -oP 'payment_fraud_latency_seconds_count\{[^}]*\} \K[0-9.E+]+' <<<"${LAT}" | head -1)
  SUM=$(grep -oP 'payment_fraud_latency_seconds_sum\{[^}]*\} \K[0-9.E+]+' <<<"${LAT}" | head -1)
  pass "payment.fraud.latency recorded (${COUNT:-?} samples)"
  [ -n "${SUM}" ] && [ -n "${COUNT}" ] && \
    info "mean $(awk -v s="${SUM}" -v c="${COUNT}" 'BEGIN{printf "%.0f", (s/c)*1000}')ms  ${DIM}(note: -XX:TieredStopAtLevel=1 is set — remove before quoting p99)${NC}"
else
  fail "payment.fraud.latency not exported" "curl ${MOCK}/actuator/prometheus | grep payment_fraud"
fi

RESOLVED=$(curl -s "${GATEWAY}/actuator/prometheus" 2>/dev/null | grep -E '^fraud_gateway_decision_resolved_total')
if [ -n "${RESOLVED}" ]; then
  P=$(grep 'source="PIPELINE"' <<<"${RESOLVED}" | grep -oE '[0-9.]+$' | head -1)
  T=$(grep 'source="TIMEOUT_DEFAULT"' <<<"${RESOLVED}" | grep -oE '[0-9.]+$' | head -1)
  pass "resolvedBy split exported — PIPELINE=${P:-0} TIMEOUT_DEFAULT=${T:-0}"
  info "TIMEOUT_DEFAULT / total is the system's primary SLO"
else
  fail "fraud_gateway_decision_resolved_total not exported"
fi

# ── Summary ──────────────────────────────────────────────────────────────────
printf "\n%s\n" "────────────────────────────────────────────────────────────"
for r in "${RESULTS[@]}"; do
  s="${r%%|*}"; m="${r#*|}"
  [ "$s" = "PASS" ] && printf "  ${GRN}PASS${NC}  %s\n" "$m" || printf "  ${RED}FAIL${NC}  %s\n" "$m"
done
printf "%s\n" "────────────────────────────────────────────────────────────"
printf "  %d passed, %d failed\n\n" "$PASS" "$FAIL"

[ "$FAIL" -eq 0 ] && { printf "${GRN}T10 PASSED${NC} — the system works end to end.\n"; exit 0; }
printf "${RED}T10 FAILED${NC} — %d scenario(s) failed.\n" "$FAIL"; exit 1
