#!/usr/bin/env bash
#
# Initialises Couchbase for the fraud pipeline: cluster, bucket, 3 scopes,
# 7 collections, 3 GSI indexes, and seed data.
#
# IDEMPOTENT AND RETRY-AWARE, deliberately. Bucket, scope and collection
# creation in Couchbase are ASYNCHRONOUS — creating a collection immediately
# after its scope fails if the cluster has not caught up yet. Every step here
# retries and tolerates "already exists"; nothing assumes the previous step is
# visible yet.
#
# Seed data uses INSERT (not UPSERT) for rules and policy, so re-running does
# NOT clobber an edit a fraud analyst made. MCC reference data uses UPSERT,
# because it is reference data nobody edits by hand.

set -uo pipefail

CB_HOST="${CB_HOST:-couchbase}"
CB_USER="${CB_USER:-Administrator}"
CB_PASSWORD="${CB_PASSWORD:-password}"
CB_BUCKET="${CB_BUCKET:-fraud-detection}"
CB_RAM_DATA="${CB_RAM_DATA:-512}"
CB_RAM_INDEX="${CB_RAM_INDEX:-256}"

CLI="/opt/couchbase/bin/couchbase-cli"
REST="http://${CB_HOST}:8091"
QUERY="http://${CB_HOST}:8093/query/service"
AUTH="${CB_USER}:${CB_PASSWORD}"

log()  { echo "[couchbase-init] $*"; }
fail() { echo "[couchbase-init] FATAL: $*" >&2; exit 1; }

# Run a N1QL statement. Echoes the response body. Never aborts the script —
# callers decide what an error means, because "already exists" is success here.
n1ql() {
  curl -s -u "${AUTH}" "${QUERY}" --data-urlencode "statement=$1" "${@:2}"
}

# Extract a single scalar from a `SELECT RAW <number>` response.
# The query service pretty-prints, so the value sits on its own line after
# `"results": [` — a regex over the raw body never matches it. Strip whitespace
# first. (This silently broke the index-readiness check on the first run: it
# always read empty, so the wait fell through to a WARN every time.)
n1ql_scalar() {
  n1ql "$1" | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$'
}

# retry <attempts> <sleep> <label> <command...>
# The label is for readability at the call site; failures are reported by the
# caller, which has more context than this helper does.
retry() {
  local attempts=$1 delay=$2; shift 3
  for i in $(seq 1 "${attempts}"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    [ "$i" -eq "${attempts}" ] && return 1
    sleep "${delay}"
  done
}

# ─────────────────────────────────────────────────────────────────────────
log "Waiting for Couchbase REST at ${REST}..."
retry 60 2 "rest" curl -sf "${REST}/pools" \
  || fail "Couchbase REST never became reachable."
log "REST reachable."

# ─────────────────────────────────────────────────────────────────────────
# 1. Cluster init. Errors if already initialised — that is a success case.
log "Initialising cluster (data=${CB_RAM_DATA}MB index=${CB_RAM_INDEX}MB)..."
init_out=$("${CLI}" cluster-init \
  --cluster "${CB_HOST}" \
  --cluster-username "${CB_USER}" \
  --cluster-password "${CB_PASSWORD}" \
  --services data,index,query \
  --cluster-ramsize "${CB_RAM_DATA}" \
  --cluster-index-ramsize "${CB_RAM_INDEX}" \
  --index-storage-setting default 2>&1)

if echo "${init_out}" | grep -qiE "already initialized|already provisioned|cluster is already"; then
  log "Cluster already initialised — continuing."
elif echo "${init_out}" | grep -qiE "^ERROR|SUCCESS" ; then
  echo "${init_out}" | sed 's/^/[couchbase-init]   /'
else
  echo "${init_out}" | sed 's/^/[couchbase-init]   /'
fi

log "Waiting for cluster to report healthy..."
retry 60 2 "pools/default" curl -sf -u "${AUTH}" "${REST}/pools/default" \
  || fail "Cluster did not become healthy."

# ─────────────────────────────────────────────────────────────────────────
# 2. Bucket
if curl -sf -u "${AUTH}" "${REST}/pools/default/buckets/${CB_BUCKET}" >/dev/null 2>&1; then
  log "Bucket '${CB_BUCKET}' exists."
else
  log "Creating bucket '${CB_BUCKET}'..."
  "${CLI}" bucket-create \
    --cluster "${CB_HOST}" -u "${CB_USER}" -p "${CB_PASSWORD}" \
    --bucket "${CB_BUCKET}" \
    --bucket-type couchbase \
    --bucket-ramsize "${CB_RAM_DATA}" \
    --bucket-replica 0 \
    --wait 2>&1 | sed 's/^/[couchbase-init]   /'
fi

retry 60 2 "bucket ready" curl -sf -u "${AUTH}" "${REST}/pools/default/buckets/${CB_BUCKET}" \
  || fail "Bucket '${CB_BUCKET}' did not become ready."
log "Bucket ready."

# ─────────────────────────────────────────────────────────────────────────
# 3. Scopes. Async — hence the retry on each.
SCOPES=(transactions intelligence audit)
for scope in "${SCOPES[@]}"; do
  if curl -sf -u "${AUTH}" "${REST}/pools/default/buckets/${CB_BUCKET}/scopes" 2>/dev/null \
       | grep -q "\"name\":\"${scope}\""; then
    log "scope exists: ${scope}"
  else
    log "creating scope: ${scope}"
    retry 15 2 "scope ${scope}" \
      "${CLI}" collection-manage --cluster "${CB_HOST}" -u "${CB_USER}" -p "${CB_PASSWORD}" \
        --bucket "${CB_BUCKET}" --create-scope "${scope}" \
      || log "  (scope ${scope} may already exist — continuing)"
  fi
done

# Wait for all three scopes to actually be visible before creating collections
# in them. This is the specific race the Couchbase docs warn about.
log "Waiting for scopes to become visible..."
for i in $(seq 1 30); do
  visible=$(curl -sf -u "${AUTH}" "${REST}/pools/default/buckets/${CB_BUCKET}/scopes" 2>/dev/null)
  ok=1
  for scope in "${SCOPES[@]}"; do
    echo "${visible}" | grep -q "\"name\":\"${scope}\"" || ok=0
  done
  [ "${ok}" -eq 1 ] && { log "All scopes visible."; break; }
  [ "$i" -eq 30 ] && fail "Scopes did not become visible."
  sleep 2
done

# ─────────────────────────────────────────────────────────────────────────
# 4. Collections
COLLECTIONS=(
  "transactions.raw-transactions"
  "transactions.outbox"
  "intelligence.fraud-rules"
  "intelligence.customer-profiles"
  "audit.decisions"
  "audit.audit-ledger"
  "audit.cases"
)

for entry in "${COLLECTIONS[@]}"; do
  scope="${entry%%.*}"
  coll="${entry#*.}"
  if curl -sf -u "${AUTH}" "${REST}/pools/default/buckets/${CB_BUCKET}/scopes" 2>/dev/null \
       | grep -q "\"name\":\"${coll}\""; then
    log "collection exists: ${entry}"
  else
    log "creating collection: ${entry}"
    retry 15 2 "collection ${entry}" \
      "${CLI}" collection-manage --cluster "${CB_HOST}" -u "${CB_USER}" -p "${CB_PASSWORD}" \
        --bucket "${CB_BUCKET}" --create-collection "${entry}" \
      || log "  (collection ${entry} may already exist — continuing)"
  fi
done

log "Waiting for collections to become visible..."
for i in $(seq 1 30); do
  visible=$(curl -sf -u "${AUTH}" "${REST}/pools/default/buckets/${CB_BUCKET}/scopes" 2>/dev/null)
  ok=1
  for entry in "${COLLECTIONS[@]}"; do
    echo "${visible}" | grep -q "\"name\":\"${entry#*.}\"" || ok=0
  done
  [ "${ok}" -eq 1 ] && { log "All collections visible."; break; }
  [ "$i" -eq 30 ] && fail "Collections did not become visible."
  sleep 2
done

# ─────────────────────────────────────────────────────────────────────────
# 5. Query service, then indexes.
log "Waiting for query service..."
for i in $(seq 1 60); do
  if n1ql "SELECT 1 AS ok" | grep -q '"status": *"success"'; then
    log "Query service ready."
    break
  fi
  [ "$i" -eq 60 ] && fail "Query service never became ready."
  sleep 2
done

log "Creating indexes..."
# Note: no primary indexes anywhere. The integration suite asserts via EXPLAIN
# that queries use IndexScan and not PrimaryScan; a primary index sitting here
# would let a badly-planned query pass that assertion by accident.
create_index() {
  local name=$1 stmt=$2
  local out; out=$(n1ql "${stmt}")
  if echo "${out}" | grep -q '"status": *"success"'; then
    log "  created: ${name}"
  elif echo "${out}" | grep -qi "already exist"; then
    log "  exists:  ${name}"
  else
    log "  FAILED:  ${name}"
    echo "${out}" | sed 's/^/[couchbase-init]     /'
    return 1
  fi
}

create_index "idx_rules_enabled" \
  "CREATE INDEX idx_rules_enabled ON \`${CB_BUCKET}\`.\`intelligence\`.\`fraud-rules\`(enabled, category)" || exit 1
create_index "idx_decisions_customer" \
  "CREATE INDEX idx_decisions_customer ON \`${CB_BUCKET}\`.\`audit\`.\`decisions\`(customerId, decisionAt DESC)" || exit 1
create_index "idx_outbox_pending" \
  "CREATE INDEX idx_outbox_pending ON \`${CB_BUCKET}\`.\`transactions\`.\`outbox\`(status) WHERE status = \"PENDING\"" || exit 1
create_index "idx_mcc_lookup" \
  "CREATE INDEX idx_mcc_lookup ON \`${CB_BUCKET}\`.\`intelligence\`.\`customer-profiles\`(mcc) WHERE docType = \"MCC_RISK\"" || exit 1

log "Waiting for indexes to come online..."
for i in $(seq 1 60); do
  pending=$(n1ql_scalar "SELECT RAW COUNT(*) FROM system:indexes WHERE state != 'online' AND bucket_id = '${CB_BUCKET}'")
  [ "${pending:-1}" = "0" ] && { log "All indexes online."; break; }
  [ "$i" -eq 60 ] && log "WARN: some indexes still building after 120s — continuing."
  sleep 2
done

# ─────────────────────────────────────────────────────────────────────────
# 6. Seed data.
#
# Rules and policy use INSERT so a re-run never overwrites a fraud analyst's
# edit. A duplicate-key error here means "already seeded" and is success.
seed_insert() {
  local label=$1 stmt=$2 param=$3 file=$4
  local out
  out=$(curl -s -u "${AUTH}" "${QUERY}" \
          --data-urlencode "statement=${stmt}" \
          --data-urlencode "\$${param}=$(cat "${file}")")
  if echo "${out}" | grep -q '"status": *"success"'; then
    log "  seeded: ${label}"
  elif echo "${out}" | grep -qiE "duplicate key|already exists"; then
    log "  exists: ${label} (left untouched — an ops edit must survive a re-run)"
  else
    log "  FAILED: ${label}"
    echo "${out}" | sed 's/^/[couchbase-init]     /'
    return 1
  fi
}

# N1QL INSERT-SELECT syntax note: the key and value EXPRESSIONS go inside the
# parentheses as `(KEY k, VALUE v)`, bound to aliases produced by the SELECT.
# The intuitive `(KEY, VALUE) SELECT keyExpr, valExpr` is a syntax error —
# "at: SELECT (reserved word)" — which is what this script failed on first run.
log "Seeding..."
seed_insert "8 fraud rules" \
  "INSERT INTO \`${CB_BUCKET}\`.\`intelligence\`.\`fraud-rules\` (KEY k, VALUE v) SELECT 'rule::' || r.ruleId AS k, r AS v FROM \$rules AS r" \
  "rules" /init/seed-fraud-rules.json || exit 1

seed_insert "decision policy" \
  "INSERT INTO \`${CB_BUCKET}\`.\`intelligence\`.\`fraud-rules\` (KEY k, VALUE v) SELECT 'policy::default' AS k, p AS v FROM \$policy AS p" \
  "policy" /init/seed-policy.json || exit 1

# MCC risk is reference data, not ops-edited — UPSERT so corrections land.
seed_insert "MCC risk table" \
  "UPSERT INTO \`${CB_BUCKET}\`.\`intelligence\`.\`customer-profiles\` (KEY k, VALUE v) SELECT 'mcc::' || m.mcc AS k, m AS v FROM \$mcc AS m" \
  "mcc" /init/seed-mcc-risk.json || exit 1

# ─────────────────────────────────────────────────────────────────────────
# 7. Verify, rather than trusting the writes above.
# REQUEST_PLUS: these run immediately after the writes above, and N1QL defaults
# to NOT_BOUNDED scan consistency — a read-after-write here would legitimately
# return 0 and fail a correct init.
log "Verifying..."
CONSISTENT=(--data-urlencode "scan_consistency=request_plus")

rule_count=$(n1ql "SELECT RAW COUNT(*) FROM \`${CB_BUCKET}\`.\`intelligence\`.\`fraud-rules\` WHERE enabled = true" \
             "${CONSISTENT[@]}" | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
if [ "${rule_count:-0}" != "8" ]; then
  fail "Expected 8 enabled rules, found ${rule_count:-0}."
fi
log "  8 enabled rules queryable via N1QL."

policy_ok=$(n1ql "SELECT RAW COUNT(*) FROM \`${CB_BUCKET}\`.\`intelligence\`.\`fraud-rules\` WHERE docType = 'DECISION_POLICY'" \
            "${CONSISTENT[@]}" | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ "${policy_ok:-0}" = "1" ] || fail "Decision policy not found."
log "  decision policy present."

mcc_count=$(n1ql "SELECT RAW COUNT(*) FROM \`${CB_BUCKET}\`.\`intelligence\`.\`customer-profiles\` WHERE docType = 'MCC_RISK'" \
            "${CONSISTENT[@]}" | tr -d ' \n' | grep -oE '"results":\[[0-9]+' | grep -oE '[0-9]+$')
[ "${mcc_count:-0}" -ge 10 ] || fail "Expected 10 MCC risk rows, found ${mcc_count:-0}."
log "  ${mcc_count} MCC risk rows present."

log "Couchbase initialisation complete."
