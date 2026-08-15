#!/usr/bin/env bash
#
# Creates every topic from spec §7 with its exact partition count.
#
# Partition counts are deliberate, not defaults:
#   6 on the transaction topics — caps consumer parallelism at 6 per group, and
#     divides evenly by 1, 2, 3 and 6 so common scaling steps stay balanced.
#     Effectively permanent: repartitioning a stateful keyed topic breaks
#     velocity continuity mid-flight (ADR-0012).
#   3 on alerts and the DLQs — lower volume, no ordering requirement beyond
#     their key.
#
# RF=1 throughout: one broker (ADR-0013).
# Idempotent — re-running is a no-op.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

RETENTION_7D=$((7 * 24 * 60 * 60 * 1000))
RETENTION_30D=$((30 * 24 * 60 * 60 * 1000))
RETENTION_1D=$((1 * 24 * 60 * 60 * 1000))

# topic : partitions : retention_ms
TOPICS=(
  "fraud.transactions.raw:6:${RETENTION_7D}"
  "fraud.transactions.enriched:6:${RETENTION_7D}"
  "fraud.transactions.scored:6:${RETENTION_7D}"
  "fraud.transactions.decisioned:6:${RETENTION_30D}"
  "fraud.transactions.actioned:6:${RETENTION_30D}"
  "fraud.alerts.realtime:3:${RETENTION_1D}"
  "fraud.transactions.raw.dlq:3:${RETENTION_30D}"
  "fraud.transactions.enriched.dlq:3:${RETENTION_30D}"
  "fraud.transactions.scored.dlq:3:${RETENTION_30D}"
)

log() { echo "[kafka-init] $*"; }

log "Waiting for broker at ${BOOTSTRAP}..."
for i in $(seq 1 60); do
  if "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list >/dev/null 2>&1; then
    log "Broker reachable."
    break
  fi
  if [ "$i" -eq 60 ]; then
    log "FATAL: broker not reachable after 60 attempts."
    exit 1
  fi
  sleep 2
done

for entry in "${TOPICS[@]}"; do
  IFS=':' read -r topic partitions retention <<< "${entry}"

  if "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --describe --topic "${topic}" >/dev/null 2>&1; then
    log "exists: ${topic}"
  else
    log "creating: ${topic} (partitions=${partitions}, rf=1)"
    "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
      --create \
      --topic "${topic}" \
      --partitions "${partitions}" \
      --replication-factor 1 \
      --config "retention.ms=${retention}" \
      --config "min.insync.replicas=1"
  fi
done

# Verify what was actually created rather than trusting the create calls.
# A topic that exists with the wrong partition count is worse than a missing
# one: consumer parallelism would be silently capped, and on a keyed topic the
# ordering guarantee velocity detection depends on would still hold, so nothing
# would look broken.
log "Verifying partition counts..."
failed=0
for entry in "${TOPICS[@]}"; do
  IFS=':' read -r topic expected _ <<< "${entry}"
  actual=$("${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" \
             --describe --topic "${topic}" 2>/dev/null \
           | awk -F'PartitionCount: ' 'NF>1 {split($2, a, "\t"); print a[1]; exit}' \
           | tr -d ' ')
  if [ "${actual}" != "${expected}" ]; then
    log "MISMATCH: ${topic} has ${actual:-<none>} partitions, expected ${expected}"
    failed=1
  else
    log "  ok: ${topic} (${actual} partitions)"
  fi
done

if [ "${failed}" -ne 0 ]; then
  log "FATAL: partition count verification failed."
  exit 1
fi

log "All ${#TOPICS[@]} topics present with correct partition counts."
