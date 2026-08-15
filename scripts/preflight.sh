#!/usr/bin/env bash
#
# Fails loudly BEFORE you start containers, rather than halfway through a
# Couchbase startup with a full disk.
#
# The full stack needs roughly 4.6 GB RAM and 4 GB of images. Running out of
# either mid-build produces confusing failures: Couchbase silently refusing to
# allocate its memory quota, or an image pull dying partway and leaving a
# corrupt layer cache.
#
#   ./scripts/preflight.sh          # full stack requirements
#   ./scripts/preflight.sh infra    # infra only (Phase 1) — lower bar

set -uo pipefail

SCOPE="${1:-full}"

# Check the ports compose will ACTUALLY bind, not the defaults. Without this,
# preflight reports a conflict on a port that an .env override has already
# moved away from — a false alarm that trains people to ignore preflight.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -f "${REPO_ROOT}/.env" ]; then
  # shellcheck disable=SC1091  # path is dynamic; file is optional
  set -a; . "${REPO_ROOT}/.env"; set +a
  ENV_NOTE=" (host ports from .env)"
else
  ENV_NOTE=""
fi

KAFKA_HOST_PORT="${KAFKA_HOST_PORT:-29092}"
REDIS_HOST_PORT="${REDIS_HOST_PORT:-6379}"
CB_UI_PORT="${CB_UI_PORT:-8091}"
CB_QUERY_PORT="${CB_QUERY_PORT:-8093}"
CB_KV_PORT="${CB_KV_PORT:-11210}"

case "${SCOPE}" in
  infra) REQ_RAM_MB=2400; REQ_DISK_GB=4 ;;
  core)  REQ_RAM_MB=3400; REQ_DISK_GB=5 ;;
  full)  REQ_RAM_MB=4600; REQ_DISK_GB=6 ;;
  *) echo "usage: $0 [infra|core|full]" >&2; exit 2 ;;
esac

RED='\033[0;31m'; GRN='\033[0;32m'; YEL='\033[0;33m'; NC='\033[0m'
fails=0
warns=0

ok()   { printf "  ${GRN}✓${NC} %s\n" "$1"; }
warn() { printf "  ${YEL}!${NC} %s\n" "$1"; warns=$((warns+1)); }
bad()  { printf "  ${RED}✗${NC} %s\n" "$1"; fails=$((fails+1)); }

echo "Preflight — scope: ${SCOPE} (needs ~${REQ_RAM_MB}MB RAM, ~${REQ_DISK_GB}GB disk)"
echo

# ── Docker ───────────────────────────────────────────────────────────────
echo "Docker"
if command -v docker >/dev/null 2>&1; then
  ok "docker present ($(docker --version | awk '{print $3}' | tr -d ,))"
else
  bad "docker not found"
fi

if docker compose version >/dev/null 2>&1; then
  ok "compose v2 present ($(docker compose version --short 2>/dev/null))"
else
  bad "docker compose v2 not found"
fi

if docker info >/dev/null 2>&1; then
  ok "docker daemon reachable"
else
  bad "docker daemon not reachable — is it running, and are you in the docker group?"
fi
echo

# ── Memory ───────────────────────────────────────────────────────────────
# 'available' is the number that matters, not 'free': free excludes reclaimable
# page cache, so it understates what a container can actually get.
echo "Memory"
if avail_mb=$(free -m 2>/dev/null | awk '/^Mem:/ {print $7}') && [ -n "${avail_mb}" ]; then
  total_mb=$(free -m | awk '/^Mem:/ {print $2}')
  if [ "${avail_mb}" -ge "${REQ_RAM_MB}" ]; then
    ok "${avail_mb}MB available of ${total_mb}MB total"
  else
    bad "${avail_mb}MB available of ${total_mb}MB — need ~${REQ_RAM_MB}MB. Close something, or run a smaller scope."
  fi
else
  warn "could not read memory (non-Linux?) — skipping"
fi
echo

# ── Disk ─────────────────────────────────────────────────────────────────
echo "Disk"
docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null || echo /var/lib/docker)
[ -d "${docker_root}" ] || docker_root=/
if avail_gb=$(df -BG --output=avail "${docker_root}" 2>/dev/null | tail -1 | tr -dc '0-9'); then
  pct=$(df --output=pcent "${docker_root}" 2>/dev/null | tail -1 | tr -dc '0-9')
  if [ "${avail_gb}" -ge "${REQ_DISK_GB}" ]; then
    ok "${avail_gb}GB free on ${docker_root} (${pct}% used)"
  else
    bad "${avail_gb}GB free on ${docker_root} (${pct}% used) — need ~${REQ_DISK_GB}GB"
    echo "      try: docker system prune -a --volumes   (destroys Couchbase data)"
  fi
  [ "${pct:-0}" -ge 90 ] && warn "filesystem is ${pct}% full — image pulls may fail unpredictably"
else
  warn "could not read disk usage — skipping"
fi
echo

# ── Ports ────────────────────────────────────────────────────────────────
echo "Ports${ENV_NOTE}"
check_port() {
  local port=$1 what=$2
  if command -v ss >/dev/null 2>&1 && ss -ltn "sport = :${port}" 2>/dev/null | grep -q ":${port}"; then
    bad "port ${port} (${what}) already in use — override it in .env (see .env.example)"
  else
    ok "port ${port} free (${what})"
  fi
}
check_port "${KAFKA_HOST_PORT}" "kafka host listener"
check_port "${REDIS_HOST_PORT}" "redis"
check_port "${CB_UI_PORT}"      "couchbase web console"
check_port "${CB_QUERY_PORT}"   "couchbase query service"
check_port "${CB_KV_PORT}"      "couchbase kv"
if [ "${SCOPE}" != "infra" ]; then
  for p in 8080 8081 8082 8083 8084 8085 8086; do check_port "$p" "service"; done
fi
echo

# ── Toolchain ────────────────────────────────────────────────────────────
echo "Toolchain"
if command -v java >/dev/null 2>&1; then
  jv=$(java -version 2>&1 | head -1 | grep -oE '"[0-9]+' | tr -d '"')
  if [ "${jv:-0}" -ge 21 ]; then ok "java ${jv}"; else bad "java ${jv} — need 21+"; fi
else
  bad "java not found — need 21+"
fi
command -v curl >/dev/null 2>&1 && ok "curl present" || bad "curl not found (smoke tests need it)"
command -v jq   >/dev/null 2>&1 && ok "jq present"   || warn "jq not found — smoke test output will be raw JSON"
# Maven is intentionally not required: every module ships a script-only wrapper.
command -v mvn  >/dev/null 2>&1 && ok "maven present" || ok "maven absent — using ./mvnw (expected)"
echo

# ── Verdict ──────────────────────────────────────────────────────────────
if [ "${fails}" -gt 0 ]; then
  printf "${RED}FAILED${NC} — %d blocking issue(s), %d warning(s).\n" "${fails}" "${warns}"
  echo "Fix the above before starting containers."
  exit 1
fi

if [ "${warns}" -gt 0 ]; then
  printf "${YEL}OK with %d warning(s).${NC}\n" "${warns}"
else
  printf "${GRN}OK${NC} — ready.\n"
fi
