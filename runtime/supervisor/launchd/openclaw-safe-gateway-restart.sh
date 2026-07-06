#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: openclaw-safe-gateway-restart.sh [--dry-run] [--allow-local-a] <instance-id>

Safely restarts one launchd-managed OpenClaw gateway instance.
USAGE
}

DRY_RUN=0
ALLOW_LOCAL_A=0
INSTANCE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --allow-local-a) ALLOW_LOCAL_A=1; shift ;;
    -h|--help) usage; exit 0 ;;
    --) shift; break ;;
    -*) echo "ERROR: unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)
      if [[ -n "$INSTANCE" ]]; then echo "ERROR: multiple instance ids supplied" >&2; exit 2; fi
      INSTANCE="$1"; shift ;;
  esac
done

if [[ -z "$INSTANCE" ]]; then echo "ERROR: instance id is required" >&2; usage >&2; exit 2; fi
if [[ ! "$INSTANCE" =~ ^local-[a-z0-9-]+$ ]]; then echo "ERROR: unsafe instance id: $INSTANCE" >&2; exit 2; fi
if [[ "$INSTANCE" == "local-a" && "$ALLOW_LOCAL_A" != "1" ]]; then
  echo "ERROR: refusing to restart local-a without --allow-local-a" >&2
  exit 3
fi

LABEL="ai.aicos.openclaw.${INSTANCE}"
DOMAIN="gui/${UID}"
PLIST="${HOME}/Library/LaunchAgents/${LABEL}.plist"
STATE_DIR="/Users/avo/aicos/.runtime-home/openclaw/instances/${INSTANCE}/state"
CONFIG_PATH="${STATE_DIR}/openclaw.json"
LOG_DIR="${STATE_DIR}/logs"
OUT_LOG="${LOG_DIR}/launchd.out.log"
ERR_LOG="${LOG_DIR}/launchd.err.log"

run() {
  printf '+'
  for arg in "$@"; do printf ' %q' "$arg"; done
  printf '\n'
  if [[ "$DRY_RUN" == "0" ]]; then command "$@"; fi
}

require_file() { [[ -f "$1" ]] || { echo "ERROR: missing $2: $1" >&2; exit 4; }; }
require_file "$PLIST" "LaunchAgent plist"
require_file "$CONFIG_PATH" "OpenClaw config"

PLIST_LABEL=$(/usr/libexec/PlistBuddy -c 'Print :Label' "$PLIST" 2>/dev/null || true)
if [[ "$PLIST_LABEL" != "$LABEL" ]]; then echo "ERROR: plist label mismatch: ${PLIST_LABEL:-<missing>} != $LABEL" >&2; exit 4; fi
PLIST_INSTANCE=$(/usr/libexec/PlistBuddy -c 'Print :EnvironmentVariables:AICOS_OPENCLAW_INSTANCE_ID' "$PLIST" 2>/dev/null || true)
if [[ "$PLIST_INSTANCE" != "$INSTANCE" ]]; then echo "ERROR: plist instance mismatch: ${PLIST_INSTANCE:-<missing>} != $INSTANCE" >&2; exit 4; fi
PROGRAM=$(/usr/libexec/PlistBuddy -c 'Print :ProgramArguments:1' "$PLIST" 2>/dev/null || true)
if [[ -z "$PROGRAM" || ! -f "$PROGRAM" ]]; then echo "ERROR: gateway program missing: ${PROGRAM:-<missing>}" >&2; exit 4; fi
PORT=$(node -e "const fs=require('fs'); const c=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); console.log(c.gateway?.port ?? c.port ?? '')" "$CONFIG_PATH")
if [[ -z "$PORT" || ! "$PORT" =~ ^[0-9]+$ ]]; then echo "ERROR: could not determine gateway port from $CONFIG_PATH" >&2; exit 4; fi

printf 'Instance: %s\nLabel: %s\nPlist: %s\nProgram: %s\nPort: %s\nDry-run: %s\n' "$INSTANCE" "$LABEL" "$PLIST" "$PROGRAM" "$PORT" "$DRY_RUN"
launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1 || echo "WARN: service is not currently bootstrapped; will bootstrap from plist"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "Dry-run OK: would kickstart ${DOMAIN}/${LABEL} only."
  exit 0
fi

BEFORE_PID=$(launchctl print "${DOMAIN}/${LABEL}" 2>/dev/null | awk '/pid = / {print $3; exit}' || true)
echo "Before pid: ${BEFORE_PID:-<none>}"
if launchctl print "${DOMAIN}/${LABEL}" >/dev/null 2>&1; then
  run launchctl kickstart -k "${DOMAIN}/${LABEL}"
else
  run launchctl bootstrap "$DOMAIN" "$PLIST"
fi

DEADLINE=$((SECONDS + 45))
while (( SECONDS < DEADLINE )); do
  PID=$(launchctl print "${DOMAIN}/${LABEL}" 2>/dev/null | awk '/pid = / {print $3; exit}' || true)
  if [[ -n "${PID:-}" && "$PID" != "0" ]]; then
    if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | grep -q .; then
      echo "Restart OK: ${LABEL} pid=${PID} listening on port ${PORT}"
      exit 0
    fi
  fi
  sleep 1
done

echo "ERROR: ${LABEL} did not become healthy on port ${PORT}" >&2
[[ -f "$ERR_LOG" ]] && { echo "--- recent stderr ---" >&2; tail -40 "$ERR_LOG" >&2 || true; }
[[ -f "$OUT_LOG" ]] && { echo "--- recent stdout ---" >&2; tail -40 "$OUT_LOG" >&2 || true; }
exit 5
