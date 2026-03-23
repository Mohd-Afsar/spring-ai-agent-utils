#!/usr/bin/env bash
# POST topology vertices to GraphDB createMultipleVertices API.
#
# Usage (local — no auth):
#   ./post_topology_to_graphdb.sh
#
# Usage (secured env — with JWT):
#   export GRAPHDB_TOKEN='your-jwt-here'   # token ONLY (no "Bearer " prefix)
#   ./post_topology_to_graphdb.sh
#
# Optional:
#   export GRAPHDB_BASE='http://localhost:9016'
#
# Optional:
#   GRAPHDB_CLIENT_CODE=8892df07-6d62-4ec4-9596-8f48574908ff
#   GRAPHDB_AUDIENCE=apim
#   INPUT_JSON=createMultipleVertices_from_db.json
#
# Security: do not commit real tokens. Prefer env vars / CI secrets.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_URL="${GRAPHDB_BASE:-http://localhost:9016}"
ENDPOINT="${BASE_URL}/topology/graphdb/createMultipleVertices"
INPUT_JSON="${INPUT_JSON:-${SCRIPT_DIR}/createMultipleVertices_from_db.json}"
CLIENT_CODE="${GRAPHDB_CLIENT_CODE:-8892df07-6d62-4ec4-9596-8f48574908ff}"
AUDIENCE="${GRAPHDB_AUDIENCE:-apim}"

if [[ -z "${GRAPHDB_TOKEN:-}" ]]; then
  echo "Note: GRAPHDB_TOKEN not set — omitting Authorization header (typical for local)." >&2
fi

if [[ ! -f "$INPUT_JSON" ]]; then
  echo "ERROR: File not found: $INPUT_JSON" >&2
  exit 1
fi

echo "POST $ENDPOINT"
echo "Body: $INPUT_JSON ($(wc -c < "$INPUT_JSON" | tr -d ' ') bytes)"

# Single arg array avoids "${EMPTY[@]}" + set -u failing on macOS Bash 3.2
CURL_ARGS=(
  -sS --fail-with-body
  --location
  --request POST "$ENDPOINT"
  --header 'accept: application/json, text/plain, */*'
  --header "audience: ${AUDIENCE}"
)
if [[ -n "${GRAPHDB_TOKEN:-}" ]]; then
  CURL_ARGS+=(--header "authorization: Bearer ${GRAPHDB_TOKEN}")
fi
CURL_ARGS+=(
  --header "client-code: ${CLIENT_CODE}"
  --header 'content-type: application/json'
  --header 'x-module: CORE'
  --data-binary "@${INPUT_JSON}"
)
curl "${CURL_ARGS[@]}"

echo ""
echo "Done."
