#!/usr/bin/env bash
#
# usage: ./scripts/start_server.sh [/opt/DeeSweetsAnalytics] [/path/to/service_account.json]
# Activates the project virtualenv, exports GOOGLE_APPLICATION_CREDENTIALS,
# and starts the Flask application.

set -euo pipefail

REPO_DIR="${1:-/opt/DeeSweetsAnalytics}"
SERVICE_ACCOUNT="${2:-${GOOGLE_APPLICATION_CREDENTIALS:-}}"
HOST="${APP_HOST:-0.0.0.0}"
PORT="${APP_PORT:-5000}"

if [[ ! -d "${REPO_DIR}" ]]; then
  echo "Repository directory '${REPO_DIR}' not found. Run setup_server.sh first." >&2
  exit 1
fi

if [[ -z "${SERVICE_ACCOUNT}" ]]; then
  echo "Provide the Google service-account JSON path as the second argument or set GOOGLE_APPLICATION_CREDENTIALS." >&2
  exit 1
fi

if [[ ! -f "${SERVICE_ACCOUNT}" ]]; then
  echo "Service-account file '${SERVICE_ACCOUNT}' not found." >&2
  exit 1
fi

if command -v realpath >/dev/null 2>&1; then
  export GOOGLE_APPLICATION_CREDENTIALS="$(realpath "${SERVICE_ACCOUNT}")"
else
  export GOOGLE_APPLICATION_CREDENTIALS="$(cd "$(dirname "${SERVICE_ACCOUNT}")" && pwd)/$(basename "${SERVICE_ACCOUNT}")"
fi
export FLASK_DEBUG=0
export APP_HOST="${HOST}"
export APP_PORT="${PORT}"

cd "${REPO_DIR}"
source webapp/.venv/bin/activate

echo "Starting Dee Sweets Analytics Explorer on ${HOST}:${PORT}..."
exec python webapp/app.py

