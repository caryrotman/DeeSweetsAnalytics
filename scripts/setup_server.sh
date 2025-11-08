#!/usr/bin/env bash
#
# usage: ./scripts/setup_server.sh [/opt/DeeSweetsAnalytics]
# Installs system dependencies, clones/updates the DeeSweetsAnalytics repo,
# and prepares the Python virtual environment with all required packages.

set -euo pipefail

REPO_DIR="${1:-/opt/DeeSweetsAnalytics}"
REPO_URL="https://github.com/caryrotman/DeeSweetsAnalytics.git"

if [[ "${EUID}" -ne 0 ]]; then
  SUDO="sudo"
else
  SUDO=""
fi

echo "Installing system packages..."
if command -v apt-get >/dev/null 2>&1; then
  ${SUDO} apt-get update -y
  ${SUDO} apt-get install -y python3 python3-venv python3-pip git
elif command -v yum >/dev/null 2>&1; then
  ${SUDO} yum install -y python3 python3-virtualenv python3-pip git
else
  echo "Unsupported package manager. Install Python 3, pip, virtualenv, and git manually." >&2
  exit 1
fi

echo "Ensuring repository is present at ${REPO_DIR}..."
if [[ -d "${REPO_DIR}/.git" ]]; then
  git -C "${REPO_DIR}" fetch --all --prune
  git -C "${REPO_DIR}" checkout main
  git -C "${REPO_DIR}" pull --ff-only origin main
else
  ${SUDO} mkdir -p "${REPO_DIR}"
  ${SUDO} chown -R "$USER":"$USER" "${REPO_DIR}"
  git clone "${REPO_URL}" "${REPO_DIR}"
fi

echo "Creating/updating Python virtual environment..."
cd "${REPO_DIR}"
python3 -m venv webapp/.venv
source webapp/.venv/bin/activate
pip install --upgrade pip wheel
pip install -r webapp/requirements.txt
deactivate

cat <<'NOTICE'

Setup complete.
- Copy your Google service-account JSON file onto the server (outside the repo).
- Export GOOGLE_APPLICATION_CREDENTIALS to point to that file before starting the app.
- To launch the web server, run: ./scripts/start_server.sh [repo-path] [service-account-json]

NOTICE

