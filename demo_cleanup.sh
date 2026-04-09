#!/usr/bin/env bash
set -euo pipefail

SESSION_NAME="sdms-demo"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_FILE="${ROOT_DIR}/data/static_tables.db"
KAFKA_LOG_DIR="${ROOT_DIR}/kafka_2.13-3.6.1/logs"
KEEP_KAFKA_STATE="false"

if [[ "${1:-}" == "--keep-kafka-state" ]]; then
  KEEP_KAFKA_STATE="true"
fi

echo "== SDMS Demo Cleanup =="

if command -v tmux >/dev/null 2>&1; then
  if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
    tmux kill-session -t "${SESSION_NAME}"
    echo "Stopped tmux session: ${SESSION_NAME}"
  else
    echo "No tmux session found: ${SESSION_NAME}"
  fi
else
  echo "tmux not installed; skipping tmux session cleanup"
fi

if [[ -f "${DB_FILE}" ]]; then
  rm -f "${DB_FILE}"
  echo "Removed demo SQLite DB: ${DB_FILE}"
else
  echo "No demo SQLite DB found at: ${DB_FILE}"
fi

if [[ "${KEEP_KAFKA_STATE}" == "false" ]]; then
  rm -rf /tmp/kafka-logs /tmp/zookeeper
  echo "Removed Kafka/Zookeeper state dirs: /tmp/kafka-logs, /tmp/zookeeper"

  if [[ -d "${KAFKA_LOG_DIR}" ]]; then
    rm -rf "${KAFKA_LOG_DIR:?}"/*
    echo "Cleared Kafka distribution logs: ${KAFKA_LOG_DIR}"
  fi
else
  echo "Keeping Kafka state/logs (requested via --keep-kafka-state)"
fi

find "${ROOT_DIR}" -type d -name "__pycache__" -prune -exec rm -rf {} + >/dev/null 2>&1 || true
echo "Removed Python __pycache__ directories"

echo
echo "Cleanup complete."
echo "You can run the demo again with:"
echo "  ./demo.sh normal"
echo "or"
echo "  ./demo.sh join"
