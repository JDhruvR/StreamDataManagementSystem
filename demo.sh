#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-normal}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_DIR="${ROOT_DIR}/kafka_2.13-3.6.1"
SESSION_NAME="sdms-demo"

if [[ ! -d "${KAFKA_DIR}" ]]; then
  echo "Kafka folder not found at: ${KAFKA_DIR}"
  echo "Download/extract Kafka first (see demo.txt)."
  exit 1
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required for demo.sh"
  echo "Install tmux or follow manual steps in demo.txt"
  exit 1
fi

if tmux has-session -t "${SESSION_NAME}" 2>/dev/null; then
  echo "Session ${SESSION_NAME} already exists."
  echo "Run: tmux kill-session -t ${SESSION_NAME}"
  exit 1
fi

tmux new-session -d -s "${SESSION_NAME}" -n infra
tmux send-keys -t "${SESSION_NAME}:infra.0" "cd \"${KAFKA_DIR}\" && bin/zookeeper-server-start.sh config/zookeeper.properties" C-m

tmux split-window -h -t "${SESSION_NAME}:infra"
tmux send-keys -t "${SESSION_NAME}:infra.1" "cd \"${KAFKA_DIR}\" && bin/kafka-server-start.sh config/server.properties" C-m

tmux split-window -v -t "${SESSION_NAME}:infra.0"
tmux send-keys -t "${SESSION_NAME}:infra.2" "cd \"${ROOT_DIR}\" && python -m sensors.pollution_sensor" C-m

if [[ "${MODE}" == "join" ]]; then
  tmux split-window -v -t "${SESSION_NAME}:infra.1"
  tmux send-keys -t "${SESSION_NAME}:infra.3" "cd \"${KAFKA_DIR}\" && bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic weather_stream" C-m
fi

tmux new-window -t "${SESSION_NAME}" -n app
tmux send-keys -t "${SESSION_NAME}:app.0" "cd \"${ROOT_DIR}\" && python -m examples.cli" C-m

tmux split-window -h -t "${SESSION_NAME}:app"
if [[ "${MODE}" == "join" ]]; then
  tmux send-keys -t "${SESSION_NAME}:app.1" "cd \"${KAFKA_DIR}\" && bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic joined_out" C-m
else
  tmux send-keys -t "${SESSION_NAME}:app.1" "cd \"${KAFKA_DIR}\" && bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out" C-m
fi

tmux select-window -t "${SESSION_NAME}:app"

echo "Started tmux session: ${SESSION_NAME}"
echo "Mode: ${MODE}"
echo
echo "Attach with: tmux attach -t ${SESSION_NAME}"
echo "Stop with:   tmux kill-session -t ${SESSION_NAME}"
echo
if [[ "${MODE}" == "join" ]]; then
  echo "In CLI, run:"
  echo "  load schemas/stream_join_demo.json"
  echo "  status"
else
  echo "In CLI, run:"
  echo "  load schemas/pollution2.json"
  echo "  status"
fi
