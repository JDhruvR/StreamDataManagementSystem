#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
export PYTHONPATH="$(pwd)"
export SDMS_ACTIVE_SCHEMA_PATH="$(pwd)/schemas/pollution2.json"
python ui/app.py