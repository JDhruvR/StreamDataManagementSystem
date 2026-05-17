# StreamDataManagementSystem

A schema-driven stream processing system for real-time event ingestion, query execution, joins, windowed aggregations, and dashboard visualization.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10-blue?style=for-the-badge&logo=python" alt="Python" />
  <img src="https://img.shields.io/badge/Apache%20Kafka-Streaming-orange?style=for-the-badge&logo=apachekafka" alt="Kafka" />
  <img src="https://img.shields.io/badge/SQLite-DB-lightgrey?style=for-the-badge&logo=sqlite" alt="SQLite" />
  <img src="https://img.shields.io/badge/Flask-Web-green?style=for-the-badge&logo=flask" alt="Flask" />
  <img src="https://img.shields.io/badge/Lark-Parser-yellow?style=for-the-badge&logo=lark" alt="Lark" />
  <img src="https://img.shields.io/badge/pandas-Data-magenta?style=for-the-badge&logo=pandas" alt="pandas" />
  <img src="https://img.shields.io/badge/Plotly-Visualization-red?style=for-the-badge&logo=plotly" alt="Plotly" />
</p>

## Table of Contents
- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Schema Format](#schema-format)
- [SQL Support](#sql-support)
- [How Data Flows](#how-data-flows)
- [Running Sensors](#running-sensors)
- [Web UI and API](#web-ui-and-api)
- [Persistent Reference Tables](#persistent-reference-tables)
- [Development Notes](#development-notes)
- [Troubleshooting](#troubleshooting)
- [Roadmap Ideas](#roadmap-ideas)

## Overview
StreamDataManagementSystem (SDMS) is designed to run continuous, schema-defined streaming queries over Kafka topics.

You define a schema JSON that includes:
- Input stream definitions
- Output stream definitions
- Window configuration
- Velocity configuration
- Continuous SQL-like queries

At runtime, SDMS deploys query pipelines and continuously processes incoming events from Kafka consumers.

## Key Features
- Schema-first streaming execution model
- SQL-like parser built with Lark
- Supported operations:
  - Projection
  - Filter (`WHERE`)
  - Grouped aggregations (`COUNT`, `SUM`, `AVG`, `MAX`, `MIN`)
  - `INNER JOIN` (stream-stream and stream-table)
- Tumbling/sliding window support (schema-level)
- Velocity trigger by count or time
- Built-in sensor simulators for demo workloads
- Flask dashboard with REST APIs for live buffers and historical SQLite data
- Dynamic runtime schema/query updates via interactive CLI

## Architecture

[![](https://img.plantuml.biz/plantuml/svg/VLR9Zjj64BtpAmew9fZXtOV0inbGOcVaaUScG6YIHQgjPfFJYyPAa7zFzSP5edrIBwzUr_geZzemPMmhCe6r0TE1uit1GCKLbePtCjD7BdkcM0jbr_QTH6csvYmG7839Hk04eW-iwjwvRA1cGazlUbOUMOENfXahJQTWzQfHhO1fiBJ8ieY6rKPrbIrHwHNycy7e0gosA3LPRdbh1ICElPFQ7-dyDyyKTnTMHY5vasZ9nX-Gn_cHrKUsBmKdu6r_zXnzn159f8ERcp-6O_W03KfKp23WYUnqzj_4-MtYXhMi5QEdR6cimX2Bx2h2t17wWF2NUtmd2oihWHLuNtBlWb_EW_1WUF2Umk_mqCcQDxy8PU7AcwGe_LcWc_bFn4Pr0ZQ2IOGSlbfKPtYnql0MhmgobeOoGKrpGdZulDv9_C7QNg3EoP7q52ql2aBHjmo-C4bFKZtAJc6k_M7ysRVXBb64qo-DMYHwmOPJjEV8X3-mjAwF4vcAzuajxI_8ldw63LDwyAZtcro_YNrO-hHEhnhgyfQfMy7KCVIEFv_pFYLtu4asNEALb-YFatDXDwTuxbs_KUTlU8zYYQEB23rqSKJEYLwmHeMoHFZ62XhABPaaCarhodck4cPl74QFMQizd1WhHbTCeqyQDux7ZSFZ_KwwAw9YXfV9lYh22n1V21Lz4sJexMRzzxgcfdikA8idLd32dA-QxfDW-WW4lElxdRGyPtsVdBHyQMoUhUcjSKEZRUlgIqR3KFYZqJw1vZnX32BFlQthoEB2tHT-doZ2Rcx-c1AsfRXuYS6ssEkmJQPL4UylhVMXw9YgQF1VdhRVu6wpzWm6QTWeWzhlGjO3KRAOvVPEQpIQyhkM5CQ2l8PnUiIQI-xQokchFoDzlVskEpaPna-SQdlXvpsy6b-ps0SdZk_xgXZ3SzVE9QVuNbMTDWmRErbF7Y759V4zSPBES9fbFn5cRBcXFcKLgPxEXWk7ckhTP3tUaVcxShtCNNBegzZpKcSPEJZnXnI8SA9ZLNvYWbUkUyURQx-TB_48_jm-_ptG9Gdo6BSaDL58nIo780U8mmTL87H9bEGaZVjDv5L1cxC8yIdnCkJovXPK8a98sraDDAypO5G9WXQMYseuc4G-pTO7Y0CppzBF4kh3do4NugSobQXrnCLFwdBmghFZLtVm_Rg4NPZJ-EOCc5ICiE43UUDL2hvtNBhHeCHgLmwA9CXNDbZuyewovLmFO1WaCzbazYfCek_fBn34hsKiSiIDikEHD7pKNVyCWMXRw5BntizaDW5VTlRIrQHeyxnSW8Dk-RouQ8NpCgOwUZd84u5gDEK13jo1pvIt5WrpCv-DeEjylNaL3KQyPC9_I0RsGULSuyYAVAacmaUQbsL1GHQOKtPJ_ZyI0Vt3_Hy0)](https://editor.plantuml.com/uml/VLR9Zjj64BtpAmew9fZXtOV0inbGOcVaaUScG6YIHQgjPfFJYyPAa7zFzSP5edrIBwzUr_geZzemPMmhCe6r0TE1uit1GCKLbePtCjD7BdkcM0jbr_QTH6csvYmG7839Hk04eW-iwjwvRA1cGazlUbOUMOENfXahJQTWzQfHhO1fiBJ8ieY6rKPrbIrHwHNycy7e0gosA3LPRdbh1ICElPFQ7-dyDyyKTnTMHY5vasZ9nX-Gn_cHrKUsBmKdu6r_zXnzn159f8ERcp-6O_W03KfKp23WYUnqzj_4-MtYXhMi5QEdR6cimX2Bx2h2t17wWF2NUtmd2oihWHLuNtBlWb_EW_1WUF2Umk_mqCcQDxy8PU7AcwGe_LcWc_bFn4Pr0ZQ2IOGSlbfKPtYnql0MhmgobeOoGKrpGdZulDv9_C7QNg3EoP7q52ql2aBHjmo-C4bFKZtAJc6k_M7ysRVXBb64qo-DMYHwmOPJjEV8X3-mjAwF4vcAzuajxI_8ldw63LDwyAZtcro_YNrO-hHEhnhgyfQfMy7KCVIEFv_pFYLtu4asNEALb-YFatDXDwTuxbs_KUTlU8zYYQEB23rqSKJEYLwmHeMoHFZ62XhABPaaCarhodck4cPl74QFMQizd1WhHbTCeqyQDux7ZSFZ_KwwAw9YXfV9lYh22n1V21Lz4sJexMRzzxgcfdikA8idLd32dA-QxfDW-WW4lElxdRGyPtsVdBHyQMoUhUcjSKEZRUlgIqR3KFYZqJw1vZnX32BFlQthoEB2tHT-doZ2Rcx-c1AsfRXuYS6ssEkmJQPL4UylhVMXw9YgQF1VdhRVu6wpzWm6QTWeWzhlGjO3KRAOvVPEQpIQyhkM5CQ2l8PnUiIQI-xQokchFoDzlVskEpaPna-SQdlXvpsy6b-ps0SdZk_xgXZ3SzVE9QVuNbMTDWmRErbF7Y759V4zSPBES9fbFn5cRBcXFcKLgPxEXWk7ckhTP3tUaVcxShtCNNBegzZpKcSPEJZnXnI8SA9ZLNvYWbUkUyURQx-TB_48_jm-_ptG9Gdo6BSaDL58nIo780U8mmTL87H9bEGaZVjDv5L1cxC8yIdnCkJovXPK8a98sraDDAypO5G9WXQMYseuc4G-pTO7Y0CppzBF4kh3do4NugSobQXrnCLFwdBmghFZLtVm_Rg4NPZJ-EOCc5ICiE43UUDL2hvtNBhHeCHgLmwA9CXNDbZuyewovLmFO1WaCzbazYfCek_fBn34hsKiSiIDikEHD7pKNVyCWMXRw5BntizaDW5VTlRIrQHeyxnSW8Dk-RouQ8NpCgOwUZd84u5gDEK13jo1pvIt5WrpCv-DeEjylNaL3KQyPC9_I0RsGULSuyYAVAacmaUQbsL1GHQOKtPJ_ZyI0Vt3_Hy0)

### Core Components
- `core/parser`: SQL grammar and parse-tree transformation
- `core/schema`: schema loading and validation
- `core/execution`: schema registry, execution engine, operator pipeline
- `core/storage`: reference table persistence support
- `streaming`: Kafka client abstractions and config
- `ui`: Flask app, API endpoints, live buffers, dashboard layer
- `sensors`: synthetic event producers for demos
- `examples/cli.py`: interactive operational control plane

## Project Structure

```text
.
├── core
│   ├── execution
│   │   ├── engine.py                - Execution engine that builds and runs query pipelines
│   │   ├── operators.py             - Operator implementations (filter/window/agg/join/sink)
│   │   └── schema_registry.py       - Manage registered schemas and engines
│   ├── parser
│   │   ├── grammar.lark             - Lark grammar for SQL-like language
│   │   └── sql_parser.py            - Parser and transformer producing query plans
│   ├── schema
│   │   └── schema_manager.py        - Load, validate, and persist schemas
│   └── storage
│       ├── reference_tables.py      - SQLite-backed reference/dimension table store
│       └── table.py                 - Simple SQLite table manager utilities
├── data
│   ├── aggregate_states.db          - SQLite DB for operator aggregation state
│   ├── static_tables.db             - SQLite DB for persistent/static reference tables
│   └── view_db.py                   - DB inspection helper script
├── examples
│   └── cli.py                       - Interactive CLI entrypoint and command loop
├── README.md                        - Project documentation (this file)
├── requirements.txt                 - Python dependencies
├── schemas
│   ├── pollution2.json              - Sample pollution schema (tumbling window)
│   ├── pollution_schema.json        - Baseline pollution monitoring schema
│   ├── smart_city.json              - Smart-city multi-stream demo schema
│   └── stream_join_demo.json        - Stream-stream join demonstration schema
├── sensors
│   ├── pollution_sensor.py          - Synthetic pollution event producer
│   ├── signal_sensor.py             - Synthetic traffic signal event producer
│   ├── vehicle_sensor.py            - Synthetic vehicle telemetry producer
│   └── weather_sensor.py            - Synthetic weather/humidity producer
├── streaming
│   ├── kafka_client.py              - Kafka producer/consumer wrappers
│   └── kafka_config.py              - Kafka broker and topic configuration helpers
└── ui
  ├── app.py                         - Flask app and API route registrations
  ├── config.py                      - UI configuration and environment mapping
  ├── data_buffer.py                 - In-memory buffers for live query outputs
  ├── db_service.py                  - SQLite access for historical data
  ├── kafka_consumer.py              - Background consumers for UI output topics
  ├── static
  │   ├── css
  │   │   └── style.css              - Dashboard stylesheet
  │   └── js
  │       ├── charts.js              - Chart rendering utilities
  │       ├── dashboard.js           - Dashboard page scripting and refresh
  │       └── data-mapper.js         - Frontend data mapping helpers
  └── templates
    └── dashboard.html               - Dashboard HTML template
```

## Prerequisites
- Linux/macOS/WSL environment
- Python 3.10+ recommended
- Java runtime (required by Kafka)
- Free ports:
  - `9092` for Kafka broker
  - `5000` for UI

## Quick Start

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Start Kafka (using bundled distribution)
From project root:

```bash
# Terminal 1: start controller
./bin/kafka-server-start.sh kafka/config/controller.properties

# Terminal 2: start broker
./bin/kafka-server-start.sh kafka/config/broker.properties
```

If you already run Kafka elsewhere, ensure broker is reachable at `localhost:9092` or pass your broker to CLI.

### 3) Start SDMS CLI with a sample schema

```bash
python -m examples.cli --schema schemas/smart_city.json --broker localhost:9092
```

### 4) Start one or more sensor simulators

```bash
python -m sensors.vehicle_sensor
python -m sensors.signal_sensor
```

### 5) Launch dashboard from CLI
Inside the SDMS prompt:

```text
sdms> ui 5000
```

Then open: http://localhost:5000

## Schema Format
A schema file defines processing behavior and data contracts.

Required top-level fields:
- `schema_name`
- `window_size`
- `window_unit` (`seconds|minutes|hours`)
- `velocity` (`{"type": "count|time", "value": <positive number>}`)
- `window_type` (`tumbling|sliding`)
- `input_streams`
- `continuous_queries`
- `output_streams`

Minimal example:

```json
{
  "schema_name": "pollution2",
  "window_size": 10,
  "window_unit": "seconds",
  "velocity": { "type": "time", "value": 10 },
  "window_type": "tumbling",
  "input_streams": [
    {
      "name": "pollution_stream",
      "topic": "pollution_stream",
      "schema": {
        "timestamp": "STRING",
        "sensor_id": "STRING",
        "pollutant": "STRING",
        "value": "FLOAT"
      }
    }
  ],
  "continuous_queries": [
    {
      "name": "avg_pollution",
      "input_stream": "pollution_stream",
      "output_stream": "pollution_out",
      "query": "SELECT pollutant, MAX(value) FROM pollution_stream GROUP BY pollutant"
    }
  ],
  "output_streams": [
    {
      "name": "pollution_out",
      "topic": "pollution_out",
      "schema": {
        "pollutant": "STRING",
        "MAX(value)": "FLOAT"
      }
    }
  ]
}
```

See real examples in `schemas/`.

## SQL Support
Supported statement types in parser grammar:
- `CREATE TABLE`
- `CREATE STREAM`
- `SELECT ... FROM ...`

Supported query clauses:
- `WHERE`
- `GROUP BY`
- `INNER JOIN ... ON ...`

Supported aggregate functions:
- `COUNT`, `SUM`, `AVG`, `MAX`, `MIN`

Example queries:

```sql
SELECT sensor_id, AVG(value)
FROM pollution_stream
WHERE value > 50
GROUP BY sensor_id
```

```sql
SELECT vehicle_id, junction_id, signal_phase
FROM vehicle_stream
INNER JOIN signal_stream ON junction_id = junction_id
WHERE signal_phase = "RED"
```

## How Data Flows
1. Sensor scripts publish events to Kafka input topics.
2. CLI-managed consumers pull events and send them to schema registry.
3. Execution engine routes each event through deployed operator pipelines.
4. Results are emitted to output Kafka topics by sink operators.
5. UI background consumers subscribe to output topics.
6. UI serves recent live data from in-memory buffers and historical/table data from SQLite.

## Running Sensors
Available producers:
- `python -m sensors.pollution_sensor`
- `python -m sensors.weather_sensor`
- `python -m sensors.vehicle_sensor`
- `python -m sensors.signal_sensor`

You can run multiple producers in parallel to exercise joins and multi-stream scenarios.

## Web UI and API
UI app entrypoint:

```bash
python -m ui.app
```

Main endpoints:
- `GET /` dashboard
- `GET /api/health` health check
- `GET /api/status` runtime status
- `GET /api/schema` active schema metadata
- `GET /api/queries` deployed query list
- `GET /api/query/<query_name>/live?limit=50` buffered live output
- `GET /api/query/<query_name>/history?table=<name>&limit=100` SQLite data
- `GET /api/database/tables` list DB tables

Environment variables used by UI:
- `SDMS_UI_HOST` (default `127.0.0.1`)
- `SDMS_UI_PORT` (default `5000`)
- `SDMS_KAFKA_BROKER` (default `localhost:9092`)
- `SDMS_SQLITE_DB` (default `data/static_tables.db`)
- `SDMS_LOG_LEVEL` (default `INFO`)

## Persistent Reference Tables
The CLI includes table management commands backed by SQLite.

Inside CLI:
- `table_create`
- `table_add_column`
- `table_insert`
- `table_update`
- `table_delete`
- `table_list`
- `table_schema`
- `table_select`

Default DB location:
- `data/static_tables.db`

Use this for stream-table join enrichment use cases.

## Troubleshooting
- Kafka connection errors:
  - Verify broker is up on `localhost:9092`
  - Confirm Java/Kafka processes are running
- Empty UI dashboards:
  - Ensure a schema is active in CLI
  - Ensure sensor producers are sending events
  - Ensure output topics exist and are subscribed by UI
- Schema load failures:
  - Validate required schema keys and stream/query references
  - Confirm `window_type` is present for current engine expectations
- Port conflicts:
  - Change UI port in command (`ui 5050`) or env vars

## Roadmap Ideas
- Add automated integration and end-to-end test suite
- Add Docker Compose setup for one-command startup
- Add schema migration/versioning tooling
- Add query lifecycle operations (pause/resume/remove)
- Improve multi-schema active selection in UI
- Add metrics and tracing (Prometheus/OpenTelemetry)

---