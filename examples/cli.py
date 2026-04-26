"""
Interactive CLI for StreamDataManagementSystem.

This CLI removes hard-coded query flow and allows users to deploy continuous
SELECT queries at runtime.
"""

import argparse
import json
import os
import re
import sqlite3
import shutil
import threading
import time
import webbrowser
import subprocess
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from core.execution.schema_registry import get_global_registry
from core.parser.sql_parser import parse_sql
from core.schema.schema_manager import SchemaManager
from streaming.kafka_client import StreamConsumer
from streaming.kafka_config import set_default_config, get_default_config
from core.storage.reference_tables import ReferenceTableStore


def _sqlite_type_to_sdms(sqlite_type: str) -> str:
    t = sqlite_type.upper()
    if "INT" in t:
        return "INT"
    if "REAL" in t or "FLOA" in t or "DOUB" in t:
        return "FLOAT"
    if "BOOL" in t:
        return "BOOLEAN"
    if "TIME" in t or "DATE" in t:
        return "TIMESTAMP"
    return "STRING"


def _infer_output_schema(
    query_plan: Dict[str, Any],
    input_streams: Dict[str, Dict[str, Any]],
    table_store: ReferenceTableStore,
) -> Dict[str, str]:
    """Infer output stream schema for SELECT, including JOIN scenarios."""
    left_stream = query_plan["from"]
    left_schema = dict(input_streams[left_stream]["schema"])
    merged_schema = dict(left_schema)

    join_cfg = query_plan.get("join")
    if isinstance(join_cfg, dict):
        join_source = join_cfg["table"]
        if join_source in input_streams:
            right_schema = dict(input_streams[join_source]["schema"])
            for key, value in right_schema.items():
                if key in merged_schema:
                    merged_schema[f"right_{key}"] = value
                else:
                    merged_schema[key] = value
        else:
            try:
                table_columns = table_store.table_schema(join_source)
            except Exception:
                table_columns = []

            for col in table_columns:
                col_name = col.get("name")
                if not col_name:
                    continue
                sdms_type = _sqlite_type_to_sdms(str(col.get("type", "TEXT")))
                if col_name in merged_schema:
                    merged_schema[f"table_{col_name}"] = sdms_type
                else:
                    merged_schema[col_name] = sdms_type

    select_expr = query_plan.get("select")
    if select_expr == "*":
        return merged_schema

    output_schema: Dict[str, str] = {}
    if not isinstance(select_expr, list):
        return output_schema

    for item in select_expr:
        if isinstance(item, str):
            output_schema[item] = merged_schema.get(item, "STRING")
        elif isinstance(item, dict) and "func" in item and "field" in item:
            alias = f"{item['func']}({item['field']})"
            output_schema[alias] = "INT" if item["func"] == "COUNT" else "FLOAT"

    return output_schema


def _normalize_command_token(token: str) -> str:
    cleaned = token.strip().lower().replace("-", "_")
    cleaned = re.sub(r"[^a-z0-9_]", "", cleaned)
    return cleaned


class StreamingCLI:
    def __init__(self, schema_path: Optional[str], broker: str):
        self.schema_path = schema_path
        self.registry = get_global_registry()
        self.schema_manager = SchemaManager()
        self.schema: Optional[Dict[str, Any]] = None
        self.schema_name: Optional[str] = None
        self.active_schema_path: Optional[str] = None
        self.engine = None

        set_default_config(broker=broker)
        self.config = get_default_config()

        self.consumer_threads: List[threading.Thread] = []
        self._consumer_keys: Set[Tuple[str, str]] = set()
        self.table_store = ReferenceTableStore(os.path.join("data", "static_tables.db"))
        
        # UI server process
        self.ui_server_process = None
        self.ui_server_port = 5000

        if schema_path:
            self.load_schema_from_file(schema_path)

    def load_schema_from_file(self, schema_path: str) -> None:
        """Load and activate a schema from file, or switch if already registered."""
        schema = self.schema_manager.load_from_file(schema_path)
        schema_name = schema["schema_name"]

        if schema_name in self.registry.schemas:
            engine = self.registry.replace_schema(schema)
            print(f"Schema '{schema_name}' reloaded and replaced with the latest definition.")
        else:
            engine = self.registry.register_schema(schema)
            print(f"Schema '{schema_name}' loaded and registered.")

        self.schema = schema
        self.schema_name = schema_name
        self.active_schema_path = schema_path
        self.engine = engine
        self.start_consumers()

    def create_empty_schema(self, schema_name: str) -> None:
        """Create and activate an empty schema that can be populated from CLI."""
        schema = {
            "schema_name": schema_name,
            "window_size": 10,
            "window_unit": "seconds",
            "velocity": {"type": "count", "value": 100},
            "input_streams": [],
            "continuous_queries": [],
            "output_streams": [],
        }

        engine = self.registry.register_schema(schema)
        self.schema = schema
        self.schema_name = schema_name
        self.active_schema_path = os.path.join("schemas", f"{schema_name}.json")
        self.engine = engine
        print(f"Empty schema '{schema_name}' created and activated.")
        self._persist_active_schema()

    def _persist_active_schema(self) -> None:
        """Persist active schema to disk."""
        if not self.schema or not self.active_schema_path:
            return

        try:
            self.schema_manager.schema = self.schema
            self.schema_manager.save_to_file(self.active_schema_path)
        except Exception as exc:
            print(f"Warning: failed to save schema to '{self.active_schema_path}': {exc}")

    def start_consumers(self) -> None:
        """Start one Kafka consumer thread for each input stream."""
        if not self.engine or not self.schema_name:
            return

        input_streams = self.engine.get_input_streams()

        for stream_name, stream_config in input_streams.items():
            consumer_key = (self.schema_name, stream_name)
            if consumer_key in self._consumer_keys:
                continue

            topic = stream_config["topic"]
            group_id = f"{self.schema_name}_{stream_name}_cli"

            thread = threading.Thread(
                target=self._run_consumer,
                args=(self.schema_name, stream_name, topic, group_id),
                daemon=True,
            )
            thread.start()
            self.consumer_threads.append(thread)
            self._consumer_keys.add(consumer_key)

        if self.consumer_threads:
            print(f"Started {len(self.consumer_threads)} consumer thread(s).")

    def _run_consumer(self, schema_name: str, stream_name: str, topic: str, group_id: str) -> None:
        try:
            consumer = StreamConsumer(topic=topic, group_id=group_id, config=self.config)
            for msg in consumer:
                self.registry.process_event(schema_name, stream_name, msg)
        except Exception as exc:
            print(f"Consumer error on schema '{schema_name}', stream '{stream_name}' (topic '{topic}'): {exc}")

    def print_status(self) -> None:
        print("\n=== Status ===")
        print(f"Kafka: {self.config.get_mode_description()}")
        print(f"Broker: {self.config.get_broker()}")
        all_schemas = self.registry.list_schemas()
        print(f"Registered schemas: {', '.join(all_schemas.keys()) if all_schemas else 'none'}")

        if not self.engine or not self.schema_name:
            print("Active schema: none")
            print("Use 'load' or 'create' to activate a schema.\n")
            return

        print(f"Active schema: {self.schema_name}")

        print("Input streams:")
        for name, cfg in self.engine.get_input_streams().items():
            print(f"  - {name} (topic: {cfg['topic']})")

        print("Output streams:")
        for name, cfg in self.engine.get_output_streams().items():
            print(f"  - {name} (topic: {cfg['topic']})")

        print("Queries:")
        queries = self.engine.get_queries()
        if not queries:
            print("  - none")
        else:
            for query in queries:
                print(
                    f"  - {query['name']}: "
                    f"{query['input_stream']} -> {query['output_stream']}"
                )
        print()

    def add_input_stream_interactive(self) -> None:
        """Add an input stream to the active schema and start its consumer."""
        if not self.engine:
            print("No active schema. Use 'load' or 'create' first.")
            return

        stream_name = input("input_stream_name> ").strip()
        if not stream_name:
            print("input_stream_name is required.")
            return

        topic = input(f"topic [{stream_name}]> ").strip() or stream_name

        print("Enter columns as name:TYPE separated by commas (example: sensor_id:STRING,value:FLOAT)")
        columns = input("columns> ").strip()
        schema_map: Dict[str, str] = {}
        if columns:
            pairs = [part.strip() for part in columns.split(",") if part.strip()]
            for pair in pairs:
                if ":" not in pair:
                    print(f"Invalid column format: {pair}")
                    return
                col_name, col_type = pair.split(":", 1)
                schema_map[col_name.strip()] = col_type.strip().upper()

        if not schema_map:
            print("At least one column is required.")
            return

        stream_cfg = {
            "name": stream_name,
            "topic": topic,
            "schema": schema_map,
        }

        try:
            self.engine._register_input_stream(stream_cfg)
            if self.schema is not None:
                input_streams = self.schema.setdefault("input_streams", [])
                input_streams.append(stream_cfg)
                self._persist_active_schema()
            self.start_consumers()
            print(f"Input stream '{stream_name}' added.")
        except Exception as exc:
            print(f"Failed to add input stream '{stream_name}': {exc}")

    def deploy_query_interactive(self) -> None:
        """Prompt the user for query details and deploy it."""
        if not self.engine:
            print("No active schema. Use 'load' or 'create' first.")
            return

        print("\nEnter SELECT query (example: SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50)")
        query_text = input("query> ").strip().rstrip(";")

        if not query_text:
            print("Empty query. Nothing deployed.")
            return

        try:
            parsed = parse_sql(query_text)
        except Exception as exc:
            print(f"Invalid query syntax: {exc}")
            return

        if not parsed or parsed[0].get("type") != "select_query":
            print("Only one SELECT query is supported per command.")
            return

        query_plan = parsed[0]
        input_stream = query_plan["from"]

        if input_stream not in self.engine.get_input_streams():
            print(f"Input stream '{input_stream}' is not registered in current schema.")
            return

        query_name = input("query_name> ").strip()
        if not query_name:
            print("query_name is required.")
            return

        output_stream = input("output_stream> ").strip()
        if not output_stream:
            print("output_stream is required.")
            return

        # Create output stream if it does not already exist.
        if output_stream not in self.engine.get_output_streams():
            topic_default = output_stream
            output_topic = input(f"output_topic [{topic_default}]> ").strip() or topic_default

            inferred_schema = _infer_output_schema(
                query_plan,
                self.engine.get_input_streams(),
                self.table_store,
            )
            if not inferred_schema:
                inferred_schema = {"value": "STRING"}

            new_output_cfg = {
                "name": output_stream,
                "topic": output_topic,
                "schema": inferred_schema,
            }
            self.engine._register_output_stream(new_output_cfg)
            if self.schema is not None:
                output_streams = self.schema.setdefault("output_streams", [])
                output_streams.append(new_output_cfg)
                self._persist_active_schema()
            print(f"Output stream '{output_stream}' created.")

        query_cfg = {
            "name": query_name,
            "input_stream": input_stream,
            "output_stream": output_stream,
            "query": query_text,
        }

        existing_names = {q["name"] for q in self.engine.get_queries()}
        if query_name in existing_names:
            print(f"Query name '{query_name}' already exists.")
            return

        try:
            self.engine._deploy_continuous_query(query_cfg)
            if self.schema is not None:
                continuous_queries = self.schema.setdefault("continuous_queries", [])
                continuous_queries.append(query_cfg)
                self._persist_active_schema()
            print(f"Query '{query_name}' deployed successfully.")
        except Exception as exc:
            print(f"Failed to deploy query '{query_name}': {exc}")

    def table_create_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        if not table_name:
            print("table_name is required.")
            return

        print("Enter columns as name:TYPE separated by commas (example: id:INT,name:STRING)")
        raw_cols = input("columns> ").strip()
        if not raw_cols:
            print("columns are required.")
            return

        columns: List[Tuple[str, str]] = []
        for pair in [part.strip() for part in raw_cols.split(",") if part.strip()]:
            if ":" not in pair:
                print(f"Invalid column format: {pair}")
                return
            name, data_type = pair.split(":", 1)
            columns.append((name.strip(), data_type.strip().upper()))

        try:
            self.table_store.create_table(table_name, columns)
            print(f"Table '{table_name}' created in {self.table_store.db_path}.")
        except Exception as exc:
            print(f"Failed to create table: {exc}")

    def table_add_column_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        col_name = input("column_name> ").strip()
        data_type = input("type (STRING/INT/FLOAT/BOOLEAN/TIMESTAMP)> ").strip().upper()
        if not table_name or not col_name or not data_type:
            print("table_name, column_name, and type are required.")
            return

        try:
            self.table_store.add_column(table_name, col_name, data_type)
            print(f"Column '{col_name}' added to '{table_name}'.")
        except Exception as exc:
            print(f"Failed to add column: {exc}")

    def table_insert_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        if not table_name:
            print("table_name is required.")
            return

        print("Enter row JSON (example: {\"id\": 1, \"name\": \"alice\"})")
        raw = input("row_json> ").strip()
        try:
            row_data = json.loads(raw)
            if not isinstance(row_data, dict):
                raise ValueError("row_json must be an object")
            self.table_store.insert_row(table_name, row_data)
            print(f"1 row inserted into '{table_name}'.")
        except Exception as exc:
            print(f"Failed to insert row: {exc}")

    def table_update_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        if not table_name:
            print("table_name is required.")
            return

        print("Enter set JSON (example: {\"name\": \"bob\"})")
        raw_set = input("set_json> ").strip()
        print("Enter where JSON (example: {\"id\": 1})")
        raw_where = input("where_json> ").strip()

        try:
            set_data = json.loads(raw_set)
            where_data = json.loads(raw_where)
            if not isinstance(set_data, dict) or not isinstance(where_data, dict):
                raise ValueError("set_json and where_json must be JSON objects")
            changed = self.table_store.update_rows(table_name, set_data, where_data)
            print(f"{changed} row(s) updated.")
        except Exception as exc:
            print(f"Failed to update rows: {exc}")

    def table_delete_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        if not table_name:
            print("table_name is required.")
            return

        print("Enter where JSON (example: {\"id\": 1})")
        raw_where = input("where_json> ").strip()
        try:
            where_data = json.loads(raw_where)
            if not isinstance(where_data, dict):
                raise ValueError("where_json must be a JSON object")
            deleted = self.table_store.delete_rows(table_name, where_data)
            print(f"{deleted} row(s) deleted.")
        except Exception as exc:
            print(f"Failed to delete rows: {exc}")

    def table_list(self) -> None:
        try:
            tables = self.table_store.list_tables()
            print(f"Tables: {', '.join(tables) if tables else 'none'}")
        except Exception as exc:
            print(f"Failed to list tables: {exc}")

    def table_schema_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        if not table_name:
            print("table_name is required.")
            return
        try:
            cols = self.table_store.table_schema(table_name)
            if not cols:
                print("No columns found (table may not exist).")
                return
            for col in cols:
                print(f"- {col['name']} ({col['type']})")
        except Exception as exc:
            print(f"Failed to read table schema: {exc}")

    def table_select_interactive(self) -> None:
        table_name = input("table_name> ").strip()
        if not table_name:
            print("table_name is required.")
            return

        raw_limit = input("limit [20]> ").strip() or "20"
        try:
            limit = int(raw_limit)
            rows = self.table_store.select_rows(table_name, limit)
            if not rows:
                print("No rows found.")
                return
            for row in rows:
                print(json.dumps(row))
        except Exception as exc:
            print(f"Failed to select rows: {exc}")

    def launch_ui(self, port: int = 5000) -> None:
        """
        Launch the UI dashboard in a background process.
        
        Args:
            port: Port to run the Flask server on
        """
        if self.ui_server_process and self.ui_server_process.poll() is None:
            print(f"UI server already running on port {self.ui_server_port}")
            print(f"Open your browser to: http://localhost:{self.ui_server_port}")
            return
        
        try:
            self.ui_server_port = port
            print(f"Starting UI server on port {port}...")

            active_schema_abs = ""
            if self.active_schema_path:
                active_schema_abs = os.path.abspath(self.active_schema_path)
            
            # Create a Python script to run the Flask app
            ui_script = f"""
import os
import sys
os.environ['SDMS_UI_PORT'] = '{port}'
os.environ['SDMS_KAFKA_BROKER'] = '{self.config.get_broker()}'
os.environ['SDMS_ACTIVE_SCHEMA_PATH'] = {active_schema_abs!r}

from ui.app import UIApp

app = UIApp()
app.initialize_kafka_consumer()
app.start(port={port})
"""
            
            # Run the UI server in a subprocess
            self.ui_server_process = subprocess.Popen(
                [sys.executable, "-c", ui_script],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            
            # Give the server time to start
            time.sleep(2)
            
            # Check if process is still running
            if self.ui_server_process.poll() is not None:
                print(f"UI server failed to start:")
                print(f"Process exited with code: {self.ui_server_process.returncode}")
                self.ui_server_process = None
                return
            
            print(f"✓ UI server started on http://localhost:{port}")
            
            # Open browser unless disabled by env var.
            auto_open = os.getenv("SDMS_UI_AUTO_OPEN_BROWSER", "true").lower() == "true"
            if auto_open:
                ui_url = f"http://localhost:{port}"
                opened = False

                # Prefer xdg-open on Linux and silence browser stderr noise.
                if sys.platform.startswith("linux"):
                    xdg_open = shutil.which("xdg-open")
                    if xdg_open:
                        try:
                            subprocess.Popen(
                                [xdg_open, ui_url],
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True,
                            )
                            opened = True
                        except Exception:
                            opened = False

                if not opened:
                    try:
                        opened = bool(webbrowser.open(ui_url, new=2))
                    except Exception:
                        opened = False

                if opened:
                    print(f"✓ Browser opened (if not, visit {ui_url})")
                else:
                    print(f"Could not open browser automatically. Visit {ui_url} manually")
            else:
                print(f"Auto-open disabled. Visit http://localhost:{port} manually")
            
        except Exception as exc:
            print(f"Failed to launch UI server: {exc}")
    
    def stop_ui(self) -> None:
        """Stop the UI server if running."""
        if self.ui_server_process and self.ui_server_process.poll() is None:
            try:
                self.ui_server_process.terminate()
                self.ui_server_process.wait(timeout=5)
                print("✓ UI server stopped")
            except subprocess.TimeoutExpired:
                self.ui_server_process.kill()
                print("✓ UI server killed")
            except Exception as e:
                print(f"Error stopping UI server: {e}")
            finally:
                self.ui_server_process = None
        
    def ui_status(self) -> None:
        """Check UI server status."""
        if self.ui_server_process is None:
            print("UI server: not started")
        elif self.ui_server_process.poll() is None:
            print(f"UI server: running on port {self.ui_server_port}")
            print(f"URL: http://localhost:{self.ui_server_port}")
        else:
            print(f"UI server: stopped (exit code: {self.ui_server_process.returncode})")

    def run(self) -> None:
        print("\nStreamDataManagementSystem Interactive CLI")
        print("Type 'help' for commands.")

        if not self.engine:
            print("No schema loaded at startup. Use 'load' or 'create' to begin.")
        else:
            self.start_consumers()

        while True:
            try:
                raw_cmd = input("sdms> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nExiting CLI.")
                self.stop_ui()
                break

            cmd_parts = raw_cmd.split(maxsplit=1)
            cmd = _normalize_command_token(cmd_parts[0]) if cmd_parts else ""
            arg = cmd_parts[1] if len(cmd_parts) > 1 else ""

            if cmd == "table" and arg:
                sub = _normalize_command_token(arg.split(maxsplit=1)[0])
                if sub:
                    cmd = f"table_{sub}"

            aliases = {
                "quit": "exit",
                "tablecreate": "table_create",
                "tableaddcolumn": "table_add_column",
                "tableinsert": "table_insert",
                "tableupdate": "table_update",
                "tabledelete": "table_delete",
                "tablelist": "table_list",
                "tableschema": "table_schema",
                "tableselect": "table_select",
            }
            cmd = aliases.get(cmd, cmd)

            if cmd in {"exit", "quit"}:
                print("Exiting CLI.")
                self.stop_ui()
                break
            if cmd == "help":
                print("\nCommands:")
                print("  help   - show commands")
                print("  load [path] - load and activate schema from file")
                print("  create - create an empty schema")
                print("  schemas - list registered schemas")
                print("  status - show schema/stream/query status")
                print("  add_input - add an input stream to active schema")
                print("  query  - deploy a new continuous SELECT query")
                print("  save   - save active schema to disk now")
                print("  ui [--port 5000] - launch web UI dashboard")
                print("  table_create - create a persistent table")
                print("  table_add_column - add a column to a table")
                print("  table_insert - insert one row using JSON")
                print("  table_update - update rows using set/where JSON")
                print("  table_delete - delete rows using where JSON")
                print("  table_list - list persistent tables")
                print("  table_schema - show table columns")
                print("  table_select - read table rows")
                print("  wait   - keep process alive without prompt")
                print("  exit   - exit the CLI\n")
                continue
            if cmd == "load":
                schema_path = arg.strip() or input("schema_path> ").strip()
                if not schema_path:
                    print("schema_path is required.")
                    continue
                try:
                    self.load_schema_from_file(schema_path)
                except Exception as exc:
                    print(f"Failed to load schema: {exc}")
                continue
            if cmd == "create":
                schema_name = input("schema_name> ").strip()
                if not schema_name:
                    print("schema_name is required.")
                    continue
                try:
                    self.create_empty_schema(schema_name)
                except Exception as exc:
                    print(f"Failed to create schema: {exc}")
                continue
            if cmd == "schemas":
                names = self.registry.list_schemas().keys()
                print(f"Registered schemas: {', '.join(names) if names else 'none'}")
                continue
            if cmd == "status":
                self.print_status()
                continue
            if cmd == "add_input":
                self.add_input_stream_interactive()
                continue
            if cmd == "query":
                self.deploy_query_interactive()
                continue
            if cmd == "save":
                if not self.schema or not self.active_schema_path:
                    print("No active schema to save.")
                    continue
                self._persist_active_schema()
                print(f"Schema saved to {self.active_schema_path}")
                continue
            if cmd == "ui":
                port = 5000
                if arg:
                    try:
                        port = int(arg)
                    except ValueError:
                        print("Invalid port. Using default port 5000.")
                        port = 5000
                
                if not self.engine:
                    print("No active schema. Load or create a schema first.")
                    continue
                
                self.launch_ui(port)
                continue

            if cmd == "table_create":
                self.table_create_interactive()
                continue
            if cmd == "table_add_column":
                self.table_add_column_interactive()
                continue
            if cmd == "table_insert":
                self.table_insert_interactive()
                continue
            if cmd == "table_update":
                self.table_update_interactive()
                continue
            if cmd == "table_delete":
                self.table_delete_interactive()
                continue
            if cmd == "table_list":
                self.table_list()
                continue
            if cmd == "table_schema":
                self.table_schema_interactive()
                continue
            if cmd == "table_select":
                self.table_select_interactive()
                continue
            if cmd == "wait":
                print("Press Ctrl+C to return to prompt.")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("Back to prompt.")
                continue
            if cmd == "":
                continue

            print("Unknown command. Type 'help'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Interactive CLI for StreamDataManagementSystem")
    parser.add_argument(
        "--schema",
        default=None,
        help="Path to schema JSON file",
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka broker address",
    )

    args = parser.parse_args()

    try:
        cli = StreamingCLI(
            schema_path=args.schema,
            broker=args.broker,
        )
    except Exception as exc:
        print(f"Startup failed: {exc}")
        return

    cli.run()


if __name__ == "__main__":
    main()
