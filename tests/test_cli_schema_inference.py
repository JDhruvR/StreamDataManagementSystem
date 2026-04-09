from core.storage.reference_tables import ReferenceTableStore
from examples.cli import _infer_output_schema


def test_infer_output_schema_stream_stream_join_collision(tmp_path):
    query_plan = {
        "type": "select_query",
        "select": ["sensor_id", "value", "humidity", "right_sensor_id"],
        "from": "pollution_stream",
        "join": {
            "join_type": "INNER",
            "table": "weather_stream",
            "left_field": "sensor_id",
            "operator": "=",
            "right_field": "sensor_id",
        },
    }
    input_streams = {
        "pollution_stream": {
            "schema": {"sensor_id": "STRING", "value": "FLOAT"}
        },
        "weather_stream": {
            "schema": {"sensor_id": "STRING", "humidity": "FLOAT"}
        },
    }

    store = ReferenceTableStore(str(tmp_path / "test_schema_inference.db"))
    inferred = _infer_output_schema(query_plan, input_streams, store)

    assert inferred == {
        "sensor_id": "STRING",
        "value": "FLOAT",
        "humidity": "FLOAT",
        "right_sensor_id": "STRING",
    }


def test_infer_output_schema_stream_table_join_uses_table_prefix(tmp_path):
    store = ReferenceTableStore(str(tmp_path / "test_schema_inference_table.db"))
    store.create_table("sensors", [("id", "STRING"), ("sensor_id", "STRING"), ("name", "STRING")])

    query_plan = {
        "type": "select_query",
        "select": ["sensor_id", "table_sensor_id", "name", {"func": "AVG", "field": "value"}],
        "from": "pollution_stream",
        "join": {
            "join_type": "INNER",
            "table": "sensors",
            "left_field": "sensor_id",
            "operator": "=",
            "right_field": "id",
        },
    }
    input_streams = {
        "pollution_stream": {
            "schema": {"sensor_id": "STRING", "value": "FLOAT"}
        }
    }

    inferred = _infer_output_schema(query_plan, input_streams, store)

    assert inferred == {
        "sensor_id": "STRING",
        "table_sensor_id": "STRING",
        "name": "STRING",
        "AVG(value)": "FLOAT",
    }
