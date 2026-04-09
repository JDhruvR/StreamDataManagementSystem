from core.execution.engine import ExecutionEngine
from core.execution.operators import SinkOperator


def _build_schema():
    return {
        "schema_name": "join_schema",
        "window_size": 10,
        "window_unit": "seconds",
        "velocity": {"type": "count", "value": 1},
        "input_streams": [
            {
                "name": "pollution_stream",
                "topic": "pollution_stream",
                "schema": {"sensor_id": "STRING", "value": "FLOAT"},
            },
            {
                "name": "weather_stream",
                "topic": "weather_stream",
                "schema": {"sensor_id": "STRING", "humidity": "FLOAT"},
            },
        ],
        "continuous_queries": [
            {
                "name": "joined",
                "input_stream": "pollution_stream",
                "output_stream": "joined_out",
                "query": "SELECT sensor_id, value, humidity FROM pollution_stream INNER JOIN weather_stream ON sensor_id = sensor_id",
            }
        ],
        "output_streams": [
            {
                "name": "joined_out",
                "topic": "joined_out",
                "schema": {"sensor_id": "STRING", "value": "FLOAT", "humidity": "FLOAT"},
            }
        ],
    }


def test_engine_routes_both_streams_for_stream_stream_join():
    engine = ExecutionEngine(_build_schema())
    captured = []

    class CaptureSink(SinkOperator):
        def process(self, event):
            captured.append(event)

    query = engine.get_queries()[0]
    join_op = query["pipeline"]
    projection = join_op.next_op
    projection.next_op = CaptureSink()

    engine.process_event("pollution_stream", {"sensor_id": "s1", "value": 70.0})
    engine.process_event("weather_stream", {"sensor_id": "s1", "humidity": 40.0})

    assert len(captured) == 1
    assert captured[0] == {"sensor_id": "s1", "value": 70.0, "humidity": 40.0}
