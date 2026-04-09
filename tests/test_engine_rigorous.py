from core.execution.engine import ExecutionEngine
from core.execution.operators import SinkOperator


class CaptureSink(SinkOperator):
    def __init__(self, captured):
        super().__init__()
        self._captured = captured

    def process(self, event):
        self._captured.append(event)


def _schema_for_rigorous_streams():
    return {
        "schema_name": "rigorous_engine_schema",
        "window_size": 10,
        "window_unit": "seconds",
        "velocity": {"type": "count", "value": 1},
        "input_streams": [
            {
                "name": "pollution_stream",
                "topic": "pollution_stream",
                "schema": {"sensor_id": "STRING", "value": "FLOAT", "pollutant": "STRING"},
            },
            {
                "name": "weather_stream",
                "topic": "weather_stream",
                "schema": {"sensor_id": "STRING", "humidity": "FLOAT", "zone": "STRING"},
            },
            {
                "name": "noise_stream",
                "topic": "noise_stream",
                "schema": {"dummy": "INT"},
            },
        ],
        "continuous_queries": [
            {
                "name": "stream_join_q",
                "input_stream": "pollution_stream",
                "output_stream": "joined_out",
                "query": "SELECT sensor_id, value, humidity FROM pollution_stream INNER JOIN weather_stream ON sensor_id = sensor_id WHERE value > 50",
            },
            {
                "name": "projection_q",
                "input_stream": "pollution_stream",
                "output_stream": "projection_out",
                "query": "SELECT sensor_id, value FROM pollution_stream WHERE value > 60",
            },
        ],
        "output_streams": [
            {
                "name": "joined_out",
                "topic": "joined_out",
                "schema": {"sensor_id": "STRING", "value": "FLOAT", "humidity": "FLOAT"},
            },
            {
                "name": "projection_out",
                "topic": "projection_out",
                "schema": {"sensor_id": "STRING", "value": "FLOAT"},
            },
        ],
    }


def test_engine_routing_multiple_streams_and_queries():
    engine = ExecutionEngine(_schema_for_rigorous_streams())

    captured_join = []
    captured_proj = []

    for q in engine.get_queries():
        if q["name"] == "stream_join_q":
            # stream join -> filter -> projection -> sink
            projection = q["pipeline"].next_op.next_op
            projection.next_op = CaptureSink(captured_join)
        elif q["name"] == "projection_q":
            # filter -> projection -> sink
            projection = q["pipeline"].next_op
            projection.next_op = CaptureSink(captured_proj)

    # Noise stream should not impact either query.
    engine.process_event("noise_stream", {"dummy": 1})
    assert captured_join == []
    assert captured_proj == []

    # Below threshold for both queries.
    engine.process_event("pollution_stream", {"sensor_id": "s1", "value": 45.0, "pollutant": "pm25"})
    engine.process_event("weather_stream", {"sensor_id": "s1", "humidity": 70.0, "zone": "z1"})
    assert captured_join == []
    assert captured_proj == []

    # Meets both query conditions and joins.
    engine.process_event("pollution_stream", {"sensor_id": "s2", "value": 71.0, "pollutant": "pm10"})
    engine.process_event("weather_stream", {"sensor_id": "s2", "humidity": 42.0, "zone": "z2"})

    assert len(captured_join) == 1
    assert captured_join[0] == {"sensor_id": "s2", "value": 71.0, "humidity": 42.0}

    assert len(captured_proj) == 1
    assert captured_proj[0] == {"sensor_id": "s2", "value": 71.0}


def test_engine_time_velocity_aggregate_window_behavior():
    schema = {
        "schema_name": "agg_time_velocity",
        "window_size": 60,
        "window_unit": "seconds",
        "velocity": {"type": "time", "value": 5},
        "input_streams": [
            {
                "name": "pollution_stream",
                "topic": "pollution_stream",
                "schema": {"sensor_id": "STRING", "value": "FLOAT"},
            }
        ],
        "continuous_queries": [
            {
                "name": "agg_q",
                "input_stream": "pollution_stream",
                "output_stream": "agg_out",
                "query": "SELECT sensor_id, AVG(value) FROM pollution_stream",
            }
        ],
        "output_streams": [
            {
                "name": "agg_out",
                "topic": "agg_out",
                "schema": {"sensor_id": "STRING", "AVG(value)": "FLOAT"},
            }
        ],
    }

    engine = ExecutionEngine(schema)
    captured = []

    # pipeline: window -> aggregate -> sink
    query = engine.get_queries()[0]
    window = query["pipeline"]
    aggregate = window.next_op
    aggregate.next_op = CaptureSink(captured)

    now = [0.0]
    window.time_fn = lambda: now[0]
    window.window_start_time = 0.0
    window.last_emit_time = 0.0

    engine.process_event("pollution_stream", {"sensor_id": "s1", "value": 10.0})
    now[0] = 4.0
    engine.process_event("pollution_stream", {"sensor_id": "s1", "value": 20.0})
    assert captured == []

    now[0] = 5.0
    engine.process_event("pollution_stream", {"sensor_id": "s1", "value": 30.0})

    assert len(captured) == 1
    assert captured[0]["sensor_id"] == "s1"
    assert captured[0]["AVG(value)"] == 20.0
