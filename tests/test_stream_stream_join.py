from core.execution.operators import StreamStreamJoinOperator


class Collector:
    def __init__(self):
        self.events = []

    def process(self, event):
        self.events.append(event)


def test_stream_stream_join_emits_on_matching_keys():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    join = StreamStreamJoinOperator(
        left_stream="pollution_stream",
        right_stream="weather_stream",
        left_field="sensor_id",
        right_field="sensor_id",
        operator="=",
        window_seconds=10,
        next_op=collector,
        time_fn=time_fn,
    )

    join.process("pollution_stream", {"sensor_id": "s1", "value": 51.2})
    assert collector.events == []

    join.process("weather_stream", {"sensor_id": "s1", "humidity": 82.0})
    assert len(collector.events) == 1
    assert collector.events[0]["sensor_id"] == "s1"
    assert collector.events[0]["value"] == 51.2
    assert collector.events[0]["humidity"] == 82.0


def test_stream_stream_join_respects_window_eviction():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    join = StreamStreamJoinOperator(
        left_stream="left_s",
        right_stream="right_s",
        left_field="id",
        right_field="id",
        operator="=",
        window_seconds=5,
        next_op=collector,
        time_fn=time_fn,
    )

    join.process("left_s", {"id": "a", "l": 1})
    now[0] = 6.0
    join.process("right_s", {"id": "a", "r": 2})

    assert collector.events == []
