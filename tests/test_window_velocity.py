from core.execution.operators import WindowOperator


class Collector:
    def __init__(self):
        self.events = []

    def process(self, payload):
        self.events.append(payload)


def test_window_operator_count_velocity_emits_every_n_events():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    op = WindowOperator(
        {"type": "TUMBLING", "size": 60, "unit": "seconds"},
        velocity_config={"type": "count", "value": 2},
        next_op=collector,
        time_fn=time_fn,
    )

    op.process({"value": 1})
    assert collector.events == []

    op.process({"value": 2})
    assert len(collector.events) == 1
    assert collector.events[0] == [{"value": 1}, {"value": 2}]

    op.process({"value": 3})
    assert len(collector.events) == 1


def test_window_operator_time_velocity_emits_by_elapsed_time():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    op = WindowOperator(
        {"type": "TUMBLING", "size": 60, "unit": "seconds"},
        velocity_config={"type": "time", "value": 5},
        next_op=collector,
        time_fn=time_fn,
    )

    op.process({"value": 1})
    now[0] = 3.0
    op.process({"value": 2})
    assert collector.events == []

    now[0] = 5.1
    op.process({"value": 3})
    assert len(collector.events) == 1
    assert collector.events[0] == [{"value": 1}, {"value": 2}, {"value": 3}]
