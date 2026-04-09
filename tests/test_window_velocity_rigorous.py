import pytest

from core.execution.operators import WindowOperator


class Collector:
    def __init__(self):
        self.payloads = []

    def process(self, payload):
        self.payloads.append(payload)


@pytest.mark.parametrize("velocity", [1, 2, 3, 5])
def test_count_velocity_matrix(velocity):
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    op = WindowOperator(
        {"type": "TUMBLING", "size": 30, "unit": "seconds"},
        velocity_config={"type": "count", "value": velocity},
        next_op=collector,
        time_fn=time_fn,
    )

    for i in range(velocity):
        op.process({"idx": i})

    assert len(collector.payloads) == 1
    assert len(collector.payloads[0]) == velocity


@pytest.mark.parametrize("velocity_seconds", [1, 2, 5])
def test_time_velocity_matrix(velocity_seconds):
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    op = WindowOperator(
        {"type": "TUMBLING", "size": 60, "unit": "seconds"},
        velocity_config={"type": "time", "value": velocity_seconds},
        next_op=collector,
        time_fn=time_fn,
    )

    op.process({"tick": 0})
    now[0] = velocity_seconds - 0.01
    op.process({"tick": 1})
    assert collector.payloads == []

    now[0] = velocity_seconds
    op.process({"tick": 2})
    assert len(collector.payloads) == 1
    assert collector.payloads[0] == [{"tick": 0}, {"tick": 1}, {"tick": 2}]


def test_tumbling_window_rollover_emits_previous_window():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    op = WindowOperator(
        {"type": "TUMBLING", "size": 5, "unit": "seconds"},
        velocity_config={"type": "count", "value": 100},
        next_op=collector,
        time_fn=time_fn,
    )

    op.process({"v": 1})
    op.process({"v": 2})
    now[0] = 5.0
    op.process({"v": 3})

    assert len(collector.payloads) == 1
    assert collector.payloads[0] == [{"v": 1}, {"v": 2}]
