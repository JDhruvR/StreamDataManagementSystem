import pytest

from core.execution.operators import StreamStreamJoinOperator


class Collector:
    def __init__(self):
        self.events = []

    def process(self, event):
        self.events.append(event)


@pytest.mark.parametrize("join_operator,left,right,should_match", [
    ("=", "a", "a", True),
    ("=", "a", "b", False),
    ("!=", "a", "b", True),
    ("!=", "a", "a", False),
])
def test_join_operator_matrix(join_operator, left, right, should_match):
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    join = StreamStreamJoinOperator(
        left_stream="left",
        right_stream="right",
        left_field="k",
        right_field="k",
        operator=join_operator,
        window_seconds=10,
        next_op=collector,
        time_fn=time_fn,
    )

    join.process("left", {"k": left, "lv": 1})
    join.process("right", {"k": right, "rv": 2})

    assert (len(collector.events) == 1) is should_match


def test_multiple_stream_pairs_and_cross_noise():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    join = StreamStreamJoinOperator(
        left_stream="s1",
        right_stream="s2",
        left_field="id",
        right_field="id",
        operator="=",
        window_seconds=20,
        next_op=collector,
        time_fn=time_fn,
    )

    join.process("s1", {"id": "a", "x": 1})
    join.process("s1", {"id": "b", "x": 2})
    join.process("s2", {"id": "z", "y": 9})  # noise
    join.process("s2", {"id": "a", "y": 3})
    join.process("s2", {"id": "b", "y": 4})

    assert len(collector.events) == 2
    assert {e["id"] for e in collector.events} == {"a", "b"}


def test_right_field_collision_gets_prefixed():
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    join = StreamStreamJoinOperator(
        left_stream="left",
        right_stream="right",
        left_field="id",
        right_field="id",
        operator="=",
        window_seconds=10,
        next_op=collector,
        time_fn=time_fn,
    )

    join.process("left", {"id": "k1", "value": 10})
    join.process("right", {"id": "k1", "value": 99, "humidity": 30})

    assert len(collector.events) == 1
    merged = collector.events[0]
    assert merged["value"] == 10
    assert merged["right_value"] == 99
    assert merged["humidity"] == 30


@pytest.mark.parametrize("window_seconds", [1, 2, 5])
def test_window_eviction_across_multiple_windows(window_seconds):
    now = [0.0]

    def time_fn():
        return now[0]

    collector = Collector()
    join = StreamStreamJoinOperator(
        left_stream="l",
        right_stream="r",
        left_field="id",
        right_field="id",
        operator="=",
        window_seconds=window_seconds,
        next_op=collector,
        time_fn=time_fn,
    )

    join.process("l", {"id": "old"})
    now[0] = window_seconds + 0.001
    join.process("r", {"id": "old"})

    assert collector.events == []
