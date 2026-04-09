import os
import sqlite3
import statistics
import time
from abc import ABC, abstractmethod
from collections import deque

class Operator(ABC):
    @abstractmethod
    def process(self, event):
        pass

class FilterOperator(Operator):
    def __init__(self, condition, next_op=None):
        self.condition = condition
        self.next_op = next_op

    def process(self, event):
        field = self.condition['field']
        op = self.condition['operator']
        val = self.condition['value']

        if field not in event:
            return

        event_val = event[field]
        passed = False
        
        if op == '>': passed = event_val > val
        elif op == '<': passed = event_val < val
        elif op == '=': passed = event_val == val
        elif op == '>=': passed = event_val >= val
        elif op == '<=': passed = event_val <= val
        elif op == '!=': passed = event_val != val

        if passed and self.next_op:
            self.next_op.process(event)

class WindowOperator(Operator):
    def __init__(self, window_config, velocity_config=None, next_op=None, time_fn=None):
        self.window_config = window_config
        self.velocity_config = velocity_config or {"type": "count", "value": 1}
        self.next_op = next_op
        self.time_fn = time_fn or time.time

        size = float(window_config["size"])
        unit = str(window_config["unit"]).upper()
        if unit == "MINUTES":
            size *= 60
        elif unit == "HOURS":
            size *= 3600
        self.window_size_seconds = size
        self.window_type = str(window_config.get("type", "TUMBLING")).upper()

        velocity_type = str(self.velocity_config.get("type", "count")).upper()
        velocity_value = float(self.velocity_config.get("value", 1))
        if velocity_value <= 0:
            raise ValueError("velocity value must be positive")
        self.velocity_type = velocity_type
        self.velocity_value = velocity_value

        now = self.time_fn()
        self.window_start_time = now
        self.last_emit_time = now
        self.events_since_emit = 0
        self.buffer = deque()

    def process(self, event):
        now = self.time_fn()

        if self.window_type == "TUMBLING":
            self._roll_tumbling_window(now)
            self.buffer.append((now, event))
        elif self.window_type == "SLIDING":
            self.buffer.append((now, event))
            self._evict_old_events(now)
        else:
            raise ValueError(f"Unsupported window type: {self.window_type}")

        self.events_since_emit += 1
        if self._should_emit(now):
            self.emit_window()
            self._reset_velocity_state(now)

    def _roll_tumbling_window(self, now):
        elapsed = now - self.window_start_time
        if elapsed < self.window_size_seconds:
            return

        if self.buffer:
            self.emit_window()
            self._reset_velocity_state(now)

        windows_advanced = int(elapsed // self.window_size_seconds)
        self.window_start_time += windows_advanced * self.window_size_seconds
        self.buffer.clear()

    def _evict_old_events(self, now):
        lower_bound = now - self.window_size_seconds
        while self.buffer and self.buffer[0][0] < lower_bound:
            self.buffer.popleft()

    def _should_emit(self, now):
        if self.velocity_type == "COUNT":
            return self.events_since_emit >= int(self.velocity_value)

        if self.velocity_type == "TIME":
            return now - self.last_emit_time >= self.velocity_value

        raise ValueError(f"Unsupported velocity type: {self.velocity_type}")

    def _reset_velocity_state(self, now):
        self.events_since_emit = 0
        self.last_emit_time = now

    def emit_window(self):
        if not self.next_op or not self.buffer:
            return
        events = [event for _, event in self.buffer]
        self.next_op.process(events)


class ProjectionOperator(Operator):
    def __init__(self, select_list, next_op=None):
        self.select_list = select_list
        self.next_op = next_op

    def process(self, event):
        if self.next_op is None:
            return

        if self.select_list == '*':
            self.next_op.process(event)
            return

        if not isinstance(self.select_list, list):
            self.next_op.process(event)
            return

        projected = {}
        for item in self.select_list:
            if isinstance(item, str) and item in event:
                projected[item] = event[item]

        self.next_op.process(projected)

class AggregateOperator(Operator):
    def __init__(self, agg_configs, group_by_fields=[], next_op=None, select_list=None):
        if isinstance(agg_configs, dict):
            self.agg_configs = [agg_configs]
        else:
            self.agg_configs = list(agg_configs)
        self.group_by = group_by_fields
        self.next_op = next_op
        self.select_list = select_list if isinstance(select_list, list) else []

    def process(self, events):
        # Expects a list of events (e.g. from a window)
        if not events: return
        
        if not isinstance(events, list):
            events = [events]

        # If non-aggregate fields are selected, aggregate per unique key tuple.
        try:
            groups = {}
            if self.group_by:
                for event in events:
                    key = tuple(event.get(gb_field) for gb_field in self.group_by)
                    groups.setdefault(key, []).append(event)
            else:
                groups[()] = events

            for key, grouped_events in groups.items():
                agg_results = {}
                for agg in self.agg_configs:
                    func = agg['func']
                    field = agg['field']
                    values = [e[field] for e in grouped_events if field in e]
                    if not values:
                        continue

                    result_val = None
                    if func == 'COUNT': result_val = len(values)
                    elif func == 'SUM': result_val = sum(values)
                    elif func == 'AVG': result_val = statistics.mean(values)
                    elif func == 'MAX': result_val = max(values)
                    elif func == 'MIN': result_val = min(values)

                    agg_results[(func, field)] = result_val

                if not agg_results:
                    continue

                # Build output in SELECT order, matching SQL-like projection shape.
                if self.select_list:
                    result_event = {}
                    for item in self.select_list:
                        if isinstance(item, str):
                            if item in self.group_by:
                                gb_idx = self.group_by.index(item)
                                result_event[item] = key[gb_idx]
                            elif grouped_events and item in grouped_events[0]:
                                result_event[item] = grouped_events[0][item]
                        elif isinstance(item, dict) and 'func' in item and 'field' in item:
                            func = item['func']
                            field = item['field']
                            if (func, field) in agg_results:
                                result_event[f"{func}({field})"] = agg_results[(func, field)]
                else:
                    result_event = {}
                    for (func, field), val in agg_results.items():
                        result_event[f"{func}({field})"] = val
                    for idx, gb_field in enumerate(self.group_by):
                        result_event[gb_field] = key[idx]

                if self.next_op:
                    self.next_op.process(result_event)
        except Exception as e:
            print(f"Aggregation error: {e}")


class StreamStreamJoinOperator:
    """INNER JOIN between two streams using processing-time window buffers."""

    def __init__(
        self,
        left_stream,
        right_stream,
        left_field,
        right_field,
        operator="=",
        window_seconds=10,
        next_op=None,
        time_fn=None,
    ):
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.left_field = left_field
        self.right_field = right_field
        self.operator = operator
        self.window_seconds = float(window_seconds)
        self.next_op = next_op
        self.time_fn = time_fn or time.time

        self.left_buffer = deque()
        self.right_buffer = deque()

    def process(self, stream_name, event):
        if self.next_op is None:
            return

        now = self.time_fn()
        self._evict_old(now)

        if stream_name == self.left_stream:
            self.left_buffer.append((now, event))
            self._join_against_buffer(event, self.right_buffer, left_event=True)
            return

        if stream_name == self.right_stream:
            self.right_buffer.append((now, event))
            self._join_against_buffer(event, self.left_buffer, left_event=False)

    def _evict_old(self, now):
        cutoff = now - self.window_seconds
        while self.left_buffer and self.left_buffer[0][0] < cutoff:
            self.left_buffer.popleft()
        while self.right_buffer and self.right_buffer[0][0] < cutoff:
            self.right_buffer.popleft()

    def _join_against_buffer(self, incoming_event, other_buffer, left_event):
        incoming_key = self.left_field if left_event else self.right_field
        other_key = self.right_field if left_event else self.left_field

        if incoming_key not in incoming_event:
            return

        for _, other_event in other_buffer:
            if other_key not in other_event:
                continue

            if not self._matches(incoming_event[incoming_key], other_event[other_key]):
                continue

            if left_event:
                merged = self._merge_events(incoming_event, other_event)
            else:
                merged = self._merge_events(other_event, incoming_event)
            self.next_op.process(merged)

    def _matches(self, left_value, right_value):
        if self.operator == "=":
            return left_value == right_value
        if self.operator == "!=":
            return left_value != right_value
        raise ValueError(f"Unsupported join operator: {self.operator}")

    @staticmethod
    def _merge_events(left_event, right_event):
        merged = dict(left_event)
        for key, value in right_event.items():
            if key in merged:
                merged[f"right_{key}"] = value
            else:
                merged[key] = value
        return merged


class JoinOperator(Operator):
    """INNER JOIN stream events with rows from a persistent SQLite table."""

    def __init__(self, table_name, left_field, right_field, operator='=', next_op=None, db_path='data/static_tables.db'):
        self.table_name = table_name
        self.left_field = left_field
        self.right_field = right_field
        self.operator = operator
        self.next_op = next_op
        self.db_path = db_path
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path) or '.', exist_ok=True)
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def process(self, event):
        if self.next_op is None:
            return

        if self.left_field not in event:
            return

        join_value = event[self.left_field]
        conn = self._get_conn()

        try:
            op_map = {'=': '=', '!=': '!='}
            sql_op = op_map.get(self.operator)
            if sql_op is None:
                print(f"Join error: unsupported join operator '{self.operator}'")
                return

            query = f'SELECT * FROM "{self.table_name}" WHERE "{self.right_field}" {sql_op} ?'
            rows = conn.execute(query, (join_value,)).fetchall()
        except Exception as e:
            print(f"Join error on table '{self.table_name}': {e}")
            return

        # INNER JOIN behavior: emit only when at least one matching table row exists.
        for row in rows:
            joined_event = dict(event)
            row_dict = dict(row)
            for key, val in row_dict.items():
                if key in joined_event:
                    joined_event[f"table_{key}"] = val
                else:
                    joined_event[key] = val
            self.next_op.process(joined_event)

class SinkOperator(Operator):
    def __init__(self, target_table=None, target_stream=None, target_topic=None, callback=None):
        self.target_table = target_table
        self.target_stream = target_stream
        self.target_topic = target_topic
        self.callback = callback

    def process(self, event):
        if self.callback:
            self.callback(event)
        
        if self.target_table:
            from core.storage.table import storage
            storage.insert(self.target_table, event)
        
        if self.target_stream and self.target_topic:
            from streaming.kafka_client import StreamProducer
            producer = StreamProducer()
            producer.send(self.target_topic, event)
