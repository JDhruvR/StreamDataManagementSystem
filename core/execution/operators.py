import os
import sqlite3
import statistics
import threading
import time
import json
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
        
        if passed:
            print(f"[FILTER] Result: {event_val}", flush=True)  # ADD THIS
        
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
    def __init__(self, agg_configs, group_by_fields=[], next_op=None, select_list=None, state_table_name=None, db_path='data/aggregate_states.db'):
        if isinstance(agg_configs, dict):
            self.agg_configs = [agg_configs]
        else:
            self.agg_configs = list(agg_configs)
        self.group_by = group_by_fields
        self.next_op = next_op
        self.select_list = select_list if isinstance(select_list, list) else []
        self.state_table_name = state_table_name
        self.db_path = db_path
        self._conn = None

    def _get_conn(self):
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path) or '.', exist_ok=True)
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            if self.state_table_name:
                self._conn.execute(
                    f'CREATE TABLE IF NOT EXISTS "{self.state_table_name}" ('
                    'group_key TEXT PRIMARY KEY, state_data TEXT)'
                )
                self._conn.commit()
        return self._conn

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

            conn = self._get_conn() if self.state_table_name else None

            for key, grouped_events in groups.items():
                state_data = {}
                key_str = json.dumps(key)
                if conn:
                    row = conn.execute(
                        f'SELECT state_data FROM "{self.state_table_name}" WHERE group_key = ?',
                        (key_str,)
                    ).fetchone()
                    if row:
                        state_data = json.loads(row['state_data'])

                agg_results = {}
                for agg in self.agg_configs:
                    func = agg['func']
                    field = agg['field']
                    state_key = f"{func}_{field}"

                    values = [e[field] for e in grouped_events if field in e]
                    if not values and state_key + "_count" not in state_data:
                        continue

                    # Local values
                    w_count = len(values)
                    if(func=="SUM"):
                        w_sum = sum(values) if values else 0
                    elif(func=="MAX"):
                        w_max = max(values) if values else None
                    elif(func=="MIN"):
                        w_min = min(values) if values else None
                    elif(func=="AVG"):
                        w_avg = sum(values) / len(values) if values else 0

                    # Merge
                    
                    prev_count = state_data.get(f"{state_key}_count", 0)
                    new_count = prev_count + w_count
                    if(func=="SUM"):
                        prev_sum = state_data.get(f"{state_key}_sum", 0)
                        new_sum = prev_sum + w_sum
                    elif(func=="MAX"):
                        prev_max = state_data.get(f"{state_key}_max")
                        new_max = max(prev_max, w_max) if prev_max is not None else w_max
                    elif(func=="MIN"):
                        prev_min = state_data.get(f"{state_key}_min")
                        new_min = min(prev_min, w_min) if prev_min is not None else w_min
                    elif(func=="AVG"):
                        prev_avg = state_data.get(f"{state_key}_avg", 0)
                        new_avg = (prev_avg * prev_count + w_avg * w_count) / (prev_count + w_count) if prev_count + w_count > 0 else 0

                    # new_count = prev_count + w_count
                    # new_sum = prev_sum + w_sum

                    # if w_max is not None:
                    #     new_max = max(prev_max, w_max) if prev_max is not None else w_max
                    # else:
                    #     new_max = prev_max

                    # if w_min is not None:
                    #     new_min = min(prev_min, w_min) if prev_min is not None else w_min
                    # else:
                    #     new_min = prev_min

                    # Update state
                    state_data[f"{state_key}_count"] = new_count
                    if(func=="SUM"):
                        state_data[f"{state_key}_sum"] = new_sum
                    elif(func=="MAX"):
                        state_data[f"{state_key}_max"] = new_max
                    elif(func=="MIN"):
                        state_data[f"{state_key}_min"] = new_min
                    elif(func=="AVG"):
                        state_data[f"{state_key}_avg"] = new_avg

                    if new_count == 0:
                        continue

                    result_val = None
                    if func == 'COUNT': result_val = new_count
                    elif func == 'SUM': result_val = new_sum
                    elif func == 'AVG': result_val = new_avg
                    elif func == 'MAX': result_val = new_max
                    elif func == 'MIN': result_val = new_min

                    agg_results[(func, field)] = result_val

                if not agg_results:
                    continue

                if conn:
                    conn.execute(
                        f'INSERT INTO "{self.state_table_name}" (group_key, state_data) VALUES (?, ?) '
                        'ON CONFLICT(group_key) DO UPDATE SET state_data = excluded.state_data',
                        (key_str, json.dumps(state_data))
                    )
                    conn.commit()

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
        state_table_name=None,
        db_path='data/join_states.db'
    ):
        self.left_stream = left_stream
        self.right_stream = right_stream
        self.left_field = left_field
        self.right_field = right_field
        self.operator = operator
        self.window_seconds = float(window_seconds)
        self.next_op = next_op
        self.time_fn = time_fn or time.time
        
        self.state_table_name = state_table_name
        self.db_path = db_path
        self._conn = None
        self._lock = threading.Lock()

        self.left_buffer = deque()
        self.right_buffer = deque()
        self._init_state()

    def _get_conn(self):
        if self._conn is None:
            os.makedirs(os.path.dirname(self.db_path) or '.', exist_ok=True)
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def _init_state(self):
        if not self.state_table_name:
            return
        conn = self._get_conn()
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS "{self.state_table_name}" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_side TEXT,
                timestamp REAL,
                event_data TEXT
            )
        ''')
        conn.commit()
        
        try:
            rows = conn.execute(f'SELECT stream_side, timestamp, event_data FROM "{self.state_table_name}" ORDER BY timestamp ASC').fetchall()
            for row in rows:
                event = json.loads(row['event_data'])
                if row['stream_side'] == 'left':
                    self.left_buffer.append((row['timestamp'], event))
                elif row['stream_side'] == 'right':
                    self.right_buffer.append((row['timestamp'], event))
        except sqlite3.OperationalError:
            pass

    def _persist_event(self, stream_side, timestamp, event):
        if not self.state_table_name:
            return
        try:
            conn = self._get_conn()
            conn.execute(
                f'INSERT INTO "{self.state_table_name}" (stream_side, timestamp, event_data) VALUES (?, ?, ?)',
                (stream_side, timestamp, json.dumps(event))
            )
            conn.commit()
        except Exception as e:
            print(f"Failed to persist join event: {e}")

    def process(self, stream_name, event):
        if self.next_op is None:
            return

        outputs = []
        with self._lock:
            now = self.time_fn()
            self._evict_old(now)

            if stream_name == self.left_stream:
                self.left_buffer.append((now, event))
                self._persist_event('left', now, event)
                outputs = self._join_against_buffer(event, self.right_buffer, left_event=True)
            elif stream_name == self.right_stream:
                self.right_buffer.append((now, event))
                self._persist_event('right', now, event)
                outputs = self._join_against_buffer(event, self.left_buffer, left_event=False)

        for merged in outputs:
            self.next_op.process(merged)

    def _evict_old(self, now):
        cutoff = now - self.window_seconds
        evicted = False
        while self.left_buffer and self.left_buffer[0][0] < cutoff:
            self.left_buffer.popleft()
            evicted = True
        while self.right_buffer and self.right_buffer[0][0] < cutoff:
            self.right_buffer.popleft()
            evicted = True
            
        if evicted and self.state_table_name:
            try:
                conn = self._get_conn()
                conn.execute(f'DELETE FROM "{self.state_table_name}" WHERE timestamp < ?', (cutoff,))
                conn.commit()
            except Exception as e:
                print(f"Failed to evict old join events from DB: {e}")

    def _join_against_buffer(self, incoming_event, other_buffer, left_event):
        incoming_key = self.left_field if left_event else self.right_field
        other_key = self.right_field if left_event else self.left_field
        results = []

        if incoming_key not in incoming_event:
            return results

        for _, other_event in list(other_buffer):
            if other_key not in other_event:
                continue

            if not self._matches(incoming_event[incoming_key], other_event[other_key]):
                continue

            if left_event:
                merged = self._merge_events(incoming_event, other_event)
            else:
                merged = self._merge_events(other_event, incoming_event)
            results.append(merged)

        return results

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
