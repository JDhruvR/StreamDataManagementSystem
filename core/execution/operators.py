import time
from abc import ABC, abstractmethod
from collections import deque
import statistics

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
    def __init__(self, window_config, next_op=None):
        self.window_config = window_config
        self.next_op = next_op
        self.buffer = []
        self.window_start_time = time.time()
        
        # Calculate size in seconds
        size = float(window_config['size'])
        unit = window_config['unit']
        if unit == 'MINUTES': size *= 60
        elif unit == 'HOURS': size *= 3600
        
        self.window_size_seconds = size
        self.window_type = window_config['type'] # TUMBLING or SLIDING

    def process(self, event):
        current_time = time.time()
        self.buffer.append(event)
        
        if self.window_type == 'TUMBLING':
            if current_time - self.window_start_time >= self.window_size_seconds:
                # Window is full, emit it
                self.emit_window()
                # Reset window
                self.buffer = []
                self.window_start_time = current_time
                
        elif self.window_type == 'SLIDING':
            # Evict old events
            self.buffer = [e for e in self.buffer if current_time - self._parse_event_time(e) <= self.window_size_seconds]
            self.emit_window()
            
    def _parse_event_time(self, event):
        # Simplification: return current time to mimic processing-time characteristic
        # For event-time, would parse event['timestamp']
        return time.time()

    def emit_window(self):
        if self.next_op and self.buffer:
            self.next_op.process(list(self.buffer))

class AggregateOperator(Operator):
    def __init__(self, agg_config, group_by_fields=[], next_op=None):
        self.agg_config = agg_config
        self.group_by = group_by_fields
        self.next_op = next_op

    def process(self, events):
        # Expects a list of events (e.g. from a window)
        if not events: return
        
        if not isinstance(events, list):
            events = [events]

        func = self.agg_config['func']
        field = self.agg_config['field']
        
        # Simple non-grouped aggregation for now
        # Expand later for complex GROUP BY
        try:
            values = [e[field] for e in events if field in e]
            if not values:
                return

            result_val = None
            if func == 'COUNT': result_val = len(values)
            elif func == 'SUM': result_val = sum(values)
            elif func == 'AVG': result_val = statistics.mean(values)
            elif func == 'MAX': result_val = max(values)
            elif func == 'MIN': result_val = min(values)

            result_event = {"aggregate": func, "field": field, "value": result_val}
            
            # Carry over group_by fields if they are common (simplified)
            for gb_field in self.group_by:
                 if gb_field in events[0]:
                     result_event[gb_field] = events[0][gb_field]
                     
            if self.next_op:
                self.next_op.process(result_event)
        except Exception as e:
            print(f"Aggregation error: {e}")

class SinkOperator(Operator):
    def __init__(self, target_table=None, callback=None):
        self.target_table = target_table
        self.callback = callback

    def process(self, event):
        if self.callback:
            self.callback(event)
        if self.target_table:
            from core.storage.table import storage
            storage.insert(self.target_table, event)
