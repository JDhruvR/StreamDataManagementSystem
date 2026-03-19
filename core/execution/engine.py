from core.parser.sql_parser import parse_sql
from core.execution.operators import FilterOperator, WindowOperator, AggregateOperator, SinkOperator
from core.storage.table import storage

class ExecutionEngine:
    def __init__(self):
        self.streams = {}
        self.tables = {}
        self.queries = []

    def execute_ddl(self, statement):
        if statement['type'] == 'create_table':
            table_name = statement['name']
            schema = [
                {'name': col['name'], 'type': col['type']} 
                for col in statement['columns']
            ]
            self.tables[table_name] = schema
            storage.create_table(table_name, schema)
            print(f"Table '{table_name}' created.")

        elif statement['type'] == 'create_stream':
            stream_name = statement['name']
            self.streams[stream_name] = statement
            print(f"Stream '{stream_name}' registered.")

    def build_query_pipeline(self, query_plan, output_callback=None):
        if query_plan['type'] != 'select_query':
            print("Not a continuous select query.")
            return None

        # Sink
        pipeline = SinkOperator(callback=output_callback)
        
        # Aggregate logic
        select_list = query_plan['select']
        agg_config = None
        group_by = []
        if isinstance(select_list, list):
            for item in select_list:
                if isinstance(item, dict) and 'func' in item:
                    agg_config = item
                else:
                    group_by.append(item)
                    
        if agg_config:
            pipeline = AggregateOperator(agg_config, group_by_fields=group_by, next_op=pipeline)

        # Window logic
        if 'window' in query_plan:
            pipeline = WindowOperator(query_plan['window'], next_op=pipeline)

        # Filter logic
        if 'where' in query_plan:
            pipeline = FilterOperator(query_plan['where'], next_op=pipeline)

        self.queries.append({
            'source': query_plan['from'],
            'pipeline': pipeline
        })
        
        return pipeline

    def handle_statement(self, sql_text, callback=None):
        try:
            plans = parse_sql(sql_text)
            for plan in plans:
                if plan['type'] in ('create_table', 'create_stream'):
                    self.execute_ddl(plan)
                elif plan['type'] == 'select_query':
                    self.build_query_pipeline(plan, output_callback=callback)
                    print("Continuous query accepted and pipeline built.")
        except Exception as e:
            print(f"Failed to execute statement: {e}")

    def process_event(self, source_stream, event):
        """ Feed an event from the source_stream to all relevant queries """
        for q in self.queries:
            if q['source'] == source_stream:
                q['pipeline'].process(event)
