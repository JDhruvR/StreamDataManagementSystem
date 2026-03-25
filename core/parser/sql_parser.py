import os
from lark import Lark, Transformer

# Load the grammar file
grammar_path = os.path.join(os.path.dirname(__file__), 'grammar.lark')
with open(grammar_path, 'r') as f:
    grammar = f.read()

parser = Lark(grammar, start='start', parser='lalr')

class SQLTransformer(Transformer):
    def identifier(self, items):
        return items[0].value

    def literal(self, items):
        val = items[0].value
        if '.' in val:
            return float(val)
        return int(val)

    def string_literal(self, items):
        return items[0].value[1:-1]  # Remove quotes

    def data_type(self, items):
        return items[0].value

    def column_def(self, items):
        return {"name": items[0], "type": items[1]}

    def column_defs(self, items):
        return list(items)

    def create_table(self, items):
        return {
            "type": "create_table",
            "name": items[0],
            "columns": items[1]
        }

    def stream_option(self, items):
        return {items[0]: items[1]}

    def stream_options(self, items):
        opts = {}
        for item in items:
            opts.update(item)
        return opts

    def create_stream(self, items):
        return {
            "type": "create_stream",
            "name": items[0],
            "columns": items[1],
            "options": items[2]
        }

    def operator(self, items):
        return items[0].value

    def condition_value(self, items):
        return items[0]

    def condition(self, items):
        return {"field": items[0], "operator": items[1], "value": items[2]}

    def where_clause(self, items):
        return items[0]

    def join_clause(self, items):
        return {
            "join_type": "INNER",
            "table": items[0],
            "left_field": items[1],
            "right_field": items[2],
        }

    def func_name(self, items):
        return items[0].value

    def aggregate_expr(self, items):
        return {"func": items[0], "field": items[1]}

    def select_expr(self, items):
        return items[0]

    def select_list(self, items):
        if len(items) == 1 and hasattr(items[0], 'value') and items[0].value == '*':
            return "*"
        return list(items)

    def select_query(self, items):
        query = {
            "type": "select_query",
            "select": items[0],
            "from": items[1]
        }
        
        # Parse JOIN/WHERE clauses if present
        if len(items) > 2:
            for item in items[2:]:
                if isinstance(item, dict) and "join_type" in item:
                    query["join"] = item
                elif isinstance(item, dict) and "field" in item and "operator" in item:
                    query["where"] = item

        return query

    def statement(self, items):
        return items[0]

    def start(self, items):
        return list(items)


def parse_sql(sql_text):
    tree = parser.parse(sql_text)
    return SQLTransformer().transform(tree)


if __name__ == '__main__':
    # Test parser
    sql = """
    CREATE STREAM pollution_stream (
        timestamp STRING,
        sensor_id STRING,
        pollutant STRING,
        value FLOAT
    ) WITH (topic="pollution_stream");

    CREATE TABLE high_pollution_table (
        sensor_id STRING,
        avg_value FLOAT
    );

    SELECT sensor_id, AVG(value)
    FROM pollution_stream
    WHERE value > 50.0;
    """
    
    parsed = parse_sql(sql.strip())
    import json
    print(json.dumps(parsed, indent=2))

