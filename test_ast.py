from core.parser.sql_parser import parser

sql = """CREATE STREAM pollution_stream ( value FLOAT ) WITH (topic=\"test\")"""
print(parser.parse(sql).pretty())
