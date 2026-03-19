Viewed engine.py:1-81
Viewed operators.py:1-132

Here is a comprehensive breakdown of how your Streaming Data Management System works from end to end, along with a guide on how to expand its capabilities.

### 🔄 1. The Full Project Workflow

When you run this system, data travels through a pipeline made of four primary stages:

#### **Stage A: Data Generation & Ingestion (The Producer)**
* **File:** [sensors/pollution_sensor.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/sensors/pollution_sensor.py:0:0-0:0)
* **What happens:** This script simulates an IoT sensor. It generates random air quality readings and produces them as JSON events (e.g., `{"sensor_id": "S1", "value": 65.5, ...}`) to a Kafka topic named `pollution_stream`. 

#### **Stage B: Query Parsing (The Brains)**
* **Files:** [core/parser/grammar.lark](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/parser/grammar.lark:0:0-0:0) & [core/parser/sql_parser.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/parser/sql_parser.py:0:0-0:0)
* **What happens:** When [run_system.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/examples/run_system.py:0:0-0:0) executes a `SELECT` statement, the system doesn't know what to do with raw text. 
  1. The **Lark Grammar** defines the allowed SQL-like syntax (e.g., `CREATE STREAM`, `WINDOW TUMBLING`).
  2. The **SQLTransformer** translates this parsed tree into a Python dictionary (an Abstract Syntax Tree or AST) representing the query plan: what stream to read from, what filters to apply, and what windowing to use.

#### **Stage C: The Execution Engine (The Pipeline)**
* **File:** [core/execution/engine.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/execution/engine.py:0:0-0:0) & [core/execution/operators.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/execution/operators.py:0:0-0:0)
* **What happens:** The Engine takes the parsed AST query plan and builds a chain of **Operators**:
  1. **FilterOperator:** Checks the incoming event (e.g., `value > 50.0`). If it passes, it forwards the event to the next operator.
  2. **WindowOperator:** Buffers passing events over a defined time (e.g., 10 seconds). Once 10 seconds pass, it releases the batch of buffered events.
  3. **AggregateOperator:** Takes the released window batch, applies the aggregate function (like `AVG` or `MAX`), and formats a result.
  4. **SinkOperator:** The final destination. In [run_system.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/examples/run_system.py:0:0-0:0), this is just a `print` callback that outputs `[ALERT] High Pollution Detected`.

#### **Stage D: Event Consumption (The Consumer)**
* **File:** [streaming/kafka_client.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/streaming/kafka_client.py:0:0-0:0) & [examples/run_system.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/examples/run_system.py:0:0-0:0)
* **What happens:** A background thread continuously listens to the Kafka `pollution_stream` topic. Every time a new message arrives, it passes it into the top of the Execution Engine pipeline we built in Stage C, starting the flow through the operators.

---

### 🚀 2. How to Expand the System

The system's modular architecture makes it incredibly easy to extend without tearing down existing code. Here are the main ways you can expand it:

#### **A. Add New SQL Clauses (e.g., `LIMIT` or `JOIN`)**
If you want to support a new SQL clause, you need to update two files:
1. **Update the Grammar ([core/parser/grammar.lark](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/parser/grammar.lark:0:0-0:0)):**
   Add your new syntax rule. For example, to add `LIMIT`:
   ```lark
   select_query: "SELECT" select_list "FROM" identifier (window_clause)? (where_clause)? (limit_clause)?
   limit_clause: "LIMIT" literal
   ```
2. **Update the Parser ([core/parser/sql_parser.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/parser/sql_parser.py:0:0-0:0)):**
   Add a transformer method to extract the limit value into your query plan dictionary:
   ```python
   def limit_clause(self, items):
       return {"limit": items[0]}
   ```

#### **B. Add New Execution Pipeline Operators**
If you want to manipulate data in a new way (like adding a `Map` operator to transform data rows), you can add a new class in [core/execution/operators.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/execution/operators.py:0:0-0:0):
1. **Create the Operator:**
   ```python
   class MapOperator(Operator):
       def __init__(self, transform_func, next_op=None):
           self.transform_func = transform_func
           self.next_op = next_op

       def process(self, event):
           # Modify the event
           transformed_event = self.transform_func(event)
           if self.next_op:
               self.next_op.process(transformed_event)
   ```
2. **Wire it up in the Engine ([core/execution/engine.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/execution/engine.py:0:0-0:0)):**
   Add logic in [build_query_pipeline](cci:1://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/execution/engine.py:26:4-61:23) to insert your `MapOperator` into the pipeline chain when it detects a specific instruction in the parsed query plan.

#### **C. Swap Storage Backends**
Currently, [core/storage/table.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/storage/table.py:0:0-0:0) likely uses built-in memory dictionaries or SQLite. If you want to expand this to something like PostgreSQL or Redis for persistent table sinks:
1. Don't touch the engine or the operators!
2. Simply create a new `PostgresStorage` class that mimics the interface (`insert()`, [create_table()](cci:1://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/parser/sql_parser.py:33:4-38:9)) of your current `storage` object and replace the import in `table.py`.

#### **D. Add New Aggregation Functions**
If you look inside `AggregateOperator.process()` in [operators.py](cci:7://file:///home/ricky_ubuntu/SEM6/dm/Proj/core/execution/operators.py:0:0-0:0), you'll see a simple `if/elif` block handling `SUM`, `AVG`, `COUNT`, etc. 
To add something like `MEDIAN`, just calculate it there using `statistics.median(values)`—the syntax for `MIN/MAX/AVG` is already covered by the grammar fix we made earlier!



bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties