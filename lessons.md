# Lessons / Decisions Tracker

- [finished] Multiple stream joins (stream-to-stream) - implemented with window-aware processing-time buffers.
- [finished] Static joins (stream -> table INNER JOIN) - implemented in parser and execution pipeline.
- [finished] Schema file allows multiple streams - implemented (multiple input and output streams per schema).
- [finished] Kafka persistence removed from message-queue runtime - queue is now ephemeral-only.
- [finished] Window + velocity fixed at schema level - implemented and enforced per schema.
- [finished] No ad-hoc queries, only schema/CLI-deployed continuous queries - implemented.
- [finished] Full "never persist input stream" policy for message queue - no persistence toggle exposed.
- [finished] Velocity supports count/time model in config and execution operators.

## Next Priority Lessons

- [next] Add explicit schema validation for JOIN targets (stream vs reference table existence checks).
- [next] Add configurable join output column aliasing strategy (beyond `right_`/`table_` prefixes).
- [next] Add startup guardrails that reject non-ephemeral broker/topic configs at runtime.
