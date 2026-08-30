# Top interview questions

1. **How do you validate end to end?** Preflight, bounded run, evidence, business-key reconciliation, then failure/recovery proof.
2. **Why are row counts not enough?** Layer semantics differ; compare keys, attributes, aggregates, and exceptions.
3. **How do you prove idempotency?** Run identical logical input twice and compare canonical keys/state.
4. **How do you prevent stale replay?** Compare the full version order; a different hash alone is never freshness.
5. **How do you recover CDC?** Inspect connector/slot/offset/checkpoint evidence, replay retained history, and reconcile state.
6. **How do you discuss unvalidated work?** State exactly what is implemented, statically tested, and still missing physical evidence.
