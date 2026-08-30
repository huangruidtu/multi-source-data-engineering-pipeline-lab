# MDEP-13 learning notes

The main lesson is that a validation harness is not runtime proof. This Story makes that distinction machine-readable: the matrix records what was required, how to run it, what success means, the current status, evidence location, and blocker.

Reconciliation is broader than counts. Compare the correct semantic sets with business keys, attribute values, aggregate values, duplicate/null checks, delete expectations, and explicit exception lists. For example, Bronze may retain deleted history while Silver should not; a payment orphan may be a warning to investigate, not a row to discard.

For recovery, separate retry, rerun, replay, and backfill. A retry repeats a failed operation; a rerun repeats a logical interval; a replay repeats retained history; a backfill processes a historical scope. Their safety depends on canonical keys, version ordering, checkpoints, and reconciliation evidence.
