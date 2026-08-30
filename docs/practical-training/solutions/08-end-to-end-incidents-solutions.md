# Solutions — 08 End-to-end incidents
## E2E-01
Owner is extractor/pipeline boundary. Reject partial publishing: all pages must return before deterministic Bronze publication. Retry source extraction; later reconcile one canonical operation. No live endpoint behavior is claimed.
## E2E-02
Flink CDC model rejects delete 420 as lower LSN after accepted 500; current Silver remains updated. A subsequent dbt run should retain its Gold fact because upstream current state did not delete it. Kafka/Flink/Iceberg/dbt physical execution remains deferred.
## E2E-03
Equal counts can mask swapped/missing keys. Use Silver→Gold anti-joins, inspect source/Silver scope, duplicate/null checks, and save query, run ID, exception keys, attributes, and explicit decision.
