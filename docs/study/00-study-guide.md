# MDEP V1 Study Guide

This pack is a retrieval aid, not a claim that the platform has run in production. Read **IMPLEMENTED** for code/configuration present in the repository, **OFFLINE / STATICALLY VALIDATED** for contract tests and parsers, and **RUNTIME DEFERRED — not runtime validated** for the Docker, cloud, and engine exercises listed in [the deferred register](../closure/runtime-debt-register.md).

## Structure and use

Start with [master architecture](01-master-architecture.md), then study modules 00–07 in dependency order. In every module, read `01` for boundaries, `02` to learn terms, `03` beside the cited files, `04` to narrate a record, `05` to discuss failure, `06` to practise answers, and `07` aloud. Never memorise an answer without checking its code path.

## Paths by role

| Role | Priority modules | Focus |
| --- | --- | --- |
| Data Engineer | 01, 02, 03, 06, 07 | contracts, batch, SQL models, quality |
| Data Platform Engineer | 00, 04, 05, 07 | ownership, CDC, state, evidence |
| Streaming Data Engineer | 04, 05, 07 | WAL, ordering, state, recovery |
| Analytics Engineer | 03, 06, 07 | Iceberg boundary, dbt, dimensional grain |
| Platform/DevOps to DE | 02, 04, 05, 07 | data semantics before infrastructure |

## Time-boxes

**2 hours:** master architecture, module 00 talking points, final playbook. **4 hours:** add modules 03–05. **8 hours:** add contracts, Airflow, Snowflake/dbt, and answer 30 Top-100 questions. **One day:** complete modules 00–07 plus failure files. **Two days:** read cited code and rehearse answers aloud. **V1.x lab:** execute the historically `BLOCKED` validation-matrix exercises in a suitable environment, record evidence, then amend wording only where evidence permits.

## Interview practice protocol

Use **PAUSE → CLARIFY → DIRECT → STRUCTURE → STOP**: pause to choose a project fact; clarify the question’s scope; give the answer first; structure it as boundary, mechanism, trade-off, evidence; stop after the evidence and invite follow-up. Say “I implemented/configured/statically tested” for this repository. Say “I would validate” rather than “I observed” for blocked runtime paths.

## Readiness checklist

- I can draw both paths and name the canonical writer of each Silver dataset.
- I can explain MDEP-9 version order and MDEP-11 CDC order without calling a Kafka partition number freshness.
- I can state the Gold grain and why `fct_payments` rebuilds.
- I can name every blocked runtime environment and the evidence needed to close it.
- I can give a 30-second answer, then stop.
