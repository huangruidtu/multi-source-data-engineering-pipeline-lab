# V1 Story closure policy

Four independent dimensions prevent a Jira status from hiding important facts:

- **Implementation completion:** code, configuration, documentation, and focused static checks are merged.
- **Runtime acceptance:** required physical behavior has been executed with retained evidence.
- **Portfolio readiness:** architecture, limitations, learning material, and interview narrative are complete.
- **Runtime-deferred status:** physical integration is deliberately outside final
  V1 acceptance; a deferred item remains evidence of scope, not incomplete
  implementation.

A Story can therefore be implementation complete, offline/static validated,
runtime deferred, and portfolio ready.

## Jira policy

| Status | Use when |
| --- | --- |
| Done | Implementation/configuration/offline validation/docs are complete and runtime work is explicitly deferred or separately tracked. |
| In Review | Implementation is merged and static validation passed, but final evidence/closure review is pending. |
| In Progress | An implementation deliverable, offline validation, documentation, or correctness fix is still missing. Runtime-deferred work alone is not a V1 blocker. |

Runtime-only subtasks are to be closed/reframed with a dated scope-decision
comment when Jira is reconciled. Completed implementation/documentation
subtasks must not remain In Progress merely because physical execution is
deferred.
