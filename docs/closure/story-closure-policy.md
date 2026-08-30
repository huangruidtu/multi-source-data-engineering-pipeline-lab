# V1 Story closure policy

Three independent dimensions prevent a Jira status from hiding important facts:

- **Implementation completion:** code, configuration, documentation, and focused static checks are merged.
- **Runtime acceptance:** required physical behavior has been executed with retained evidence.
- **Portfolio readiness:** architecture, limitations, learning material, and interview narrative are complete.

A Story can therefore be implementation complete, runtime blocked, and portfolio ready.

## Jira policy

| Status | Use when |
| --- | --- |
| Done | Implementation/static/docs are complete, known runtime debt is registered, and the Story does not require observed runtime behavior to be complete. |
| In Review | Implementation is merged and static validation passed, but final evidence/closure review is pending. |
| In Progress | An implementation deliverable/correctness fix is still missing, or a runtime-specific subtask explicitly requires physical acceptance evidence. |

Runtime-specific subtasks remain In Progress until their evidence exists. Completed implementation/documentation subtasks should not remain In Progress merely because a separate runtime acceptance item is blocked.
