# Story Learning Documentation Standard

Every implemented Jira Story must have a dedicated `docs/learning/<story-key>/` directory before it is considered complete. The directory must contain:

```text
implementation-guide.md
architecture-notes.md
runbook.md
learning-notes.md
interview-qa.md
interview-talking-points.md
```

Documentation must describe the actual repository state. Use **implemented**, **validated**, **assumption**, and **deferred** explicitly; do not present planned technology or unexecuted validation as completed. The six documents cover code/configuration, architecture decisions and boundaries, reproducible operations, contextual data-engineering learning, evidence-based interview answers, and spoken interview narratives. Completion of this documentation and interview material is part of each future Story's Definition of Done.
