# MDEP-13 evidence directory

Runtime evidence is intentionally local and ignored by Git. Each invocation of
`scripts/validate-mdep-13-e2e.ps1` creates `validation/evidence/<run-id>/` with
the following safe-to-review structure:

```text
preflight.json             # detected capabilities, not credentials
validation-summary.json    # per-stage status and evidence references
commands.log               # executed commands and stdout/stderr
environment.txt            # host/tool versions with sensitive values redacted
batch/  cdc/  silver/  gold/  failures/  reconciliation/
```

Do not store passwords, keys, tokens, connection strings, raw personally
identifiable production data, or unredacted environment dumps in this tree.
Commit a deliberately sanitized example only when it is stable and needed for
teaching; none is included by default.
