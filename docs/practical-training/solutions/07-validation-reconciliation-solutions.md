# Solutions — 07 Validation and reconciliation
## VR-01
Classify FAILED, not PASSED. `Invoke-RecordedCommand` reads native `LASTEXITCODE`, records stdout/stderr, and treats nonzero as failure. Ignoring it creates false evidence.
## VR-02
Counts differ legitimately: Bronze is history, Silver current state, Gold aggregate. Use R01/R02 anti-joins for source/Silver keys and R05 fact-to-mart grain comparison; preserve exception keys/counts/run ID.
## VR-03
`BLOCKED` is unavailable capability in the historical evidence matrix; `NOT_RUN` is intentionally not attempted; final V1 says physical exercises are runtime deferred, not V1 blockers.
