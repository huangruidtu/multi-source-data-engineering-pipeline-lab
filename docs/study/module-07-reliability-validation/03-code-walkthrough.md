# Module 07 — Code walkthrough

Reading order: `docs/project-evidence.md` → `validation/mdep-13-validation-matrix.yml` → `scripts/preflight-mdep-13.ps1` → `scripts/validate-mdep-13-e2e.ps1` → `validation/reconciliation/README.md` → `docs/closure/` → `tests/test_mdep13_validation_framework.py`.

The matrix is the reviewed list of executable stages and current blockers. Preflight discovers prerequisite gaps. The runner writes statuses/evidence only when exit semantics support them; the reviewed MDEP-13 false-PASSED bug was corrected so no failed/missing execution becomes success. Reconciliation templates compare appropriate keys/grains rather than asserting raw equality. Closure docs classify what is code/static/runtime/portfolio complete and preserve RD-08–13. These are control-plane artifacts; no validator writes Silver or Gold.
