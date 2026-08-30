import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class Mdep60FinalizationTests(unittest.TestCase):
    def test_authoritative_finalization_documents_exist_and_link_to_real_evidence(self):
        finalization = ROOT / 'docs' / 'finalization'
        for name in (
            'architecture-implementation-mapping.md', 'end-to-end-data-flow.md',
            'key-design-decisions.md', 'data-model-and-grain.md',
            'failure-and-recovery-reasoning.md', 'offline-validation-coverage.md',
            'production-gap-analysis.md', 'code-reading-guide.md',
            'learning-guide.md', 'interview-qa.md',
        ):
            self.assertTrue((finalization / name).is_file(), name)
        architecture = (finalization / 'architecture-implementation-mapping.md').read_text(encoding='utf-8')
        for phrase in ('record_hash', 'provide.transaction.metadata=true', 'conservatively rejected as a conflict', 'PyFlink', 'GOLD_GOLD', 'fct_payments', 'false-PASS'):
            self.assertIn(phrase, architecture)

    def test_readme_navigation_targets_exist_and_runtime_boundary_is_explicit(self):
        readme = (ROOT / 'README.md').read_text(encoding='utf-8')
        self.assertIn('Full infrastructure runtime validation is deferred from', readme)
        for path in (
            ROOT / 'docs/finalization/architecture-implementation-mapping.md',
            ROOT / 'docs/finalization/end-to-end-data-flow.md',
            ROOT / 'docs/finalization/key-design-decisions.md',
            ROOT / 'docs/finalization/data-model-and-grain.md',
            ROOT / 'docs/finalization/offline-validation-coverage.md',
            ROOT / 'docs/finalization/code-reading-guide.md',
            ROOT / 'docs/finalization/learning-guide.md',
            ROOT / 'docs/finalization/interview-qa.md',
        ):
            self.assertTrue(path.is_file(), path)


if __name__ == '__main__':
    unittest.main()
