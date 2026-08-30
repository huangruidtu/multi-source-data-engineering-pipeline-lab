import re
import unittest
from pathlib import Path


ROOT = Path('.')
CLOSURE = ROOT / 'docs/closure'


class Mdep14ClosureTests(unittest.TestCase):
    def test_status_matrix_covers_every_v1_story_without_runtime_claim(self):
        text = (CLOSURE / 'v1-final-status-matrix.md').read_text(encoding='utf-8')
        for story in range(6, 14): self.assertIn(f'MDEP-{story}', text)
        self.assertNotIn('RUNTIME_COMPLETE', text)

    def test_runtime_debt_and_required_closure_artifacts_exist(self):
        debt = (CLOSURE / 'runtime-debt-register.md').read_text(encoding='utf-8')
        for item in ('RD-08', 'RD-09', 'RD-10', 'RD-11', 'RD-12', 'RD-13'): self.assertIn(item, debt)
        for name in ('v1-status-audit.md', 'story-closure-policy.md', 'v1-retrospective.md', 'v1-baseline.md'):
            self.assertTrue((CLOSURE / name).is_file())
        self.assertTrue((ROOT / 'docs/interview/v1-final-story.md').is_file())

    def test_retrospective_captures_required_corrections_and_no_tool_sprawl(self):
        text = (CLOSURE / 'v1-retrospective.md').read_text(encoding='utf-8')
        for phrase in ('MDEP-9', 'MDEP-10', 'MDEP-11', 'GOLD_GOLD', 'fct_payments', 'false-PASSED'):
            self.assertIn(phrase, text)
        baseline = (CLOSURE / 'v1-baseline.md').read_text(encoding='utf-8')
        for forbidden in ('Databricks', 'Delta Lake', 'Redshift', 'BigQuery', 'Airbyte', 'Fivetran', 'Dagster', 'Prefect'):
            self.assertIn(forbidden, baseline)
        all_text = '\n'.join(path.read_text(encoding='utf-8') for path in CLOSURE.glob('*.md'))
        self.assertNotRegex(all_text, r'(?i)(aws_secret_access_key\s*=|password\s*=|BEGIN [A-Z ]*PRIVATE KEY)')


if __name__ == '__main__': unittest.main()
