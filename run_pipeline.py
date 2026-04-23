"""One-shot orchestration: runs all phases end-to-end."""
from __future__ import annotations
import subprocess
import sys

STEPS = [
    ("Phase 2 - Data generation",        ["-m", "src.data_generation.generate_data"]),
    ("Phase 3 - Prism transformation",   ["-m", "src.transformation.prism"]),
    ("Phase 4 - Accounting Center",      ["-m", "src.accounting_center.rule_engine"]),
    ("Phase 5 - GL posting",             ["-m", "src.gl_posting.post_to_gl"]),
    ("Phase 6 - Reconciliation",         ["-m", "src.reconciliation.recon_engine"]),
    ("Phase 7 - Exceptions",             ["-m", "src.exceptions.detect_exceptions"]),
    ("Phase 8 - AI explanations",        ["-m", "src.ai_explanation.explain"]),
    ("Warehouse - Load DuckDB",          ["-m", "src.warehouse.load_duckdb"]),
]


def main() -> None:
    for name, args in STEPS:
        print(f"\n=== {name} ===")
        r = subprocess.run([sys.executable, *args])
        if r.returncode != 0:
            print(f"FAILED at {name}")
            sys.exit(r.returncode)
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
