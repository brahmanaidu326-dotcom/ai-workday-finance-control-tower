"""Load all CSVs into a single DuckDB file for Power BI / Streamlit consumption."""
from __future__ import annotations
from pathlib import Path
import sys

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import RAW_DIR, PROCESSED_DIR, OUTPUTS_DIR, DUCKDB_PATH

DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

TABLES = {
    "source_transactions":       RAW_DIR / "source_transactions.csv",
    "worktag_master":            RAW_DIR / "worktag_master.csv",
    "accounting_center_rules":   RAW_DIR / "accounting_center_rules.csv",
    "exception_lookup":          RAW_DIR / "exception_lookup.csv",
    "close_calendar":            RAW_DIR / "close_calendar.csv",
    "chart_of_accounts":         RAW_DIR / "chart_of_accounts.csv",
    "prism_transformed_transactions": PROCESSED_DIR / "prism_transformed_transactions.csv",
    "journal_output":            PROCESSED_DIR / "journal_output.csv",
    "gl_postings":               PROCESSED_DIR / "gl_postings.csv",
    "reconciliation_results":    PROCESSED_DIR / "reconciliation_results.csv",
    "exceptions":                OUTPUTS_DIR / "exceptions" / "exceptions.csv",
    "exceptions_with_ai":        OUTPUTS_DIR / "exceptions" / "exceptions_with_ai.csv",
}


def main():
    for ext in ("", ".wal", ".tmp"):
        p = Path(str(DUCKDB_PATH) + ext)
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
    try:
        con = duckdb.connect(str(DUCKDB_PATH))
    except Exception as e:
        print("[Warehouse] Cannot open DuckDB:", e)
        print("[Warehouse] Skipping DuckDB load. CSVs remain the source of truth.")
        return
    for name, path in TABLES.items():
        if not path.exists():
            print(f"  skip {name} (missing)")
            continue
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(
            f"CREATE TABLE {name} AS SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE)"
        )
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  loaded {name}: {cnt:,}")
    con.close()
    print(f"[Warehouse] DuckDB at {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
