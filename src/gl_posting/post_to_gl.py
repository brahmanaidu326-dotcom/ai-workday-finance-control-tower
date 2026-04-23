"""Phase 5: GL posting simulation.

Posts DRAFT journal lines to the general ledger with realistic behavior:
- late postings (after close cutoff)
- a few orphan journal entries (manual GL without source txn)
- blocked journals remain unposted

Writes data/processed/gl_postings.csv.
"""
from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import random
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import PROCESSED_DIR, RAW_DIR, ERROR_RATES, RANDOM_SEED, ENTITIES

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    journals = pd.read_csv(PROCESSED_DIR / "journal_output.csv")
    cal = pd.read_csv(RAW_DIR / "close_calendar.csv")
    return journals, cal


def post(journals: pd.DataFrame, cal: pd.DataFrame) -> pd.DataFrame:
    cutoff = {row["period"]: row["close_cutoff"] for _, row in cal.iterrows()}
    rng = np.random.default_rng(RANDOM_SEED)

    rows = []
    gl_seq = 0
    for _, j in journals.iterrows():
        if j["status"] == "BLOCKED":
            continue

        # Default posting a few days after period end
        period = j["posting_period"]
        close_cut = datetime.fromisoformat(cutoff.get(period, "2026-04-05"))
        base = close_cut - timedelta(days=int(rng.integers(1, 5)))

        # Late posting injection
        late = rng.random() < ERROR_RATES["late_posting"]
        if late:
            base = close_cut + timedelta(days=int(rng.integers(1, 8)))

        gl_seq += 1
        rows.append({
            "gl_id": f"GL-{gl_seq:07d}",
            "journal_id": j["journal_id"],
            "journal_line": j["journal_line"],
            "source_txn_id": j["source_txn_id"],
            "prism_txn_id": j["prism_txn_id"],
            "account": j["account"],
            "dr_cr": j["dr_cr"],
            "amount_usd": j["amount_usd"],
            "entity": j["entity"],
            "cost_center": j["cost_center"],
            "posting_period": period,
            "posted_date": base.date().isoformat(),
            "late_posting": bool(late),
            "posting_status": "POSTED",
        })

    gl = pd.DataFrame(rows)

    # --- Orphan journals (GL entries with no source) ---
    n_orphans = int(len(gl) * ERROR_RATES["orphan_journal"])
    orphans = []
    for i in range(n_orphans):
        gl_seq += 1
        amt = float(round(rng.gamma(2.0, 400) + 50, 2))
        side = rng.choice(["DR", "CR"])
        orphans.append({
            "gl_id": f"GL-{gl_seq:07d}",
            "journal_id": f"JNL-MANUAL-{i:04d}",
            "journal_line": 1,
            "source_txn_id": None,
            "prism_txn_id": None,
            "account": str(rng.choice(["6000", "6100", "6200", "4000"])),
            "dr_cr": side,
            "amount_usd": amt,
            "entity": rng.choice(ENTITIES),
            "cost_center": rng.choice([f"CC-{x}00" for x in range(1, 6)]),
            "posting_period": rng.choice(["2026-01", "2026-02", "2026-03"]),
            "posted_date": "2026-03-20",
            "late_posting": False,
            "posting_status": "POSTED",
        })
    gl = pd.concat([gl, pd.DataFrame(orphans)], ignore_index=True)
    return gl


def main() -> None:
    journals, cal = load_inputs()
    gl = post(journals, cal)
    gl.to_csv(PROCESSED_DIR / "gl_postings.csv", index=False)
    print(f"[Phase 5] GL rows: {len(gl):,}  late: {gl['late_posting'].sum():,}  "
          f"orphans: {gl['source_txn_id'].isna().sum():,}")


if __name__ == "__main__":
    main()
