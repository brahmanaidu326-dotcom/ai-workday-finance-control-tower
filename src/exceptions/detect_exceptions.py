"""Phase 7: Exception detection & rule-based root cause.

Reads reconciliation_results.csv and classifies each non-reconciled row
into one of the taxonomy codes from exception_lookup.csv.

Writes outputs/exceptions/exceptions.csv.
"""
from __future__ import annotations
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import RAW_DIR, PROCESSED_DIR, OUTPUTS_DIR

EXC_DIR = OUTPUTS_DIR / "exceptions"
EXC_DIR.mkdir(parents=True, exist_ok=True)


def classify(row, gl_lookup) -> str:
    if row["recon_status"] == "ORPHAN":
        return "ORPHAN_JOURNAL"
    if row["recon_status"] == "UNPOSTED":
        if not row.get("worktag_valid", False):
            if pd.isna(row.get("cost_center")):
                return "MISSING_WORKTAG"
            return "INVALID_COST_CENTER"
        return "MISSING_RULE"
    # VARIANCE path
    if row.get("amount_mismatch_injected"):
        return "AMOUNT_MISMATCH"
    if row.get("fx_drift_injected"):
        return "FX_DRIFT"
    if str(row["source_txn_id"]).endswith("-DUP"):
        return "DUPLICATE_TRANSACTION"
    # Late posting lookup
    if row["source_txn_id"] in gl_lookup and gl_lookup[row["source_txn_id"]]:
        return "LATE_POSTING"
    # Fallback
    return "AMOUNT_MISMATCH"


def main() -> None:
    recon = pd.read_csv(PROCESSED_DIR / "reconciliation_results.csv")
    lookup = pd.read_csv(RAW_DIR / "exception_lookup.csv")
    gl = pd.read_csv(PROCESSED_DIR / "gl_postings.csv")

    late_lookup = (gl.groupby("source_txn_id")["late_posting"].max()
                   .fillna(False).astype(bool).to_dict())

    problems = recon[recon["recon_status"] != "RECONCILED"].copy()
    problems["exception_type"] = problems.apply(
        lambda r: classify(r, late_lookup), axis=1)

    # Merge taxonomy details
    tax = lookup.drop(columns=["exception_code"]).drop_duplicates("exception_type")
    enriched = problems.merge(tax, on="exception_type", how="left")

    # Add total variance for ranking
    enriched["abs_variance_usd"] = (
        enriched[["var_src_prism", "var_prism_jnl", "var_jnl_gl"]]
        .abs().max(axis=1).fillna(0)
    )

    enriched.to_csv(EXC_DIR / "exceptions.csv", index=False)
    print(f"[Phase 7] Exceptions: {len(enriched):,}  "
          f"by type: {enriched['exception_type'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
