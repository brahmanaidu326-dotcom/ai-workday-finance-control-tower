"""Phase 6: Reconciliation engine.

Compares hops:
  Source -> Prism
  Prism  -> Journal
  Journal -> GL

Writes data/processed/reconciliation_results.csv with one row per source
transaction (and orphan journals) summarizing variance, status, and flags.
"""
from __future__ import annotations
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import RAW_DIR, PROCESSED_DIR

TOLERANCE = 0.50  # USD tolerance per hop


def load() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    src = pd.read_csv(RAW_DIR / "source_transactions.csv")
    prism = pd.read_csv(PROCESSED_DIR / "prism_transformed_transactions.csv")
    journals = pd.read_csv(PROCESSED_DIR / "journal_output.csv")
    gl = pd.read_csv(PROCESSED_DIR / "gl_postings.csv")
    cal = pd.read_csv(RAW_DIR / "close_calendar.csv")
    return src, prism, journals, gl, cal


def reconcile(src, prism, journals, gl, cal) -> pd.DataFrame:
    # Aggregate GL DR side as the "posted" amount (both sides should tie)
    gl_posted = (gl[gl["dr_cr"] == "DR"]
                 .groupby("source_txn_id", dropna=False)["amount_usd"].sum()
                 .rename("gl_amount_usd").reset_index())

    jnl_posted = (journals[journals["status"] == "DRAFT"]
                  .loc[journals["dr_cr"] == "DR"]
                  .groupby("source_txn_id")["amount_usd"].sum()
                  .rename("journal_amount_usd").reset_index())

    src_abs = src.copy()
    src_abs["source_amount_usd"] = src_abs["amount"].abs()  # simplified, FX applied in Prism

    prism_amt = prism[["source_txn_id", "amount_usd", "prism_status",
                       "worktag_valid", "fx_drift_injected",
                       "amount_mismatch_injected"]].copy()
    prism_amt["prism_amount_usd"] = prism_amt["amount_usd"].abs()

    recon = (src_abs[["source_txn_id", "source_system", "event_type",
                      "transaction_date", "entity", "cost_center", "source_amount_usd"]]
             .merge(prism_amt[["source_txn_id", "prism_amount_usd", "prism_status",
                               "worktag_valid", "fx_drift_injected", "amount_mismatch_injected"]],
                    on="source_txn_id", how="left")
             .merge(jnl_posted, on="source_txn_id", how="left")
             .merge(gl_posted, on="source_txn_id", how="left"))

    # Variance calcs
    recon["var_src_prism"] = (recon["prism_amount_usd"].fillna(0) -
                              recon["source_amount_usd"].fillna(0)).round(2)
    recon["var_prism_jnl"] = (recon["journal_amount_usd"].fillna(0) -
                              recon["prism_amount_usd"].fillna(0)).round(2)
    recon["var_jnl_gl"]   = (recon["gl_amount_usd"].fillna(0) -
                             recon["journal_amount_usd"].fillna(0)).round(2)

    recon["hop_src_prism_ok"] = recon["var_src_prism"].abs() <= TOLERANCE
    recon["hop_prism_jnl_ok"] = recon["var_prism_jnl"].abs() <= TOLERANCE
    recon["hop_jnl_gl_ok"]    = recon["var_jnl_gl"].abs() <= TOLERANCE

    recon["fully_reconciled"] = (recon["hop_src_prism_ok"] &
                                 recon["hop_prism_jnl_ok"] &
                                 recon["hop_jnl_gl_ok"] &
                                 recon["gl_amount_usd"].notna())

    recon["recon_status"] = np.where(
        recon["fully_reconciled"], "RECONCILED",
        np.where(recon["gl_amount_usd"].isna(), "UNPOSTED", "VARIANCE"))

    # Append orphan journal rows (GL with no source)
    orphans = gl[gl["source_txn_id"].isna()].groupby(
        ["journal_id", "account", "entity", "cost_center", "posting_period"],
        dropna=False, as_index=False)["amount_usd"].sum()
    orphans = orphans.rename(columns={"amount_usd": "gl_amount_usd"})
    orphans["source_txn_id"] = orphans["journal_id"].apply(lambda x: f"ORPHAN-{x}")
    orphans["source_system"] = "GL_MANUAL"
    orphans["event_type"] = "MANUAL_JOURNAL"
    orphans["recon_status"] = "ORPHAN"
    orphans["transaction_date"] = None
    for c in ["source_amount_usd", "prism_amount_usd", "journal_amount_usd",
              "var_src_prism", "var_prism_jnl", "var_jnl_gl"]:
        orphans[c] = np.nan
    for c in ["hop_src_prism_ok", "hop_prism_jnl_ok", "hop_jnl_gl_ok",
              "fully_reconciled", "worktag_valid",
              "fx_drift_injected", "amount_mismatch_injected"]:
        orphans[c] = False
    orphans["prism_status"] = "N/A"

    cols = ["source_txn_id", "source_system", "event_type", "transaction_date",
            "entity", "cost_center", "source_amount_usd", "prism_amount_usd",
            "journal_amount_usd", "gl_amount_usd",
            "var_src_prism", "var_prism_jnl", "var_jnl_gl",
            "hop_src_prism_ok", "hop_prism_jnl_ok", "hop_jnl_gl_ok",
            "fully_reconciled", "prism_status", "worktag_valid",
            "fx_drift_injected", "amount_mismatch_injected", "recon_status"]
    final = pd.concat([recon[cols], orphans.reindex(columns=cols)],
                      ignore_index=True)
    return final


def main() -> None:
    src, prism, journals, gl, cal = load()
    recon = reconcile(src, prism, journals, gl, cal)
    recon.to_csv(PROCESSED_DIR / "reconciliation_results.csv", index=False)
    print(f"[Phase 6] Recon rows: {len(recon):,}  status counts: "
          f"{recon['recon_status'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
