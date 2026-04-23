"""Phase 3: Prism-style transformation layer.

Reads data/raw/source_transactions.csv,
performs FX normalization, worktag validation, currency conversion,
and writes data/processed/prism_transformed_transactions.csv.
"""
from __future__ import annotations
from pathlib import Path
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import RAW_DIR, PROCESSED_DIR, ERROR_RATES, RANDOM_SEED

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

FX_RATES = {"USD": 1.0, "EUR": 1.08, "GBP": 1.26, "INR": 0.012}


def load_raw() -> tuple[pd.DataFrame, pd.DataFrame]:
    src = pd.read_csv(RAW_DIR / "source_transactions.csv")
    wt = pd.read_csv(RAW_DIR / "worktag_master.csv")
    return src, wt


def transform(src: pd.DataFrame, wt: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(RANDOM_SEED)
    df = src.copy()

    # --- Worktag validation ---
    active_wts = set(wt.loc[wt["active"], "cost_center"].tolist())
    df["worktag_valid"] = df["cost_center"].apply(
        lambda x: pd.notna(x) and x in active_wts
    )

    # --- FX normalization to USD ---
    df["fx_rate"] = df["currency"].map(FX_RATES).fillna(1.0)
    df["amount_usd"] = (df["amount"] * df["fx_rate"]).round(2)

    # Inject small FX drift in a small fraction of rows (rate table mismatch)
    drift_mask = rng.random(len(df)) < ERROR_RATES["fx_drift"]
    df.loc[drift_mask, "amount_usd"] = (df.loc[drift_mask, "amount_usd"] *
                                        rng.uniform(1.01, 1.03, drift_mask.sum())).round(2)
    df["fx_drift_injected"] = drift_mask

    # Inject amount mismatch noise (transformation bug simulation)
    mismatch_mask = rng.random(len(df)) < ERROR_RATES["amount_mismatch"]
    df.loc[mismatch_mask, "amount_usd"] = (df.loc[mismatch_mask, "amount_usd"] +
                                           rng.uniform(-25, 25, mismatch_mask.sum())).round(2)
    df["amount_mismatch_injected"] = mismatch_mask

    # --- Canonical fields ---
    df["prism_txn_id"] = df["source_txn_id"].str.replace("SRC-", "PRS-", regex=False)
    df["posting_period"] = pd.to_datetime(df["transaction_date"]).dt.strftime("%Y-%m")
    df["transformed_ts"] = pd.Timestamp.utcnow().isoformat()

    # Derived row-level status
    df["prism_status"] = np.where(df["worktag_valid"], "VALID", "BLOCKED")

    cols = ["prism_txn_id", "source_txn_id", "source_system", "event_type",
            "transaction_date", "posting_period", "entity", "cost_center",
            "worktag_valid", "vendor_customer", "description",
            "amount", "currency", "fx_rate", "amount_usd",
            "prism_status", "fx_drift_injected", "amount_mismatch_injected",
            "transformed_ts"]
    return df[cols]


def main() -> None:
    src, wt = load_raw()
    out = transform(src, wt)
    out.to_csv(PROCESSED_DIR / "prism_transformed_transactions.csv", index=False)
    print(f"[Phase 3] Prism rows: {len(out):,}  valid: {out['worktag_valid'].sum():,}  blocked: {(~out['worktag_valid']).sum():,}")


if __name__ == "__main__":
    main()
