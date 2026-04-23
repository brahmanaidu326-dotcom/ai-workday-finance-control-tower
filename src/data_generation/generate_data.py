"""Phase 2: Generate synthetic enterprise finance data.

Produces:
- data/raw/source_transactions.csv
- data/raw/accounting_center_rules.csv
- data/raw/exception_lookup.csv
- data/raw/close_calendar.csv
- data/raw/worktag_master.csv
"""
from __future__ import annotations
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import (
    RANDOM_SEED, SIMULATION_START_DATE, SIMULATION_END_DATE,
    NUM_SOURCE_TRANSACTIONS, ERROR_RATES, SOURCE_SYSTEMS,
    CHART_OF_ACCOUNTS, ENTITIES, COST_CENTERS, CURRENCIES, RAW_DIR,
)

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
fake = Faker()
Faker.seed(RANDOM_SEED)

RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------- helpers ----------
def rand_date(start: str, end: str) -> datetime:
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    return s + timedelta(days=random.randint(0, (e - s).days))

def maybe(rate: float) -> bool:
    return random.random() < rate


# ---------- 1. worktag_master ----------
def build_worktag_master() -> pd.DataFrame:
    rows = []
    for cc in COST_CENTERS:
        rows.append({
            "worktag_id": f"WT-{cc}",
            "cost_center": cc,
            "department": f"Dept-{cc[-3:]}",
            "active": True,
        })
    # A couple of inactive worktags to cause validation failures
    rows.append({"worktag_id": "WT-CC-900", "cost_center": "CC-900", "department": "Dept-OLD", "active": False})
    return pd.DataFrame(rows)


# ---------- 2. source_transactions ----------
def build_source_transactions(n: int = NUM_SOURCE_TRANSACTIONS) -> pd.DataFrame:
    rows = []
    for i in range(n):
        system = random.choices(SOURCE_SYSTEMS, weights=[0.45, 0.25, 0.20, 0.10])[0]
        txn_date = rand_date(SIMULATION_START_DATE, SIMULATION_END_DATE)
        amt = round(np.random.gamma(2.0, 500.0) + 50, 2)
        sign = 1 if system in {"AR"} else -1 if system in {"AP", "EXPENSE", "PAYROLL"} else 1
        currency = random.choices(CURRENCIES, weights=[0.75, 0.12, 0.08, 0.05])[0]
        entity = random.choice(ENTITIES)
        cc = random.choice(COST_CENTERS)

        # Inject errors
        if maybe(ERROR_RATES["invalid_cost_center"]):
            cc = "CC-900"  # inactive worktag
        if maybe(ERROR_RATES["missing_worktag"]):
            cc = None

        rows.append({
            "source_txn_id": f"SRC-{system}-{i:06d}",
            "source_system": system,
            "transaction_date": txn_date.date().isoformat(),
            "vendor_customer": fake.company(),
            "description": fake.catch_phrase()[:60],
            "amount": round(amt * sign, 2),
            "currency": currency,
            "entity": entity,
            "cost_center": cc,
            "event_type": {
                "AP": "INVOICE_RECEIVED",
                "AR": "INVOICE_ISSUED",
                "EXPENSE": "EXPENSE_REPORT",
                "PAYROLL": "PAYROLL_RUN",
            }[system],
            "created_ts": (txn_date + timedelta(hours=random.randint(1, 48))).isoformat(),
        })

    df = pd.DataFrame(rows)

    # Duplicate injection
    n_dup = int(len(df) * ERROR_RATES["duplicate_txn"])
    dups = df.sample(n=n_dup, random_state=RANDOM_SEED).copy()
    dups["source_txn_id"] = dups["source_txn_id"].apply(lambda x: x + "-DUP")
    df = pd.concat([df, dups], ignore_index=True)

    return df


# ---------- 3. accounting_center_rules ----------
def build_accounting_center_rules() -> pd.DataFrame:
    """Maps (source_system, event_type) -> journal lines."""
    rules = [
        # AP: DR Expense, CR Accounts Payable
        {"rule_id": "R-AP-01", "source_system": "AP", "event_type": "INVOICE_RECEIVED",
         "dr_account": "6000", "cr_account": "2000", "description": "AP invoice accrual",
         "active": True},
        # AR: DR Accounts Receivable, CR Revenue
        {"rule_id": "R-AR-01", "source_system": "AR", "event_type": "INVOICE_ISSUED",
         "dr_account": "1200", "cr_account": "4000", "description": "AR invoice",
         "active": True},
        # EXPENSE: DR T&E, CR Cash
        {"rule_id": "R-EXP-01", "source_system": "EXPENSE", "event_type": "EXPENSE_REPORT",
         "dr_account": "6200", "cr_account": "1000", "description": "Expense report payout",
         "active": True},
        # PAYROLL: DR Payroll, CR Cash
        {"rule_id": "R-PR-01", "source_system": "PAYROLL", "event_type": "PAYROLL_RUN",
         "dr_account": "6100", "cr_account": "1000", "description": "Payroll run",
         "active": True},
        # Intentionally inactive rule (creates missing-mapping exceptions for a niche event)
        {"rule_id": "R-AP-02", "source_system": "AP", "event_type": "INVOICE_CREDIT_MEMO",
         "dr_account": "2000", "cr_account": "6000", "description": "Credit memo",
         "active": False},
    ]
    return pd.DataFrame(rules)


# ---------- 4. exception_lookup ----------
def build_exception_lookup() -> pd.DataFrame:
    taxonomy = [
        ("EXC-001", "MISSING_WORKTAG", "High",    "Source txn has no cost center / worktag assigned",
         "Finance Ops", "Assign correct worktag in source system and reprocess"),
        ("EXC-002", "INVALID_COST_CENTER", "High","Cost center is inactive or unknown in master data",
         "Master Data", "Update cost center mapping or close/replace inactive cost center"),
        ("EXC-003", "DUPLICATE_TRANSACTION", "Medium","Same transaction posted more than once",
         "Finance Ops", "Reverse duplicate journal and notify source system owner"),
        ("EXC-004", "LATE_POSTING", "Medium",    "Journal posted after close cutoff",
         "Controller", "Post in next period or request late-post approval"),
        ("EXC-005", "AMOUNT_MISMATCH", "High",   "Source amount differs from GL posted amount",
         "Finance Systems", "Investigate transformation rounding or currency conversion"),
        ("EXC-006", "ORPHAN_JOURNAL", "High",    "GL entry has no matching source transaction",
         "Finance Systems", "Identify manual journal source and attach documentation"),
        ("EXC-007", "FX_DRIFT", "Low",           "FX conversion variance beyond tolerance",
         "Treasury", "Review FX rate table and re-run translation"),
        ("EXC-008", "MISSING_RULE", "High",      "No active Accounting Center rule for event type",
         "Accounting", "Activate or create accounting rule for the event"),
    ]
    return pd.DataFrame(taxonomy, columns=[
        "exception_code", "exception_type", "severity", "description", "owner", "recommended_action"
    ])


# ---------- 5. close_calendar ----------
def build_close_calendar() -> pd.DataFrame:
    periods = [("2026-01", "2026-01-01", "2026-01-31", "2026-02-05"),
               ("2026-02", "2026-02-01", "2026-02-28", "2026-03-05"),
               ("2026-03", "2026-03-01", "2026-03-31", "2026-04-05")]
    rows = []
    for p, start, end, close in periods:
        rows.append({
            "period": p,
            "period_start": start,
            "period_end": end,
            "close_cutoff": close,
            "status": "OPEN" if p == "2026-03" else "CLOSED",
        })
    return pd.DataFrame(rows)


def main() -> None:
    src = build_source_transactions()
    src.to_csv(RAW_DIR / "source_transactions.csv", index=False)

    build_worktag_master().to_csv(RAW_DIR / "worktag_master.csv", index=False)
    build_accounting_center_rules().to_csv(RAW_DIR / "accounting_center_rules.csv", index=False)
    build_exception_lookup().to_csv(RAW_DIR / "exception_lookup.csv", index=False)
    build_close_calendar().to_csv(RAW_DIR / "close_calendar.csv", index=False)

    # Chart of accounts for reference
    pd.DataFrame(
        [{"account": a, "account_name": n} for a, n in CHART_OF_ACCOUNTS.items()]
    ).to_csv(RAW_DIR / "chart_of_accounts.csv", index=False)

    print(f"[Phase 2] Source transactions: {len(src):,}")
    print(f"[Phase 2] Files written to: {RAW_DIR}")


if __name__ == "__main__":
    main()
