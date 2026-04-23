"""Phase 4: Accounting Center rule simulation.

Reads prism transformed transactions and accounting center rules,
generates a two-line (DR/CR) journal for each valid posting event,
and writes data/processed/journal_output.csv.
"""
from __future__ import annotations
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import RAW_DIR, PROCESSED_DIR


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    prism = pd.read_csv(PROCESSED_DIR / "prism_transformed_transactions.csv")
    rules = pd.read_csv(RAW_DIR / "accounting_center_rules.csv")
    return prism, rules


def apply_rules(prism: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    active_rules = rules[rules["active"]]
    rule_map = active_rules.set_index(["source_system", "event_type"]).to_dict("index")

    journals: list[dict] = []
    seq = 0

    for _, r in prism.iterrows():
        key = (r["source_system"], r["event_type"])
        rule = rule_map.get(key)

        # Block anything without a worktag OR without an active rule
        if not r["worktag_valid"] or rule is None:
            # Still track the attempt for downstream exception detection
            seq += 1
            journals.append({
                "journal_id": f"JNL-{seq:07d}",
                "journal_line": 1,
                "prism_txn_id": r["prism_txn_id"],
                "source_txn_id": r["source_txn_id"],
                "rule_id": rule["rule_id"] if rule else None,
                "account": None,
                "dr_cr": None,
                "amount_usd": r["amount_usd"],
                "entity": r["entity"],
                "cost_center": r["cost_center"],
                "posting_period": r["posting_period"],
                "status": "BLOCKED",
                "block_reason": "NO_RULE" if rule is None else "INVALID_WORKTAG",
            })
            continue

        seq += 1
        jid = f"JNL-{seq:07d}"
        amt = abs(r["amount_usd"])

        # DR line
        journals.append({
            "journal_id": jid,
            "journal_line": 1,
            "prism_txn_id": r["prism_txn_id"],
            "source_txn_id": r["source_txn_id"],
            "rule_id": rule["rule_id"],
            "account": str(rule["dr_account"]),
            "dr_cr": "DR",
            "amount_usd": amt,
            "entity": r["entity"],
            "cost_center": r["cost_center"],
            "posting_period": r["posting_period"],
            "status": "DRAFT",
            "block_reason": None,
        })
        # CR line
        journals.append({
            "journal_id": jid,
            "journal_line": 2,
            "prism_txn_id": r["prism_txn_id"],
            "source_txn_id": r["source_txn_id"],
            "rule_id": rule["rule_id"],
            "account": str(rule["cr_account"]),
            "dr_cr": "CR",
            "amount_usd": amt,
            "entity": r["entity"],
            "cost_center": r["cost_center"],
            "posting_period": r["posting_period"],
            "status": "DRAFT",
            "block_reason": None,
        })

    return pd.DataFrame(journals)


def main() -> None:
    prism, rules = load_inputs()
    journals = apply_rules(prism, rules)
    journals.to_csv(PROCESSED_DIR / "journal_output.csv", index=False)
    stats = journals["status"].value_counts().to_dict()
    print(f"[Phase 4] Journal lines: {len(journals):,}  status: {stats}")


if __name__ == "__main__":
    main()
