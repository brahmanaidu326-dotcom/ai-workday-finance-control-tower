"""Phase 8: AI explanation layer.

Generates plain-English explanation, likely root cause, and recommended next
action for each exception. Uses Claude API when ANTHROPIC_API_KEY is set;
otherwise falls back to a deterministic template engine.

Writes outputs/exceptions/exceptions_with_ai.csv.
"""
from __future__ import annotations
import math
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config.settings import OUTPUTS_DIR

EXC_DIR = OUTPUTS_DIR / "exceptions"
EXC_DIR.mkdir(parents=True, exist_ok=True)


def clean(v, default="N/A"):
    if v is None:
        return default
    if isinstance(v, float) and math.isnan(v):
        return default
    return v


def to_str(v, default="N/A"):
    return str(clean(v, default))


TEMPLATES = {
    "MISSING_WORKTAG": (
        "Transaction {txn} from {sys} for entity {ent} was received without a cost center or worktag, so Accounting Center could not post it.",
        "Source system submitted the record without the required worktag - often a missing default on the requester profile or a bypassed validation.",
        "Assign the correct cost center in the source system and re-run the Prism batch.",
    ),
    "INVALID_COST_CENTER": (
        "Transaction {txn} references cost center {cc}, which is inactive or missing from the worktag master for entity {ent}.",
        "Master data drift - the cost center was decommissioned or renamed but the source system was not updated.",
        "Remap to an active cost center or reactivate the master record, then reprocess via Prism.",
    ),
    "DUPLICATE_TRANSACTION": (
        "Transaction {txn} appears more than once across source, Prism, and GL.",
        "Likely a retried integration job or re-submission with no idempotency check.",
        "Reverse the duplicate journal, notify the source system owner, and enforce a dedup check on source_txn_id.",
    ),
    "LATE_POSTING": (
        "Transaction {txn} posted after the close cutoff for its period.",
        "Approval or integration delay pushed the posting past the close window - typically a workflow or SLA issue.",
        "If the period is still open, post with late-post approval; otherwise book to next period with an accrual.",
    ),
    "AMOUNT_MISMATCH": (
        "Source amount for {txn} differs from the GL-posted amount by {var}. Source={src} vs GL={gl}.",
        "Transformation rounding, currency translation, or a downstream manual adjustment on the journal line.",
        "Trace the Prism transformation log, validate FX rates, and confirm no manual edits on the journal.",
    ),
    "FX_DRIFT": (
        "Translation variance on {txn}: expected vs actual USD differ by {var}.",
        "FX rate table stale or misaligned between source capture date and the Prism run.",
        "Refresh the FX rate table and re-translate. Align source capture-date policy with Treasury.",
    ),
    "ORPHAN_JOURNAL": (
        "GL entry {txn} has no matching source transaction or Prism record.",
        "Manual journal entered directly in the GL without upstream lineage - either a legitimate adjustment or a bypass.",
        "Identify the preparer, attach documentation, and decide whether to reverse or approve as an adjustment.",
    ),
    "MISSING_RULE": (
        "No active Accounting Center rule exists for source={sys}, event_type={event}. Transaction {txn} is blocked.",
        "Rule was deactivated or never created for this event variant.",
        "Activate or author the accounting rule and rerun the Accounting Center for the affected period.",
    ),
}


def render_template(row):
    etype = to_str(row.get("exception_type"), "UNKNOWN")
    tpl = TEMPLATES.get(etype)

    ctx = {
        "txn": to_str(row.get("source_txn_id")),
        "sys": to_str(row.get("source_system")),
        "event": to_str(row.get("event_type")),
        "ent": to_str(row.get("entity")),
        "cc": to_str(row.get("cost_center")),
        "src": to_str(row.get("source_amount_usd"), "0"),
        "gl": to_str(row.get("gl_amount_usd"), "0"),
        "var": to_str(row.get("abs_variance_usd"), "0"),
    }

    if tpl is None:
        return {
            "ai_explanation": "Unclassified exception for " + ctx["txn"] + ".",
            "ai_root_cause": "Unknown - requires manual review.",
            "ai_recommended_action": "Route to Finance Systems for triage.",
            "ai_source": "template",
        }

    expl, root, act = tpl
    try:
        ex = expl.format(**ctx)
        rc = root.format(**ctx)
        ac = act.format(**ctx)
    except Exception as e:
        ex = "Exception " + etype + " for " + ctx["txn"]
        rc = "See taxonomy row for details"
        ac = "Route to Finance Systems"
    return {
        "ai_explanation": ex,
        "ai_root_cause": rc,
        "ai_recommended_action": ac,
        "ai_source": "template",
    }


def call_claude(row):
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        return None
    try:
        import anthropic
    except ImportError:
        return None
    client = anthropic.Anthropic(api_key=key)
    prompt = (
        "You are a senior Workday finance systems analyst. A close exception "
        "was detected. Return EXACTLY three labeled lines:\n"
        "EXPLANATION: <1 sentence>\n"
        "ROOT_CAUSE: <1 sentence>\n"
        "ACTION: <1 sentence>\n\n"
        "EXCEPTION: " + str(row)
    )
    try:
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        text = msg.content[0].text
        parts = {"EXPLANATION": "", "ROOT_CAUSE": "", "ACTION": ""}
        for line in text.splitlines():
            for k in parts:
                if line.startswith(k + ":"):
                    parts[k] = line.split(":", 1)[1].strip()
        return {
            "ai_explanation": parts["EXPLANATION"],
            "ai_root_cause": parts["ROOT_CAUSE"],
            "ai_recommended_action": parts["ACTION"],
            "ai_source": "claude",
        }
    except Exception as e:
        print("[Phase 8] Claude call failed:", e)
        return None


def main(limit=None):
    exc = pd.read_csv(EXC_DIR / "exceptions.csv")
    rows = exc.to_dict("records")
    if limit:
        rows = rows[:limit]

    use_claude = bool(os.getenv("ANTHROPIC_API_KEY"))
    print("[Phase 8] Mode:", "claude+template" if use_claude else "template-only")

    out = []
    for r in rows:
        ai = None
        etype = to_str(r.get("exception_type"), "")
        if use_claude and etype in ("AMOUNT_MISMATCH", "ORPHAN_JOURNAL"):
            ai = call_claude(r)
            time.sleep(0.2)
        if ai is None:
            ai = render_template(r)
        r.update(ai)
        out.append(r)

    df = pd.DataFrame(out)
    df.to_csv(EXC_DIR / "exceptions_with_ai.csv", index=False)
    print("[Phase 8] AI explanations written:", len(df))


if __name__ == "__main__":
    main()
