"""AI-Powered Workday Finance Control Tower - Operator Console."""
from __future__ import annotations
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import PROCESSED_DIR, OUTPUTS_DIR, RAW_DIR

st.set_page_config(page_title="Finance Control Tower", layout="wide")


@st.cache_data
def load():
    recon = pd.read_csv(PROCESSED_DIR / "reconciliation_results.csv")
    exc = pd.read_csv(OUTPUTS_DIR / "exceptions" / "exceptions_with_ai.csv")
    gl = pd.read_csv(PROCESSED_DIR / "gl_postings.csv")
    rules = pd.read_csv(RAW_DIR / "accounting_center_rules.csv")
    cal = pd.read_csv(RAW_DIR / "close_calendar.csv")
    return recon, exc, gl, rules, cal


recon, exc, gl, rules, cal = load()

st.sidebar.title("Finance Control Tower")
st.sidebar.caption("Workday-style close operations")
page = st.sidebar.radio(
    "View",
    ["Executive Close Overview",
     "Source-to-GL Traceability",
     "Accounting Center Rule Monitor",
     "Exception Root-Cause",
     "Close Risk"],
)

periods = sorted(recon["transaction_date"].dropna().astype(str).str[:7].unique().tolist())
entities = sorted(recon["entity"].dropna().unique().tolist())
period_sel = st.sidebar.multiselect("Period", periods, default=periods)
entity_sel = st.sidebar.multiselect("Entity", entities, default=entities)


def filter_recon(df):
    return df[df["transaction_date"].astype(str).str[:7].isin(period_sel) & df["entity"].isin(entity_sel)]


def filter_exc(df):
    return df[df["transaction_date"].astype(str).str[:7].isin(period_sel) & df["entity"].isin(entity_sel)]


if page == "Executive Close Overview":
    st.title("Executive Close Overview")
    r = filter_recon(recon)
    e = filter_exc(exc)

    c1, c2, c3, c4, c5 = st.columns(5)
    rate = (r["recon_status"] == "RECONCILED").mean() if len(r) else 0
    c1.metric("Reconciliation Rate", f"{rate:.1%}")
    var_usd = (r["gl_amount_usd"].sum() - r["source_amount_usd"].sum())
    c2.metric("Total Variance (USD)", f"${var_usd:,.0f}")
    c3.metric("Exceptions", f"{len(e):,}")
    high = (e["severity"] == "High").sum() if len(e) else 0
    c4.metric("High Severity", f"{high:,}")
    risk = ((e["severity"] == "High").sum() * 3 +
            (e["exception_type"] == "LATE_POSTING").sum() * 2 +
            (e["exception_type"] == "ORPHAN_JOURNAL").sum() * 2 +
            (e["severity"] != "High").sum()) / max(len(r), 1) * 100
    c5.metric("Close Risk Index", f"{risk:.1f}")

    st.subheader("Exception mix by type")
    st.bar_chart(e["exception_type"].value_counts())

    st.subheader("Variance by source system")
    by_src = r.groupby("source_system")[["source_amount_usd", "gl_amount_usd"]].sum()
    by_src["variance"] = by_src["gl_amount_usd"] - by_src["source_amount_usd"]
    st.dataframe(by_src.style.format("${:,.0f}"), use_container_width=True)

    st.subheader("Top 10 exceptions")
    top = e.sort_values("abs_variance_usd", ascending=False).head(10)
    st.dataframe(top[["source_txn_id", "exception_type", "severity", "abs_variance_usd", "ai_explanation"]], use_container_width=True)

elif page == "Source-to-GL Traceability":
    st.title("Source-to-GL Traceability")
    txn = st.text_input("Source txn id", value=str(recon["source_txn_id"].iloc[0]))
    row = recon[recon["source_txn_id"] == txn]
    if row.empty:
        st.warning("No match.")
    else:
        r = row.iloc[0]
        st.markdown(f"**Source system:** {r['source_system']}  |  **Entity:** {r['entity']}  |  **Cost Center:** {r['cost_center']}  |  **Status:** `{r['recon_status']}`")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Source USD", f"${r['source_amount_usd']:,.2f}" if pd.notna(r['source_amount_usd']) else "-")
        c2.metric("Prism USD",  f"${r['prism_amount_usd']:,.2f}"  if pd.notna(r['prism_amount_usd']) else "-")
        c3.metric("Journal USD", f"${r['journal_amount_usd']:,.2f}" if pd.notna(r['journal_amount_usd']) else "-")
        c4.metric("GL USD",     f"${r['gl_amount_usd']:,.2f}"     if pd.notna(r['gl_amount_usd']) else "-")
        st.write("**Hop variances:**")
        st.write({
            "Source to Prism": r["var_src_prism"],
            "Prism to Journal": r["var_prism_jnl"],
            "Journal to GL": r["var_jnl_gl"],
        })
        ex = exc[exc["source_txn_id"] == txn]
        if not ex.empty:
            ex = ex.iloc[0]
            st.error(f"**Exception: {ex['exception_type']}** ({ex['severity']})")
            st.write("**AI explanation:**", ex["ai_explanation"])
            st.write("**Root cause:**", ex["ai_root_cause"])
            st.write("**Recommended action:**", ex["ai_recommended_action"])

elif page == "Accounting Center Rule Monitor":
    st.title("Accounting Center Rule Monitor")
    j = pd.read_csv(PROCESSED_DIR / "journal_output.csv")
    st.subheader("Rule catalog")
    st.dataframe(rules, use_container_width=True)
    st.subheader("Journal status by rule")
    by_rule = j.groupby(["rule_id", "status"]).size().unstack(fill_value=0)
    st.dataframe(by_rule, use_container_width=True)
    st.bar_chart(by_rule)

elif page == "Exception Root-Cause":
    st.title("Exception Root-Cause")
    e = filter_exc(exc)
    types = sorted(e["exception_type"].dropna().unique().tolist())
    sel_type = st.multiselect("Exception type", types, default=types)
    sev = st.multiselect("Severity", ["High", "Medium", "Low"], default=["High", "Medium", "Low"])
    e = e[e["exception_type"].isin(sel_type) & e["severity"].isin(sev)]
    st.caption(f"{len(e):,} exceptions")

    if len(e) == 0:
        st.info("No exceptions match filters.")
    else:
        sel = st.selectbox("Select an exception", e["source_txn_id"].tolist()[:500])
        row = e[e["source_txn_id"] == sel].iloc[0]
        c1, c2 = st.columns([2, 3])
        with c1:
            st.write(f"**Type:** {row['exception_type']}")
            st.write(f"**Severity:** {row['severity']}")
            st.write(f"**Owner:** {row['owner']}")
            st.write(f"**Variance USD:** ${row['abs_variance_usd']:,.2f}")
            st.write(f"**Entity:** {row['entity']}")
            st.write(f"**Source system:** {row['source_system']}")
        with c2:
            st.markdown("### AI-generated analysis")
            st.info(row["ai_explanation"])
            st.warning(f"**Root cause:** {row['ai_root_cause']}")
            st.success(f"**Recommended action:** {row['ai_recommended_action']}")
        st.subheader("All exceptions (filtered)")
        st.dataframe(e[["source_txn_id", "exception_type", "severity", "owner", "abs_variance_usd", "ai_explanation"]].head(500), use_container_width=True)

elif page == "Close Risk":
    st.title("Close Risk")
    e = filter_exc(exc)
    r = filter_recon(recon)
    st.subheader("Reconciliation funnel")
    funnel = {
        "Source txns": len(r),
        "Prism-valid": (r["prism_status"] == "VALID").sum(),
        "Journal-drafted": r["journal_amount_usd"].notna().sum(),
        "GL-posted": r["gl_amount_usd"].notna().sum(),
        "Reconciled": (r["recon_status"] == "RECONCILED").sum(),
    }
    st.bar_chart(pd.Series(funnel))
    st.subheader("Exceptions by owner")
    st.bar_chart(e["owner"].value_counts())
    st.subheader("Close calendar")
    st.dataframe(cal, use_container_width=True)
