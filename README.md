<img width="1920" height="1128" alt="Screenshot 2026-04-22 222826" src="https://github.com/user-attachments/assets/d8c41550-5f5b-484a-aa75-0aca3a56f048" />
<img width="1920" height="1128" alt="Screenshot 2026-04-22 222812" src="https://github.com/user-attachments/assets/e0708f2f-eb8f-473a-b5c2-b4a7df284f5b" />
# AI-Powered Workday Finance Control Tower

An enterprise-grade simulation of a Workday-style finance operations control tower that traces every transaction through:

**Source System → Prism Transformation → Accounting Center Rules → GL Posting → Reconciliation → AI Root-Cause Explanation**

Built to mirror how real finance systems teams identify why numbers don't tie during close, classify exceptions, and drive remediation.

## Business problem
Finance teams often cannot explain why balances don't match across source systems, reporting data, journals, and the GL. This causes close delays, manual journal work, audit risk, and low trust in the numbers. This project demonstrates an end-to-end lineage and exception-intelligence layer that attacks all four.

## Architecture

```
 Source Systems (AP, AR, Expense, Payroll)
            |
            v
 [ Prism-style Transformation Layer ]
   - Worktag validation
   - FX normalization to USD
   - Canonical schema
            |
            v
 [ Accounting Center Rule Engine ]
   - Maps (source_system, event_type) -> DR/CR journal lines
            |
            v
 [ GL Posting Simulator ]
   - Realistic delays, late postings, orphan journals
            |
            v
 [ Reconciliation Engine ]
   - Hop-by-hop variance: Source vs Prism vs Journal vs GL
            |
            v
 [ Exception Detection + AI Root-Cause ]
   - Taxonomy classification + Claude explanations
            |
            v
 Power BI Control Tower   |   Streamlit Operator Console
```

## Stack
- Python 3.10+
- pandas, numpy, DuckDB
- Streamlit (operator console)
- Power BI (executive control tower)
- Anthropic Claude (root-cause explanations; template fallback included)

## Project layout

```
ai-workday-finance-control-tower/
  config/settings.py          Global simulation parameters
  src/
    data_generation/          Phase 2 - synthetic CSVs
    transformation/           Phase 3 - Prism layer
    accounting_center/        Phase 4 - rule engine
    gl_posting/               Phase 5 - GL simulator
    reconciliation/           Phase 6 - recon engine
    exceptions/               Phase 7 - classification
    ai_explanation/           Phase 8 - Claude + templates
    warehouse/                DuckDB loader
  app/streamlit_app.py        Phase 10 - Operator console
  powerbi/dashboard_spec.md   Phase 9 - 5-page BI spec with DAX
  docs/                       Architecture, case study, portfolio collateral
  run_pipeline.py             End-to-end orchestrator
  data/                       raw / processed / warehouse
  outputs/                    reconciliation / exceptions / reports
```

## Quick start

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt

# Run the full pipeline
python run_pipeline.py

# Launch the operator console
streamlit run app/streamlit_app.py
```

Set `ANTHROPIC_API_KEY` to enable live Claude-generated explanations; otherwise the deterministic template engine is used (same schema, same UX).

## What gets produced (typical run, 5,000 source txns)

| Artifact | Rows |
|---|---|
| `data/raw/source_transactions.csv` | ~5,050 (with duplicates) |
| `data/processed/prism_transformed_transactions.csv` | 5,050 |
| `data/processed/journal_output.csv` | ~9,800 (2 lines per valid txn) |
| `data/processed/gl_postings.csv` | ~9,700 + orphans |
| `data/processed/reconciliation_results.csv` | ~5,150 |
| `outputs/exceptions/exceptions.csv` | ~1,700 |
| `outputs/exceptions/exceptions_with_ai.csv` | ~1,700 with explanation + action |

Exception taxonomy: MISSING_WORKTAG, INVALID_COST_CENTER, DUPLICATE_TRANSACTION, LATE_POSTING, AMOUNT_MISMATCH, FX_DRIFT, ORPHAN_JOURNAL, MISSING_RULE.

## Power BI

Connect Power BI Desktop to `data/warehouse/control_tower.duckdb` (via DuckDB ODBC) or to the CSVs in `data/processed/` and `outputs/exceptions/`. Follow `powerbi/dashboard_spec.md` for the star schema, DAX measures, and 5-page layout.

## License
MIT (simulated data only; no proprietary Workday content).
