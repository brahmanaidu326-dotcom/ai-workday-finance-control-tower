"""Global project settings for the AI-Powered Workday Finance Control Tower."""
from pathlib import Path

# --- Project identity ---
PROJECT_NAME = "AI-Powered Workday Finance Control Tower"
PROJECT_VERSION = "0.1.0"

# --- Paths ---
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
OUTPUTS_DIR = ROOT_DIR / "outputs"

DUCKDB_PATH = WAREHOUSE_DIR / "control_tower.duckdb"

# --- Simulation parameters (used in later phases) ---
RANDOM_SEED = 42
SIMULATION_START_DATE = "2026-01-01"
SIMULATION_END_DATE = "2026-03-31"
NUM_SOURCE_TRANSACTIONS = 5000

# Intentional data-quality injection rates (tuned for realistic mess)
ERROR_RATES = {
    "missing_worktag": 0.03,
    "invalid_cost_center": 0.02,
    "duplicate_txn": 0.01,
    "late_posting": 0.04,
    "amount_mismatch": 0.015,
    "orphan_journal": 0.01,
    "fx_drift": 0.02,
}

# --- Source systems simulated ---
SOURCE_SYSTEMS = ["AP", "AR", "EXPENSE", "PAYROLL"]

# --- Chart of Accounts (minimal, realistic) ---
CHART_OF_ACCOUNTS = {
    "6000": "Operating Expense",
    "6100": "Payroll Expense",
    "6200": "Travel & Expense",
    "2000": "Accounts Payable",
    "1200": "Accounts Receivable",
    "4000": "Revenue",
    "1000": "Cash",
}

# --- Entities / Cost Centers (sample) ---
ENTITIES = ["US01", "US02", "EMEA01", "APAC01"]
COST_CENTERS = ["CC-100", "CC-200", "CC-300", "CC-400", "CC-500"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
