# impact_metrics_scraper.py
import os
import sys
import json
import importlib
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Date
from functions import get_logger
from dotenv import load_dotenv

load_dotenv()

# ========= CONFIG =========

# MariaDB connection via env vars
DB_USER = os.getenv("TDB_USER")
DB_PASSWORD = os.getenv("TDB_PASSWORD")
DB_NAME = os.getenv("TDB_DB")
DB_HOST = os.getenv("TDB_HOST")
#DB_URI = f"mariadb+mariadbconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
DB_URI = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}?charset=utf8mb4"

# How to pull results from each script (we’ll call a small function that returns a dict)
ORCHESTRATIONS = [
    # 1) Run-only ingestion: updates positive_pr table (no metrics returned)
    {"name": "imports_positive_pr", "mode": "cli_run", "as_module": False},

    # 2) Then collect metrics (these return dicts):
    {"name": "white_pages_scraper",   "mode": "call_func", "callable_name": "collect_metrics"},
    {"name": "github_scraper",        "mode": "call_func", "callable_name": "collect_metrics"},
    {"name": "google_sheets_scraper", "mode": "call_func", "callable_name": "collect_metrics"},
    {"name": "FRED_scraper",          "mode": "call_func", "callable_name": "collect_metrics"},
]

# Explicit dtypes (keeps MariaDB schema consistent)
EXPLICIT_DTYPES = {
    # white_pages_scraper
    "case_study_count": Integer(),
    "white_papers_count": Integer(),

    # github_scraper
    "open_app_count": Integer(),
    "regional_apps_count": Integer(),
    "open_components_count": Integer(),
    "os_org_count": Integer(),

    # google_sheets_scraper
    "open_resource_partners": Integer(),

    # FRED_scraper
    "total_pr": Integer(),
    "open_resources_aquifer": Integer(),
    "distinct_completed_OBS_count": Integer(),
    "total_product_count": Integer(),
    "bible_count_rolled": Integer(),
    "nt_count_rolled": Integer(),
    "ot_count_rolled": Integer(),
    "unique_language_engagement_ids": Integer(),

    # meta
    "run_ts": Date(),  # date only
}

# ========= IMPLEMENTATION =========

mylogger = get_logger()

@dataclass
class ScriptSpec:
    name: str
    mode: str
    variable: Optional[str] = None
    callable_name: Optional[str] = None
    as_module: Optional[bool] = None  # for cli_json (optional mode)

def _import_module(module_name: str):
    return importlib.import_module(module_name)

def _expect_dict(obj: Any, script_label: str) -> Dict[str, Any]:
    """
    Normalize collector output to a dict.
    - dict -> dict
    - list[dict] -> merged dict if length==1, else error (ambiguous for single-row run)
    - other -> wrap as {"value": obj}
    """
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if isinstance(obj, list):
        if len(obj) == 0:
            return {}
        if all(isinstance(x, dict) for x in obj):
            if len(obj) == 1:
                return obj[0]
            raise ValueError(f"{script_label}: returned {len(obj)} rows; single combined-row run requires exactly one.")
        # list of scalars -> shove into array as a JSON string
        return {"value": json.dumps(obj)}
    # scalar
    return {"value": obj}

def _extract_import_var(spec: ScriptSpec) -> Dict[str, Any]:
    mod = _import_module(spec.name)
    if not spec.variable:
        raise ValueError(f"{spec.name}: 'variable' must be set for mode=import_var")
    val = getattr(mod, spec.variable)
    return _expect_dict(val, spec.name)

def _extract_call_func(spec: ScriptSpec) -> Dict[str, Any]:
    mod = _import_module(spec.name)
    if not spec.callable_name:
        raise ValueError(f"{spec.name}: 'callable_name' must be set for mode=call_func")
    fn: Callable = getattr(mod, spec.callable_name)
    return _expect_dict(fn(), spec.name)

def _extract_cli_json(spec: ScriptSpec) -> Dict[str, Any]:
    cmd = [sys.executable, "-m", spec.name] if spec.as_module else [sys.executable, spec.name]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"{spec.name}: non-zero exit {proc.returncode}\nSTDERR:\n{proc.stderr}")
    return _expect_dict(json.loads(proc.stdout.strip()), spec.name)

def _run_cli(spec: ScriptSpec) -> Dict[str, Any]:
    """
    Execute a script and return no metrics (empty dict).
    Use when a script performs ingestion/side effects in its __main__ block.
    """
    # If as_module=True: python -m <module>; else: python <path-or-file>
    cmd = [sys.executable, "-m", spec.name] if spec.as_module else [sys.executable, f"{spec.name}.py"]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"{spec.name}: exit {proc.returncode}\nSTDERR:\n{proc.stderr}\nSTDOUT:\n{proc.stdout}"
        )
    # Optional: surface the script’s output in orchestrator logs
    if proc.stdout.strip():
        mylogger.debug(f"[{spec.name} stdout]\n{proc.stdout}")
    return {}  # no metrics, just side effects

MODE_HANDLERS = {
    "import_var": _extract_import_var,
    "call_func": _extract_call_func,
    "cli_json": _extract_cli_json,
    "cli_run": _run_cli,          # <— NEW
}

def _merge_metrics(rows_by_script: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge all metric dicts into a single dict. If any duplicate keys exist across scripts,
    raise a clear error so you can rename them.
    """
    merged: Dict[str, Any] = {}
    key_owner: Dict[str, str] = {}
    for script, payload in rows_by_script.items():
        for k, v in payload.items():
            if k in merged:
                prev = key_owner[k]
                raise ValueError(
                    f"Duplicate metric key '{k}' from '{script}' also returned by '{prev}'. "
                    "Rename one of them to avoid collision."
                )
            merged[k] = v
            key_owner[k] = script
    return merged

def run_all(orchestrations: List[Dict[str, Any]]) -> pd.DataFrame:
    run_date = datetime.now(timezone.utc).date()  # date only
    collected: Dict[str, Dict[str, Any]] = {}
    errors: Dict[str, str] = {}

    # Execute each collector
    for spec_dict in orchestrations:
        spec = ScriptSpec(**spec_dict)
        handler = MODE_HANDLERS.get(spec.mode)

        mylogger.info(f"- Starting '{spec.name}' collector ")

        if handler is None:
            errors[spec.name] = f"Unknown mode '{spec.mode}'"
            continue
        try:
            payload = handler(spec)
            collected[spec.name] = payload or {}
            mylogger.info(f"{spec.name}: collected {len(payload or {})} fields")
        except Exception as e:
            errors[spec.name] = str(e)
            mylogger.error(f"{spec.name}: {e}")

    # Merge into a single row of metrics
    merged_metrics: Dict[str, Any] = {}
    if collected:
        merged_metrics = _merge_metrics(collected)

    # Attach meta fields
    merged_metrics["run_ts"] = run_date
    if errors:
        # store error map as JSON text for inspection; does not block the run
        merged_metrics["errors"] = json.dumps(errors, ensure_ascii=False)

    # Build single-row DataFrame
    df = pd.DataFrame([merged_metrics])
    return df

def _dtype_map_for(df: pd.DataFrame) -> Dict[str, Any]:
    dtypes = {}
    for col in df.columns:
        if col in EXPLICIT_DTYPES:
            dtypes[col] = EXPLICIT_DTYPES[col]
    return dtypes

def write_to_mariadb(df: pd.DataFrame, table: str, if_exists: str = "append") -> None:
    if df.empty:
        mylogger.warn("No row produced; skipping DB write.")
        return
    # Normalize column names for MariaDB
    df = df.copy()
    df.columns = [c.replace(" ", "_") for c in df.columns]

    engine = create_engine(DB_URI, pool_pre_ping=True, pool_recycle=3600)
    try:
        df.to_sql(
            name=table,
            con=engine,
            if_exists=if_exists,   # 'append' in prod; 'replace' only when resetting
            index=False,
            dtype=_dtype_map_for(df),
            method="multi",
            chunksize=1000,
        )
        mylogger.info(f"Wrote 1 combined row to {table}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    df = run_all(ORCHESTRATIONS)
    mylogger.info(f"Produced columns: {list(df.columns)}")
    write_to_mariadb(df, "impact_model_metrics", if_exists="append")
