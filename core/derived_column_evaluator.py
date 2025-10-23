import pandas as pd
import numpy as np
from typing import Dict, Any

def evaluate_derived(df: pd.DataFrame, derived_defs: Dict[str, Any], context: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Evaluate derived columns based on provided expressions.
    
    Parameters:
    - df: DataFrame for the current table
    - derived_defs: dict of {column_name: formula}
        Example: {"total_salary": "base + bonus + hra"}
    - context: optional dict for extra variables or graph-level metadata
        (e.g., Markov blanket features, scenario constants)
    
    Supports vectorized operations and safe numpy functions.
    """
    if not derived_defs:
        return df

    df = df.copy()
    safe_context = {
        "np": np,
        "pd": pd,
        **(context or {})
    }

    for col, expr in derived_defs.items():
        if not expr:
            continue
        try:
            # Prefer pandas.eval for vectorized arithmetic
            df[col] = pd.eval(expr, engine="python", local_dict={**df.to_dict("series"), **safe_context})
        except Exception:
            try:
                # fallback: per-row evaluation
                df[col] = df.apply(lambda row: eval(expr, {"np": np, "pd": pd}, {**row.to_dict(), **(context or {})}), axis=1)
            except Exception as e:
                print(f"[WARN] Failed to evaluate derived column '{col}': {e}")
                df[col] = np.nan

    return df
