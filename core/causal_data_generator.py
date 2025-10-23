import pandas as pd
import numpy as np
from typing import Dict, Any

def generate_causal_by_scm(df: pd.DataFrame, scm_spec: Dict[str, Any], metadata: Dict[str, Any] = None) -> pd.DataFrame:
    """
    SCM-based generator for causal feature creation.

    Parameters:
        df: input DataFrame (base table)
        scm_spec: dict mapping column -> {'fn': expression}
            Example:
                {
                    "income": {"fn": "50*education + 10*experience + noise(0,1000)"},
                    "spending": {"fn": "0.5*income + noise(0,200)"}
                }
        metadata: optional table-level metadata (from graph nodes)

    Returns:
        DataFrame with generated causal columns added or updated.
    """
    if df is None or df.empty:
        return df
    if not scm_spec:
        return df

    df = df.copy()
    local_env = {"np": np}
    if metadata:
        local_env.update(metadata)

    for col, spec in scm_spec.items():
        expr = spec.get("fn")
        if not expr:
            continue

        # replace noise(a,b) with np.random.normal(a,b)
        expr_eval = expr.replace("noise(", "np.random.normal(")
        try:
            df[col] = df.eval(expr_eval, local_dict=local_env)
        except Exception:
            # fallback row-wise safe eval
            df[col] = df.apply(lambda row: eval(expr_eval, {"np": np, **row.to_dict(), **(metadata or {})}), axis=1)
    return df
