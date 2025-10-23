import pandas as pd
import re
from typing import Dict, List, Any

def enforce_constraints(df: pd.DataFrame, constraints: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Enforce intra-table and simple value-based constraints on a DataFrame.

    constraints example:
    [
        {"type": "value_range", "column": "age", "min": 22, "max": 60},
        {"type": "unique", "column": "employee_id"},
        {"type": "categorical", "column": "gender", "values": ["M", "F"]},
        {"type": "nullability", "column": "name", "nullable": False},
        {"type": "regex", "column": "email", "pattern": ".+@.+"}
    ]
    """
    if df is None or not isinstance(df, pd.DataFrame) or not constraints:
        return df

    df = df.copy()

    for c in constraints:
        ctype = c.get("type")
        col = c.get("column")
        if col not in df.columns:
            continue

        # 1. Numeric or value range constraint
        if ctype == "value_range":
            mn = c.get("min")
            mx = c.get("max")
            if mn is not None:
                df[col] = df[col].clip(lower=mn)
            if mx is not None:
                df[col] = df[col].clip(upper=mx)

        # 2. Categorical allowed values
        elif ctype == "categorical":
            allowed = c.get("values", [])
            if allowed:
                df = df[df[col].isin(allowed)]

        # 3. Nullability constraint
        elif ctype == "nullability":
            allow_null = c.get("nullable", True)
            if not allow_null:
                df = df[df[col].notnull()]

        # 4. Regex pattern enforcement
        elif ctype == "regex":
            pattern = c.get("pattern")
            if pattern:
                df = df[df[col].astype(str).str.match(pattern, na=False)]

        # 5. Uniqueness constraint
        elif ctype == "unique":
            df = df.drop_duplicates(subset=[col])

        # 6. Custom inter-table / graph constraints (placeholder)
        elif ctype == "graph_relation":
            # Placeholder: apply external validation at post-generation stage
            pass

    return df.reset_index(drop=True)
