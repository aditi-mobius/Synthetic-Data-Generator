import pandas as pd
import random
from typing import Optional, Dict, Callable, Any

def assign_foreign_key(
    child_df: pd.DataFrame,
    child_col: str,
    parent_df: pd.DataFrame,
    parent_col: str,
    condition: Optional[Dict[str, Any]] = None,
    seed: Optional[int] = None
) -> pd.DataFrame:
    """
    Assign values to child_df[child_col] sampled from parent_df[parent_col].

    Args:
        child_df: The child table DataFrame.
        child_col: Column in child_df where foreign key values will be assigned.
        parent_df: The parent table DataFrame.
        parent_col: Column in parent_df containing unique key values.
        condition: Optional dict or callable to control selective assignment.
            Example dict: {"col": "isActive", "value": True}
            Example callable: lambda row: row["age"] > 18
        seed: Optional integer for deterministic randomization.

    Returns:
        Updated child DataFrame with the foreign key column populated.
    """
    if seed is not None:
        random.seed(seed)

    if parent_df is None or parent_col not in parent_df.columns:
        raise ValueError("Parent data must contain parent_col")

    parent_ids = parent_df[parent_col].dropna().unique().tolist()
    if len(parent_ids) == 0:
        raise ValueError("No parent keys to sample from")

    child_df = child_df.copy()

    # If no condition → assign randomly to all rows
    if condition is None:
        child_df[child_col] = random.choices(parent_ids, k=len(child_df))
        return child_df

    # If condition is callable
    if callable(condition):
        mask = child_df.apply(condition, axis=1)
        child_df.loc[mask, child_col] = random.choices(parent_ids, k=mask.sum())
        child_df.loc[~mask, child_col] = None
        return child_df

    # If condition is dict
    cond_col = condition.get("col")
    cond_val = condition.get("value")

    if cond_col is None:
        raise ValueError("Condition dict must have a 'col' key")

    mask = child_df[cond_col] == cond_val
    child_df.loc[mask, child_col] = random.choices(parent_ids, k=mask.sum())
    child_df.loc[~mask, child_col] = None

    return child_df
