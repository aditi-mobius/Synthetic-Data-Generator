import math
import pandas as pd
from typing import List

def split_into_batches(df: pd.DataFrame, batch_size: int) -> List[pd.DataFrame]:
    """Return list of DataFrame batches"""
    if batch_size <= 0:
        return [df]
    n = len(df)
    parts = []
    for i in range(0, n, batch_size):
        parts.append(df.iloc[i:i+batch_size].reset_index(drop=True))
    return parts
