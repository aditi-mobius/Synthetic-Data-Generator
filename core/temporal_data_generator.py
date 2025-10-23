import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any


def generate_time_series_for_table(df: pd.DataFrame, time_spec: Dict[str, Any]) -> pd.DataFrame:
    """
    Add/generate a time column according to time_spec:
    time_spec example:
    {
        "column": "timestamp",
        "start": "2022-01-01",
        "freq": "D"
    }
    """
    df = df.copy()
    n = len(df)
    start_str = time_spec.get("start", datetime.utcnow().isoformat())
    start = pd.to_datetime(start_str)
    freq = time_spec.get("freq", "D")  # 'H' for hour, 'D' for day, 'T' for minute
    times = pd.date_range(start=start, periods=n, freq=freq)
    df[time_spec.get("column", "timestamp")] = times
    return df
