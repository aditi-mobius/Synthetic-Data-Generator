import numpy as np
import random
from typing import Dict, List, Any
from ._faker_manager import FakerManager

def generate_column(column_def: Dict[str, Any], n: int, faker_manager: FakerManager, table_locale: str | None) -> List[Any]:
    """
    Generate a column of synthetic values for a given column definition.
    Supports base types and distributions for scenario-aware data generation.

    Example column_def:
    {
        "name": "age",
        "type": "integer",
        "distribution": {"type": "normal", "mean": 30, "stddev": 10, "min": 0, "max": 100}
    }

    Args:
        faker_manager: The FakerManager to get locale-specific instances.
        table_locale: The locale specified for the table, if any.
    """
    ctype = column_def.get("type", "string")
    dist = column_def.get("distribution", {}) or {}
    dtype = dist.get("type")

    # Determine the correct locale: column > table > global default
    column_locale = dist.get("locale")
    faker_instance = faker_manager.get_instance(column_locale or table_locale)

    # 1. Name or ID-like fields
    if dtype == "name" or column_def.get("name", "").lower() in ("name", "fullname"):
        return [faker_instance.name() for _ in range(n)]
    if dtype == "name_male":
        return [faker_instance.name_male() for _ in range(n)]
    if dtype == "name_female":
        return [faker_instance.name_female() for _ in range(n)]

    # 2. Custom formatted strings (like EMP####)
    if dtype == "custom_format" or dist.get("pattern"):
        pattern = dist.get("pattern") or ""
        results = []
        for _ in range(n):
            s = "".join(str(random.randint(0, 9)) if ch == "#" else ch for ch in pattern)
            results.append(s)
        return results

    # 3. Categorical values
    if dtype == "categorical":
        values = dist.get("values", [])
        probs = dist.get("probabilities")
        if values:
            return list(np.random.choice(values, size=n, p=probs))
        return [faker_instance.word() for _ in range(n)]

    # 4. Sequential IDs or ordered values
    if dtype == "sequential":
        start = int(dist.get("start", 1))
        step = int(dist.get("step", 1))
        return list(range(start, start + step * n, step))

    # 5. Dates / temporal data
    if dtype == "date":
        start_date = dist.get("start_date", "-30d")
        end_date = dist.get("end_date", "today")
        return [faker_instance.date_between(start_date=start_date, end_date=end_date) for _ in range(n)]

    # 6. Integer distributions (normal or uniform)
    if ctype == "integer" or dtype in ("normal", "uniform", None):
        if dtype == "uniform":
            low = int(dist.get("min", 0))
            high = int(dist.get("max", 100))
            return np.random.randint(low, high + 1, size=n).tolist()
        else:
            mean = float(dist.get("mean", 50))
            std = float(dist.get("stddev", 10))
            arr = np.random.normal(loc=mean, scale=std, size=n).round().astype(int).tolist()
            mn = int(dist.get("min", -10**9))
            mx = int(dist.get("max", 10**9))
            return [max(mn, min(mx, int(x))) for x in arr]

    # 7. Floating-point distributions
    if ctype == "float":
        mean = float(dist.get("mean", 50.0))
        std = float(dist.get("stddev", 10.0))
        arr = np.random.normal(loc=mean, scale=std, size=n).tolist()
        mn = float(dist.get("min", -1e9))
        mx = float(dist.get("max", 1e9))
        return [max(mn, min(mx, float(x))) for x in arr]

    # 8. Boolean fields
    if ctype == "boolean":
        probs = dist.get("probabilities", [0.5, 0.5])
        return list(np.random.choice([True, False], size=n, p=probs))

    # 9. Fallback: generic strings
    return [faker_instance.word() for _ in range(n)]
