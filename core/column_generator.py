import numpy as np
import random
from typing import Dict, List, Any, Callable
from ._faker_manager import FakerManager


def generate_column(column_def: Dict[str, Any], n: int, faker_manager: FakerManager, table_locale: str | None, table_data: 'DataFrame' = None) -> List[Any]:
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
        table_data: The DataFrame containing already generated columns for the current table.
    """
    ctype = column_def.get("type", "string")
    dist = column_def.get("distribution", {}) or {}
    dtype = dist.get("type")

    # Determine the correct locale: column > table > global default
    column_locale = dist.get("locale")
    faker_instance = faker_manager.get_instance(column_locale or table_locale)

    # --- Generator functions for different distribution types ---

    def _generate_name(col_def, num_rows, faker):
        return [faker.name() for _ in range(num_rows)]

    def _generate_name_male(col_def, num_rows, faker):
        return [faker.name_male() for _ in range(num_rows)]

    def _generate_name_female(col_def, num_rows, faker):
        return [faker.name_female() for _ in range(num_rows)]

    def _generate_custom_format(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        pattern = dist.get("pattern") or ""
        return ["".join(str(random.randint(0, 9)) if ch == "#" else ch for ch in pattern) for _ in range(num_rows)]

    def _generate_categorical(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        values = dist_def.get("values", [])
        probs = dist_def.get("probabilities")
        if values:
            return list(np.random.choice(values, size=num_rows, p=probs))
        return [faker.word() for _ in range(num_rows)]

    def _generate_sequential(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        start = int(dist_def.get("start", 1))
        step = int(dist_def.get("step", 1))
        return list(range(start, start + step * num_rows, step))

    def _generate_date(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        start_date = dist_def.get("start_date", "-30d")
        end_date = dist_def.get("end_date", "today")
        return [faker.date_between(start_date=start_date, end_date=end_date) for _ in range(num_rows)]

    def _generate_integer(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        dist_type = dist_def.get("type", "uniform")  # Default to uniform for integers
        if dist_type == "uniform":
            low = int(dist_def.get("min", 0))
            high = int(dist_def.get("max", 100))
            return np.random.randint(low, high + 1, size=num_rows).tolist()
        # Default to normal distribution for integers if not uniform
        mean = float(dist_def.get("mean", 50))
        std = float(dist_def.get("stddev", 10))
        arr = np.random.normal(loc=mean, scale=std, size=num_rows).round().astype(int)
        mn = int(dist_def.get("min", -10**9))
        mx = int(dist_def.get("max", 10**9))
        return np.clip(arr, mn, mx).tolist()

    def _generate_float(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        mean = float(dist_def.get("mean", 50.0))
        std = float(dist_def.get("stddev", 10.0))
        arr = np.random.normal(loc=mean, scale=std, size=num_rows)
        mn = float(dist_def.get("min", -1e9))
        mx = float(dist_def.get("max", 1e9))
        return np.clip(arr, mn, mx).tolist()

    def _generate_boolean(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        probs = dist_def.get("probabilities", [0.5, 0.5])
        return list(np.random.choice([True, False], size=num_rows, p=probs))

    def _generate_conditional(col_def, num_rows, faker):
        dist_def = col_def.get("distribution", {})
        on_column = dist_def.get("on")
        cases = dist_def.get("cases", {})
        if not on_column or not cases or table_data is None:
            return [None] * num_rows

        results = []
        for _, row in table_data.iterrows():
            on_value = row[on_column]
            case_def = cases.get(on_value)
            if case_def:
                # The sub-generator needs a column definition.
                # We create a temporary one.
                temp_col_def = {"distribution": case_def}
                # We only need one value.
                results.append(generate_column(temp_col_def, 1, faker_manager, table_locale, table_data)[0])
            else:
                results.append(None)
        return results

    # --- Mapping from type to generator function ---
    generator_map: Dict[str, Callable] = {
        "name": _generate_name,
        "name_male": _generate_name_male,
        "name_female": _generate_name_female,
        "custom_format": _generate_custom_format,
        "categorical": _generate_categorical,
        "sequential": _generate_sequential,
        "date": _generate_date,
        "integer": _generate_integer,
        "float": _generate_float,
        "boolean": _generate_boolean,
        "uniform": _generate_integer,  # Assuming uniform is for integers
        "normal": _generate_integer,   # Assuming normal is for integers by default
        "conditional": _generate_conditional,
    }

    # --- Main generation logic ---
    # Prioritize distribution type, then column type, then fallback
    generator_key = dtype
    if not generator_key:
        if ctype in generator_map:
            generator_key = ctype
        elif dist.get("pattern"):
            generator_key = "custom_format"
        elif column_def.get("name", "").lower() in ("name", "fullname"):
            generator_key = "name"

    if generator_key in generator_map:
        return generator_map[generator_key](column_def, n, faker_instance)

    # Fallback for simple types without a specific distribution
    if ctype in generator_map:
        return generator_map[ctype](column_def, n, faker_instance)

    # Final fallback
    return [faker_instance.word() for _ in range(n)]
