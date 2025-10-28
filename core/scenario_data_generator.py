import pandas as pd
import numpy as np
from typing import Dict, Any, List
from ._faker_manager import FakerManager

# -------------------------------
# Scenario Transformer
# -------------------------------
def generate_scenario_data(working_schema: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Orchestrates full data generation given a schema (graph-based or table-based).
    Automatically detects whether the schema uses 'tables' or 'nodes'.

    Steps:
        1. Analyze dependencies between tables/nodes.
        2. Generate columns for each entity.
        3. Enforce constraints.
        4. Assign foreign keys.
        5. Apply SCM/causal generation.
    """
    from core.dependency_analyzer import analyze_dependencies
    from core.column_generator import generate_column
    from core.constraint_enforcer import enforce_constraints
    from core.custom_providers import TransliteratedArabicProvider
    from core.fk_manager import assign_foreign_key
    from core.temporal_data_generator import generate_time_series_for_table
    from core.causal_data_generator import generate_causal_by_scm

    # ---------------------------
    # Detect schema format
    # ---------------------------
    if "tables" in working_schema:
        table_list = working_schema["tables"]
        schema_type = "tables"
    elif "nodes" in working_schema:
        # Convert node dicts to uniform table-like format
        table_list = [{"name": name, **node_def} for name, node_def in working_schema["nodes"].items()]
        schema_type = "nodes"
    else:
        raise KeyError("Schema must contain either 'tables' or 'nodes'.")

    print(f"[INFO] Detected schema type: {schema_type}")

    tables_data = {}
    dependencies, _ = analyze_dependencies(working_schema) # Correctly unpack the tuple
    
    # Initialize a FakerManager to handle locale management efficiently
    global_locale = working_schema.get("metadata", {}).get("default_locale", "en_US")
    faker_manager = FakerManager(global_locale)
    faker_manager.add_provider_for_locale("ar_PS", TransliteratedArabicProvider)
    print(f"  [INFO] Using global default locale '{global_locale}'")

    # Step 1: Generate base columns for each table in dependency order
    for table_name in dependencies:
        table_spec = next((t for t in table_list if t["name"] == table_name), None)
        if table_spec is None:
            continue
        
        # Determine the locale for this table, falling back to the global default
        table_locale = table_spec.get("locale")
        if table_locale:
            print(f"  [INFO] Using table-specific locale '{table_locale}' for table '{table_name}'")
        
        n = int(table_spec.get("rows", 5))
        df = pd.DataFrame()

        for col_def in table_spec.get("columns", []):
            col_name = col_def["name"]
            df[col_name] = generate_column(col_def, n, faker_manager, table_locale)

        # Step 2: Enforce constraints
        constraints = table_spec.get("constraints", [])
        df = enforce_constraints(df, constraints)

        tables_data[table_name] = df

        # Step 2.5: Generate time-series data if specified
        time_series_spec = table_spec.get("time_series_spec")
        if time_series_spec:
            df = generate_time_series_for_table(df, time_series_spec)
            tables_data[table_name] = df
        print(f"  [DEBUG] After initial generation for {table_name}: {len(df)} rows")

    # Step 3: Assign foreign keys
    for table_spec in table_list:
        tname = table_spec["name"]
        df = tables_data.get(tname)
        if df is None:
            continue

        for col in table_spec.get("columns", []):
            fk = col.get("foreign_key")
            if fk:
                parent_name = fk.get("table")
                parent_col = fk.get("column")
                parent_df = tables_data.get(parent_name)
                if parent_df is not None:
                    df = assign_foreign_key(df, col["name"], parent_df, parent_col)

        tables_data[tname] = df
        print(f"  [DEBUG] After FK assignment for {tname}: {len(df)} rows")

    # Step 4: Apply causal models (if present)
    for table_spec in table_list:
        tname = table_spec["name"]
        df = tables_data.get(tname)
        if df is None:
            continue
        scm_spec = table_spec.get("scm")
        if scm_spec:
            df = generate_causal_by_scm(df, scm_spec)
        tables_data[tname] = df
        print(f"  [DEBUG] After SCM for {tname}: {len(df)} rows")

    return tables_data


def apply_scenario(df: pd.DataFrame, scenario: Dict[str, Any]) -> pd.DataFrame:
    """
    Apply scenario-based transformations to the DataFrame.
    Example:
        {"type": "inflation", "field": "price", "multiplier": 1.1}
    """
    df = df.copy()
    stype = scenario.get("type")

    if stype == "inflation":
        field = scenario.get("field")
        mult = float(scenario.get("multiplier", 1.0))
        if field in df.columns:
            df[field] *= mult

    elif stype == "high_demand":
        field = scenario.get("field", "demand")
        scale = float(scenario.get("scale", 1.3))
        if field in df.columns:
            df[field] = df[field].astype(float) * scale

    # Add more scenario handlers as needed
    return df
