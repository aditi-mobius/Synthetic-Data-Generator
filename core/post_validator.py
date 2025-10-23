import pandas as pd
from typing import Dict, Any

def post_generation_validate(all_tables: Dict[str, pd.DataFrame], spec: Dict) -> Dict[str, pd.DataFrame]:
    """
    Post-generation validation:
    - Verify FK consistency between child and parent tables.
    - Enforce uniqueness constraints.
    - Handle nullability and total participation.
    - Returns possibly modified all_tables.
    """

    if not spec or 'tables' not in spec:
        return all_tables

    for table in spec['tables']:
        tname = table['name']
        df = all_tables.get(tname)
        if df is None or not isinstance(df, pd.DataFrame):
            continue

        # --- Foreign Key Validation ---
        for col in table.get('columns', []):
            fk = col.get('foreign_key')
            if fk:
                parent_name = fk.get('table')
                parent_col = fk.get('column')
                parent_df = all_tables.get(parent_name)
                if parent_df is not None and parent_col in parent_df.columns:
                    valid_set = set(parent_df[parent_col].dropna().unique().tolist())
                    invalid_mask = ~df[col['name']].isin(valid_set)
                    if invalid_mask.any():
                        # Invalidate wrong FKs
                        df.loc[invalid_mask, col['name']] = None
                all_tables[tname] = df

        # --- Uniqueness Validation ---
        for col in table.get('columns', []):
            if col.get('unique', False) and col['name'] in df.columns:
                # Drop duplicates keeping first occurrence
                df = df.drop_duplicates(subset=[col['name']])
                all_tables[tname] = df

        # --- Nullability / Total Participation ---
        for col in table.get('columns', []):
            nullable = col.get('nullable', True)
            if not nullable and col['name'] in df.columns:
                df = df[df[col['name']].notnull()]
                all_tables[tname] = df

    return all_tables
