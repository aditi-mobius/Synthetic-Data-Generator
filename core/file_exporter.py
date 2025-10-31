import os
import pandas as pd
from typing import Dict


def export_all_tables(tables_data: Dict[str, pd.DataFrame], output_dir: str, fmt: str = "csv") -> Dict[str, str]:
    """
    Export all pandas DataFrames to the specified format.

    Args:
        tables_data: A dictionary mapping table names to pandas DataFrames.
        output_dir: The directory to save the files in.
        fmt: The output format (e.g., "csv", "json", "parquet").

    Returns:
        A dictionary mapping table names to their output file paths.
    """
    paths = {}
    for table_name, df in tables_data.items():
        path = os.path.join(output_dir, f"{table_name}.{fmt}")
        if fmt == "csv":
            df.to_csv(path, index=False, encoding='utf-8')
        elif fmt == "json":
            # Manually convert date/datetime columns to ISO date strings to avoid the time part.
            for col in df.select_dtypes(include=['datetime64[ns]', 'object']).columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]) or isinstance(df[col].iloc[0], (pd.Timestamp, pd.NaT.__class__)):
                    df[col] = df[col].dt.strftime('%Y-%m-%d')
            df.to_json(path, orient="records", indent=2, force_ascii=False)
        elif fmt == "parquet":
            df.to_parquet(path, index=False)
        else:
            raise ValueError(f"Unsupported format: {fmt}")
        paths[table_name] = path
        print(f"  [INFO] Exported {table_name} to {path}")
    return paths