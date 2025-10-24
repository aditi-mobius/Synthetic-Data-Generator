import os
import argparse
import json
from core.graph_parser import parse_graph
from core.scenario_data_generator import generate_scenario_data
from core.post_validator import post_generation_validate
from core.file_exporter import export_all_tables
from typing import Dict, Any, Optional

def generate_tables_from_graph(output_dir: str, output_format: str = "csv", row_counts_override: str = None, locale: str = "en_US", graph_path: Optional[str] = None, graph_data: Optional[Dict[str, Any]] = None, save_to_disk: bool = True):
    # Parse the scenario graph into working schema
    working_schema = parse_graph(graph_path=graph_path, graph_data=graph_data)



    # Set the default locale from the command-line argument.
    # This will be used by the data generator unless a column specifies its own locale.
    working_schema.setdefault("metadata", {})["default_locale"] = locale
    
    # Override row counts if provided
    if row_counts_override:
        try:
            overrides = json.loads(row_counts_override)
            if isinstance(overrides, dict):
                # Apply specific overrides from a dictionary
                for table in working_schema.get("tables", []):
                    if table["name"] in overrides:
                        table["rows"] = overrides[table["name"]]
                        print(f"[INFO] Overriding row count for '{table['name']}' to {table['rows']}")
            elif isinstance(overrides, int):
                # Apply a single row count to all tables
                print(f"[INFO] Overriding row count for all tables to {overrides}")
                for table in working_schema.get("tables", []):
                    table["rows"] = overrides
            else:
                print(f"[WARN] --row-counts must be a JSON dictionary or a single integer. Ignoring.")
        except json.JSONDecodeError:
            print(f"[WARN] Invalid JSON in --row-counts argument: {row_counts_override}")

    print(f"[DEBUG] Schema parsed. Tables found: {len(working_schema.get('tables', []))}")

    # Generate data
    generated_tables = generate_scenario_data(working_schema)
    print(f"[DEBUG] Data generated. Tables: {len(generated_tables)}. Sizes: {[f'{name}:{len(df)}' for name, df in generated_tables.items()] if generated_tables else 'No tables generated.'}")

    # Perform post-generation validation (FKs, uniqueness, etc.)
    generated_tables = post_generation_validate(generated_tables, working_schema)
    print(f"[DEBUG] Post-validation complete. Tables: {len(generated_tables)}. Sizes: {[f'{name}:{len(df)}' for name, df in generated_tables.items()] if generated_tables else 'No tables after validation.'}")


    # Export all tables
    os.makedirs(output_dir, exist_ok=True)
    paths = export_all_tables(generated_tables, output_dir, fmt=output_format, save_to_disk=save_to_disk)
    return paths

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--graph", type=str, help="Path to input scenario graph")
    parser.add_argument("--out", type=str, default="data/output", help="Output folder")
    parser.add_argument("--fmt", type=str, default="json", help="Output format: csv/json/parquet")
    parser.add_argument("--row-counts", type=str, help='JSON string to override row counts, e.g., \'{"Customer": 200, "Order": 500}\'')
    parser.add_argument("--locale", type=str, default="en_US", help="Default locale for Faker data generation (e.g., 'en_US', 'fr_FR', 'ja_JP', 'ar_PS)")
    args = parser.parse_args()

    paths = generate_tables_from_graph(graph_path=args.graph, output_dir=args.out, output_format=args.fmt, row_counts_override=args.row_counts, locale=args.locale)
    print("Generated files:", json.dumps(paths, indent=2))
