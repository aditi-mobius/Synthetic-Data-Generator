import os
import argparse
import json
import time
import copy
import datetime
from core.graph_parser import parse_graph
from core.scenario_data_generator import generate_scenario_data
from core.post_validator import post_generation_validate
from core.file_exporter import export_all_tables

def json_date_serializer(obj):
    """JSON serializer for objects not serializable by default json code, like dates."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def generate_tables_from_graph(
    graph_path: str, 
    output_dir: str, 
    output_format: str = "json", 
    row_counts_override: str = None, 
    locale: str = "en_US",
    stream: bool = False,
    rate: int = 2,
    interval: int = 5,
    duration: int = 30,
    stream_out_path: str = None
):
    # Parse the scenario graph into working schema
    working_schema = parse_graph(graph_path)
    
    # Set the default locale from the command-line argument.
    # This will be used by the data generator unless a column specifies its own locale.
    working_schema.setdefault("metadata", {})["default_locale"] = locale
    
    if stream:
        # --- Streaming Generation Logic ---
        output_info = f"to directory '{stream_out_path}'" if stream_out_path else "to stdout"
        print(f"[INFO] Starting streaming generation: {rate} rows every {interval}s for {duration}s, outputting {output_info}.")
        
        open_files = {}
        sequential_state = {}  # State manager for sequential generators
        is_first_write = {}
        if stream_out_path:
            os.makedirs(stream_out_path, exist_ok=True)
            # Create and open a file for each table, overwriting existing ones.
            for table in working_schema.get("tables", []):
                table_name = table["name"]
                # Use the format specified, defaulting to csv
                if output_format == 'jsonl':
                    file_path = os.path.join(stream_out_path, f"{table_name}.jsonl")
                else:
                    file_path = os.path.join(stream_out_path, f"{table_name}.{output_format}")
                # 'w' mode truncates the file if it exists, achieving the overwrite effect.
                open_files[table_name] = open(file_path, 'w')
                if output_format == 'json':
                    # For JSON, write the opening bracket for the array
                    open_files[table_name].write('[\n')
                
                is_first_write[table_name] = True # Used to control headers for CSV and commas for JSON
                for column in table.get("columns", []):
                    if column.get("distribution", {}).get("type") == "sequential":
                        sequential_state[(table_name, column["name"])] = column["distribution"].get("start", 1)
        
        try:
            start_time = time.time()
            while time.time() - start_time < duration:
                # Create a deep copy of the schema to avoid modifying the original
                batch_schema = copy.deepcopy(working_schema)

                # Override row counts for all tables for this batch
                for table in batch_schema.get("tables", []):
                    table["rows"] = rate
                    # Update sequential start values from our state manager
                    for column in table.get("columns", []):
                        dist = column.get("distribution", {})
                        if dist.get("type") == "sequential":
                            state_key = (table["name"], column["name"])
                            current_start = sequential_state.get(state_key, dist.get("start", 1))
                            dist["start"] = current_start
                            sequential_state[state_key] = current_start + (rate * dist.get("step", 1))

                # Generate and validate a small batch of data
                generated_tables = generate_scenario_data(batch_schema)
                validated_tables = post_generation_validate(generated_tables, batch_schema)

                if open_files:
                    # Write each table's data to its respective file
                    for table_name, df in validated_tables.items():
                        if table_name in open_files:
                            # Write header only on the first batch for each file
                            file_handle = open_files[table_name] # This is a file handle
                            write_header = is_first_write.get(table_name, False)

                            if output_format == 'jsonl':
                                # Write each record as a separate JSON object on a new line
                                for record in df.to_dict(orient="records"):
                                    file_handle.write(json.dumps(record, default=json_date_serializer) + '\n')
                            elif output_format == 'json':
                                # Append records to the JSON array
                                records = df.to_dict(orient="records") # This is a list of dicts
                                for i, record in enumerate(records):
                                    # Add a comma and newline before each new object, except for the very first one in the file
                                    if not is_first_write[table_name] or i > 0:
                                        file_handle.write(',\n')
                                    file_handle.write('  ' + json.dumps(record, default=json_date_serializer)) # Indent for readability
                            elif output_format == 'csv':
                                df.to_csv(file_handle, index=False, header=write_header, lineterminator='\n')
                            # Parquet is not suitable for streaming appends in this manner, so it's omitted here.

                            # Force the OS to write the buffer to disk, making it visible to `tail -f`
                            file_handle.flush() 
                            os.fsync(file_handle.fileno())
                            # After the first write, subsequent writes should not include the header
                            if write_header:
                                is_first_write[table_name] = False
                else:
                    # Fallback to printing a single JSON object to stdout
                    output_json = json.dumps({
                        table_name: df.to_dict(orient="records")
                        for table_name, df in validated_tables.items()
                    }, default=json_date_serializer)
                    print(output_json)

                time.sleep(interval)
        finally:
            # Close all opened files
            for f in open_files.values():
                # For JSON, write the closing bracket
                if output_format == 'json':
                    f.write('\n]')
                f.close()
            print("[INFO] Streaming finished.")

        return {"status": "Streaming complete"}
    else:
        # --- Batch (File-based) Generation Logic ---
        print(f"[DEBUG] Schema parsed. Tables found: {len(working_schema.get('tables', []))}")

        # Override row counts if provided (ONLY in batch mode)
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
            except (json.JSONDecodeError, TypeError):
                print(f"[WARN] Invalid JSON or type in --row-counts argument: {row_counts_override}")

        # Generate data
        generated_tables = generate_scenario_data(working_schema)
        print(f"[DEBUG] Data generated. Tables: {len(generated_tables)}. Sizes: {[f'{name}:{len(df)}' for name, df in generated_tables.items()] if generated_tables else 'No tables generated.'}")

        # Perform post-generation validation (FKs, uniqueness, etc.)
        generated_tables = post_generation_validate(generated_tables, working_schema)
        print(f"[DEBUG] Post-validation complete. Tables: {len(generated_tables)}. Sizes: {[f'{name}:{len(df)}' for name, df in generated_tables.items()] if generated_tables else 'No tables after validation.'}")

        # Export all tables to files
        os.makedirs(output_dir, exist_ok=True)
        paths = export_all_tables(generated_tables, output_dir, fmt=output_format)
        return paths

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic Data Generator")
    
    # Batch arguments
    batch_group = parser.add_argument_group('Batch Generation Options')
    batch_group.add_argument("--graph", type=str, required=True, help="Path to input scenario graph")
    batch_group.add_argument("--out", type=str, default="data/output", help="Output folder for batch generation. Default: data/output")
    batch_group.add_argument("--fmt", type=str, default="csv", choices=["csv", "json", "parquet", "jsonl"], help="Output format for batch generation. Default: csv")
    batch_group.add_argument("--row-counts", type=str, default="100", help='JSON string or integer to override row counts, e.g., \'{"Customer": 200}\' or \'50\'')
    batch_group.add_argument("--locale", type=str, default="en_US", help="Default locale for Faker data generation (e.g., 'en_US', 'fr_FR')")

    # Streaming arguments
    stream_group = parser.add_argument_group('Streaming Generation Options')
    stream_group.add_argument("--stream", action='store_true', help="Enable streaming mode. Output is printed to stdout.")
    stream_group.add_argument("--stream-out", type=str, help="Output directory to save streaming files. If not provided, prints to stdout.")
    stream_group.add_argument("--rate", type=int, default=2, help="Number of rows to generate per interval in streaming mode.")
    stream_group.add_argument("--interval", type=int, default=5, help="Time in seconds between generation intervals in streaming mode.")
    stream_group.add_argument("--duration", type=int, default=10, help="Total time in seconds to stream data for.")

    args = parser.parse_args()

    paths = generate_tables_from_graph(
        args.graph, args.out, args.fmt, args.row_counts, args.locale,
        args.stream, args.rate, args.interval, args.duration, args.stream_out
    )
    if not args.stream:
        print("Generated files:", json.dumps(paths, indent=2))
