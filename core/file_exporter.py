import os
import pandas as pd
from typing import Dict
import io
import mimetypes
import asyncio
from collections import namedtuple

# Create a simple mock file object that the uploader can use.
# This mimics the structure of a file object from a web framework like FastAPI.
MockFile = namedtuple("MockFile", ["filename", "file", "content_type"])

# We don't have a real request object here, so we'll pass None.
# The uploader function will need to handle this.
from utils.cms_uploader import upload_to_cms


async def export_all_tables(tables_data: Dict[str, pd.DataFrame], output_dir: str, fmt: str = "csv", save_to_disk: bool = True) -> Dict[str, str]:
    """
    Export all pandas DataFrames to the specified format.

    Args:
        tables_data: A dictionary mapping table names to pandas DataFrames.
        output_dir: The directory to save the files in.
        fmt: The output format (e.g., "csv", "json", "parquet").
        save_to_disk: If True, saves the file to the local disk.

    Returns:
        A dictionary mapping table names to their output file paths.
    """
    
    async def export_and_upload_one(table_name: str, df: pd.DataFrame):
        """Helper to process one table."""
        try:
            file_name = f"{table_name}.{fmt}"
            local_path = os.path.join(output_dir, file_name)
            
            # --- Create in-memory file stream ---
            stream = io.BytesIO()
            if fmt == "csv":
                df.to_csv(stream, index=False, encoding='utf-8')
            elif fmt == "json":
                df.to_json(stream, orient="records", indent=2, force_ascii=False)
            elif fmt == "parquet":
                df.to_parquet(stream, index=False)
            else:
                raise ValueError(f"Unsupported format: {fmt}")
            stream.seek(0) # Rewind the stream to the beginning

            # --- Optionally save to disk ---
            if save_to_disk:
                with open(local_path, "wb") as f:
                    f.write(stream.getvalue())
                print(f"  [INFO] Exported {table_name} to {local_path}")
                stream.seek(0) # Rewind again after reading for save

            # --- Upload to CMS ---
            content_type, _ = mimetypes.guess_type(file_name)
            if content_type is None:
                content_type = 'application/octet-stream'
            
            file_to_upload = MockFile(filename=file_name, file=stream, content_type=content_type)
            
            cms_response = await upload_to_cms(request=None, file=file_to_upload)
            return table_name, {"local_path": local_path if save_to_disk else None, "cms_url": cms_response}
        except Exception as e:
            print(f"  [ERROR] Failed to upload {table_name} to CMS: {e}")
            return table_name, {"local_path": local_path if save_to_disk else None, "cms_url": None}

    # Run all uploads concurrently
    tasks = [export_and_upload_one(name, df) for name, df in tables_data.items()]
    results_list = await asyncio.gather(*tasks)
    
    # Convert list of tuples back to a dictionary
    results = {name: data for name, data in results_list}

    return results