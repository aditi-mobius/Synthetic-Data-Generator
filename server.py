from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import pandas as pd
from enum import Enum
import io
import asyncio
import json
import copy
import datetime

# core generation logic
from core.graph_parser import parse_graph_from_dict
from core.scenario_data_generator import generate_scenario_data
from core.post_validator import post_generation_validate

# utility for CMS upload
# from utils.cms_uploader import upload_to_cms # Assuming this might be used later

app = FastAPI(
    title="Synthetic Data Generator API",
    description="An API to generate complex, relational synthetic data from a graph definition.",
    version="1.0.0"
)

def json_date_serializer(obj):
    """JSON serializer for objects not serializable by default json code, like dates."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

class StreamingOptions(BaseModel):
    """Defines the streaming generation settings."""
    rate: int = Field(2, description="Number of rows to generate per interval.")
    interval_seconds: int = Field(5, description="Time in seconds between generation intervals.")
    duration_seconds: int = Field(30, description="Total time in seconds to generate data for.")

class GenerationOptions(BaseModel):
    """Defines the generation settings."""
    row_counts: Optional[Dict[str, int]] = Field(None, description="Override row counts for specific tables.")
    locale: str = Field("en_US", description="Default locale for Faker data generation (e.g., 'ar_PS', 'fr_FR').")
    stream: Optional[StreamingOptions] = Field(None, description="If provided, enables streaming generation via the /generate-stream endpoint.")

class BatchGenerationOptions(BaseModel):
    """Defines the generation settings for batch mode."""
    row_counts: Optional[Dict[str, int]] = Field(None, description="Override row counts for specific tables.")
    locale: str = Field("en_US", description="Default locale for Faker data generation (e.g., 'ar_PS', 'fr_FR').")

class ResponseType(str, Enum):
    """Enum for the supported response types."""
    RAW_JSON = "raw_json"
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"
    JSONL = "jsonl"

class GenerationRequest(BaseModel):
    """The request body for the streaming data generation endpoint."""
    nodes: List[Dict[str, Any]] = Field(..., description="List of nodes (tables) in the graph.")
    edges: List[Dict[str, Any]] = Field([], description="List of edges (relationships) between nodes.")
    constraints: Optional[List[Dict[str, Any]]] = Field(None, description="Global constraints for the graph.")
    options: Optional[GenerationOptions] = Field(default_factory=GenerationOptions, description="Settings for data generation.")

class BatchGenerationRequest(BaseModel):
    """The request body for the batch data generation endpoint."""
    nodes: List[Dict[str, Any]] = Field(..., description="List of nodes (tables) in the graph.")
    edges: List[Dict[str, Any]] = Field([], description="List of edges (relationships) between nodes.")
    constraints: Optional[List[Dict[str, Any]]] = Field(None, description="Global constraints for the graph.")
    response_type: ResponseType = Field(ResponseType.RAW_JSON, description="The desired output format. 'raw_json' returns data in the response body. Other formats upload files to CMS and return URLs.")
    options: Optional[BatchGenerationOptions] = Field(default_factory=BatchGenerationOptions, description="Settings for data generation.")

@app.post("/generate-batch")
async def generate_batch_endpoint(request: BatchGenerationRequest, authorization: Optional[str] = Header(None)):
    """
    Generate and return synthetic data based on a graph definition.
    - If `response_type` is `raw_json`, the data is returned in the response body.
    - For other formats (`json`, `csv`, `parquet`), files are uploaded to a content service and their URLs are returned. An `Authorization: Bearer <token>` header is required for uploads.
    """
    try:
        print("[INFO] Starting batch generation request.")
        # 1. Parse the graph dictionary from the request body
        # Reconstruct the graph dictionary to pass to the parser
        graph_dict = {
            "nodes": request.nodes,
            "edges": request.edges,
            "constraints": request.constraints or []
        }
        working_schema = parse_graph_from_dict(graph_dict) # This function is in core/graph_parser.py
        cdn_base_url = "https://cdn.gov-cloud.ai"

        # 2. Inject settings from the request into the working schema
        if request.options:
            working_schema.setdefault("metadata", {})["default_locale"] = request.options.locale
            if request.options.row_counts:
                for table in working_schema.get("tables", []):
                    if table["name"] in request.options.row_counts:
                        table["rows"] = request.options.row_counts[table["name"]]

        # 3. Generate and validate the data AFTER all options have been set
        generated_tables = generate_scenario_data(working_schema)
        validated_tables = post_generation_validate(generated_tables, working_schema)

        # 4. Handle output based on response_type
        if request.response_type == ResponseType.RAW_JSON:
            # For raw_json, convert the final DataFrames to a dictionary and return
            output = {
                table_name: df.to_dict(orient="records")
                for table_name, df in validated_tables.items()
            }
            return output
        
        # Handle file-based output and upload for other response_types
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization.split(" ")[1]

        if not token:
            raise HTTPException(status_code=401, detail="Authorization token is required for file uploads. Please provide it in the 'Authorization: Bearer <token>' header.")

        output_urls = {}
        file_format = request.response_type.value

        for table_name, df in validated_tables.items():
            buffer = io.BytesIO()
            filename = f"{table_name}.{file_format}"

            if file_format == "csv":
                df.to_csv(buffer, index=False, encoding='utf-8')
            elif file_format == "json":
                df.to_json(buffer, orient="records", indent=2, force_ascii=False)
            elif file_format == "jsonl":
                df.to_json(buffer, orient="records", lines=True, force_ascii=False)
            elif file_format == "parquet":
                df.to_parquet(buffer, index=False)
            
            # buffer.seek(0)
            # upload_result = await upload_to_cms(buffer, filename, token)
            # Mocking upload result for now
            upload_result = {"cdnUrl": f"/uploads/{filename}"}

            if "error" in upload_result:
                raise HTTPException(status_code=502, detail=f"Failed to upload {filename} to CMS: {upload_result.get('details', upload_result['error'])}")
            cdn_main_url = upload_result.get("cdnUrl")

            output_urls[table_name] = f"{cdn_base_url}{cdn_main_url}"

        return {"cdn_urls": output_urls}

    except Exception as e:
        print(f"[ERROR] An error occurred during batch generation: {e}")
        # Catch any errors from the generation pipeline and return a 500 error
        raise HTTPException(status_code=500, detail=f"An error occurred during data generation: {e}") from e


@app.post("/generate-stream")
async def generate_stream_endpoint(request: GenerationRequest):
    """
    Generates synthetic data and streams it back to the client over time using Server-Sent Events (SSE).
    """
    if not (request.options and request.options.stream):
        raise HTTPException(status_code=400, detail="Streaming options ('options.stream') are required for this endpoint.")

    stream_options = request.options.stream

    async def event_generator():
        total_time = 0
        sequential_state = {} # State manager for sequential generators

        # Initialize state from the base graph definition
        base_schema = parse_graph_from_dict({
            "nodes": request.nodes, "edges": request.edges, "constraints": request.constraints or []
        })
        for table in base_schema.get("tables", []):
            for column in table.get("columns", []):
                if column.get("distribution", {}).get("type") == "sequential":
                    sequential_state[(table["name"], column["name"])] = column["distribution"].get("start", 1)

        print(f"[INFO] Starting API stream: {stream_options.rate} rows every {stream_options.interval_seconds}s for {stream_options.duration_seconds}s.")
        
        while total_time < stream_options.duration_seconds:
            # Create a deep copy of the schema for this batch
            batch_schema = copy.deepcopy(base_schema)

            # Inject locale and update sequential start values
            batch_schema.setdefault("metadata", {})["default_locale"] = request.options.locale
            batch_schema.setdefault("metadata", {})["default_locale"] = request.options.locale if request.options else "en_US"
            for table in batch_schema.get("tables", []):
                table["rows"] = stream_options.rate
                for column in table.get("columns", []):
                    dist = column.get("distribution", {})
                    if dist.get("type") == "sequential":
                        state_key = (table["name"], column["name"])
                        current_start = sequential_state.get(state_key, dist.get("start", 1))
                        dist["start"] = current_start
                        sequential_state[state_key] = current_start + (stream_options.rate * dist.get("step", 1))

            # Generate and validate a small batch of data
            generated_tables = generate_scenario_data(batch_schema)
            validated_tables = post_generation_validate(generated_tables, batch_schema)

            # Convert to JSON and yield in SSE format
            output = {
                table_name: df.to_dict(orient="records")
                for table_name, df in validated_tables.items()
            }
            yield f"data: {json.dumps(output, default=json_date_serializer)}\n\n"

            # Wait for the next interval
            await asyncio.sleep(stream_options.interval_seconds)
            total_time += stream_options.interval_seconds
        
        print("[INFO] API stream finished.")

    return StreamingResponse(event_generator(), media_type="text/event-stream")
