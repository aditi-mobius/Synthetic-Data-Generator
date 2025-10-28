from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import pandas as pd
from enum import Enum
import io

# core generation logic
from core.graph_parser import parse_graph_from_dict
from core.scenario_data_generator import generate_scenario_data
from core.post_validator import post_generation_validate

# utility for CMS upload
from utils.cms_uploader import upload_to_cms

app = FastAPI(
    title="Synthetic Data Generator API",
    description="An API to generate complex, relational synthetic data from a graph definition.",
    version="1.0.0"
)

class GenerationOptions(BaseModel):
    """Defines the generation settings."""
    row_counts: Optional[Dict[str, int]] = Field(None, description="Override row counts for specific tables.")
    locale: str = Field("en_US", description="Default locale for Faker data generation (e.g., 'en_US', 'fr_FR').")

class ResponseType(str, Enum):
    """Enum for the supported response types."""
    RAW_JSON = "raw_json"
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"

class GenerationRequest(BaseModel):
    """The request body for the data generation endpoint."""
    nodes: List[Dict[str, Any]] = Field(..., description="List of nodes (tables) in the graph.")
    edges: List[Dict[str, Any]] = Field([], description="List of edges (relationships) between nodes.")
    constraints: Optional[List[Dict[str, Any]]] = Field(None, description="Global constraints for the graph.")
    response_type: ResponseType = Field(ResponseType.RAW_JSON, description="The desired output format. 'raw_json' returns data in the response body. Other formats upload files to CMS and return URLs.")
    options: Optional[GenerationOptions] = Field(default_factory=GenerationOptions, description="Settings for data generation.")


@app.post("/generate")
async def generate_data_endpoint(request: GenerationRequest, authorization: Optional[str] = Header(None)):
    """
    Generate and return synthetic data based on a graph definition.
    - If `response_type` is `raw_json`, the data is returned in the response body.
    - For other formats (`json`, `csv`, `parquet`), files are uploaded to a content service and their URLs are returned. An `Authorization: Bearer <token>` header is required for uploads.
    """
    try:
        # 1. Parse the graph dictionary from the request body
        # Reconstruct the graph dictionary to pass to the parser
        graph_dict = {
            "nodes": request.nodes,
            "edges": request.edges,
            "constraints": request.constraints or []
        }
        working_schema = parse_graph_from_dict(graph_dict)
        cdn_base_url = "https://cdn.gov-cloud.ai"

        # 2. Inject settings from the request into the working schema
        if request.options:
            working_schema.setdefault("metadata", {})["default_locale"] = request.options.locale
            if request.options.row_counts:
                for table in working_schema.get("tables", []):
                    if table["name"] in request.options.row_counts:
                        table["rows"] = request.options.row_counts[table["name"]]

        # 3. Run the existing generation and validation pipeline
        generated_tables = generate_scenario_data(working_schema)
        validated_tables = post_generation_validate(generated_tables, working_schema)

        # 4. Handle output based on response_type
        if request.response_type == ResponseType.RAW_JSON:
            output = {
                table_name: df.to_dict(orient="records")
                for table_name, df in validated_tables.items()
            }
            return output
        else:
            # Handle file-based output and upload
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
                elif file_format == "parquet":
                    df.to_parquet(buffer, index=False)
                
                buffer.seek(0)
                
                upload_result = await upload_to_cms(buffer, filename, token)

                if "error" in upload_result:
                    
                    raise HTTPException(status_code=502, detail=f"Failed to upload {filename} to CMS: {upload_result.get('details', upload_result['error'])}")
                cdn_main_url = upload_result.get("cdnUrl")

                output_urls[table_name] = f"{cdn_base_url}{cdn_main_url}"

            return {"cdn_urls": output_urls}

    except Exception as e:
        # Catch any errors from the generation pipeline and return a 500 error
        raise HTTPException(status_code=500, detail=f"An error occurred during data generation: {e}") from e
