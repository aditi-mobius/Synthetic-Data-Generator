import os
from typing import Dict, Any, Optional

from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel, Field

from main import generate_tables_from_graph

app = FastAPI(
    title="Synthetic Data Generator API",
    description="An API to generate synthetic data from a graph definition.",
    version="1.0.0"
)

class GenerationRequest(BaseModel):
    graph: Dict[str, Any] = Field(..., description="The JSON object defining the data generation graph.")
    output_dir: str = Field("data/output", description="Directory to save the output files on the server.")
    output_format: str = Field("csv", enum=["csv", "json", "parquet"], description="The format for the output files.")
    row_counts: Optional[str] = Field(None, description='JSON string or integer to override row counts, e.g., \'{"Customer": 200}\' or 100.')
    save_to_disk: bool = Field(True, description="Whether to save the generated files to the local disk.")
    locale: str = Field("en_US", description="Default locale for data generation (e.g., 'en_US', 'ar_PS').")

@app.post("/generate/", summary="Generate Synthetic Data from Graph")
async def generate_data_endpoint(
    request: GenerationRequest = Body(...)
):
    """
    Generate synthetic data tables based on a provided graph structure.

    - **graph**: The JSON object defining the nodes (tables), edges (relations), and constraints.
    - **output_dir**: The server path where generated files will be stored.
    - **output_format**: The desired file format for the output tables.
    - **row_counts**: Override row counts for all or specific tables.
    - **save_to_disk**: If `false`, files will only be uploaded to CMS and not saved locally.
    - **locale**: The default locale for generating fake data.
    """
    try:
        # The generate_tables_from_graph function now accepts a dictionary for the graph
        # and returns a dictionary with local paths and CMS URLs.
        results = generate_tables_from_graph(
            graph_data=request.graph,
            output_dir=request.output_dir,
            output_format=request.output_format,
            row_counts_override=request.row_counts,
            locale=request.locale,
            save_to_disk=request.save_to_disk
        )
        return {"message": "Data generated and uploaded successfully", "files": results}
    except Exception as e:
        # In a real application, you would log this exception.
        raise HTTPException(status_code=500, detail=f"An error occurred during data generation: {str(e)}")
