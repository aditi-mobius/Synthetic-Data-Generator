from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
import pandas as pd

# core generation logic
from core.graph_parser import parse_graph_from_dict
from core.scenario_data_generator import generate_scenario_data
from core.post_validator import post_generation_validate

app = FastAPI(
    title="Synthetic Data Generator API",
    description="An API to generate complex, relational synthetic data from a graph definition.",
    version="1.0.0"
)

class GenerationOptions(BaseModel):
    """Defines the generation settings."""
    row_counts: Optional[Dict[str, int]] = Field(None, description="Override row counts for specific tables.")
    locale: str = Field("en_US", description="Default locale for Faker data generation (e.g., 'en_US', 'fr_FR').")

class GenerationRequest(BaseModel):
    """The request body for the data generation endpoint."""
    nodes: List[Dict[str, Any]] = Field(..., description="List of nodes (tables) in the graph.")
    edges: List[Dict[str, Any]] = Field([], description="List of edges (relationships) between nodes.")
    constraints: Optional[List[Dict[str, Any]]] = Field(None, description="Global constraints for the graph.")
    options: Optional[GenerationOptions] = Field(default_factory=GenerationOptions, description="Settings for data generation.")


@app.post("/generate", response_model=Dict[str, List[Dict[str, Any]]])
async def generate_data_endpoint(request: GenerationRequest):
    """
    Generate and return synthetic data based on a graph definition.
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

        # 4. Convert pandas DataFrames to JSON-friendly format and return
        output = {
            table_name: df.to_dict(orient="records")
            for table_name, df in validated_tables.items()
        }
        return output

    except Exception as e:
        # Catch any errors from the generation pipeline and return a 500 error
        raise HTTPException(status_code=500, detail=f"An error occurred during data generation: {e}")
