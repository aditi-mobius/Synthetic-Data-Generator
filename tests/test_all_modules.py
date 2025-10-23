import os
import json
from core.graph_parser import parse_graph
from core.scenario_data_generator import generate_scenario_data
from core.post_validator import post_generation_validate
from core.file_exporter import export_all_tables

def test_graph_pipeline():
    # Check graph file exists
    graph_path = os.path.join("config", "test_graph.json")
    assert os.path.exists(graph_path), "Graph file missing in config/"

    # Parse graph
    working_schema = parse_graph(graph_path)
    assert "tables" in working_schema and "edges" in working_schema, "Parsed schema invalid"

    # Generate data
    tables = generate_scenario_data(working_schema)
    assert isinstance(tables, dict) and len(tables) > 0, "No tables generated"
    for tname, df in tables.items():
        assert not df.empty, f"Table {tname} is empty"

    # Post-generation validation
    validated = post_generation_validate(tables, working_schema)
    assert isinstance(validated, dict), "Post-validation failed"

    # Export (to a temp folder)
    out_dir = "data/output/test_run"
    paths = export_all_tables(validated, out_dir, fmt="csv")
    for tname, path in paths.items():
        assert os.path.exists(path), f"Export failed for table {tname}"

    print("All graph-based pipeline tests passed!")


if __name__ == "__main__":
    test_graph_pipeline()
