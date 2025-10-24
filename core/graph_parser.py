import os
import json
import pickle
import networkx as nx


def _load_from_json(path):
    with open(path, "r") as f:
        data = json.load(f)
        # Handle older networkx versions by ensuring the edge key is 'links'
        if "edges" in data and "links" not in data:
            data["links"] = data.pop("edges")
        # The 'attrs' argument is not supported in all versions, so we avoid it.
        return nx.node_link_graph(data, directed=True, multigraph=False)


def _load_from_gpickle(path):
    with open(path, "rb") as f:
        return pickle.load(f)


def load_graph(graph_path):
    """
    Dynamically load a graph from supported formats.
    Supported: .graphml, .json (node-link), .gpickle/.pkl
    """
    ext = os.path.splitext(graph_path)[1].lower()
    loaders = {
            ".graphml": nx.read_graphml,
            ".json": _load_from_json,
            ".gpickle": _load_from_gpickle,
            ".pkl": _load_from_gpickle,
        }
    if ext not in loaders:
        raise ValueError(f"Unsupported graph format: {ext}")
    return loaders[ext](graph_path)


def load_ecore_model(path):
    """
    Load and parse an Ecore model into a dictionary.
    (Placeholder: assumes the Ecore model is JSON for now.)
    """
    if not path or not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return json.load(f)


def parse_graph(graph_path: str = None, graph_data: dict = None):
    """
    Parse a scenario graph into a working schema compatible with the generation pipeline.
    Produces working_schema with 'tables' and 'edges'.

    Args:
        graph_path: The file path to the graph JSON file.
        graph_data: A dictionary containing the graph data directly.
    """
    if graph_data:
        # To support older networkx versions, we ensure the edge key is 'links'
        # before passing the data to the parser.
        if "edges" in graph_data and "links" not in graph_data:
            graph_data["links"] = graph_data.pop("edges")
        G = nx.node_link_graph(graph_data, directed=True, multigraph=False)
    elif graph_path:
        G = load_graph(graph_path)
    else:
        raise ValueError("Either 'graph_path' or 'graph_data' must be provided.")

    tables = []
    edges = []

    for node_id, attrs in G.nodes(data=True):
        table_name = attrs.get("name", str(node_id))
        row_count = attrs.get("row_count", 100)
        locale = attrs.get("locale")
        time_series_spec = attrs.get("time_series_spec")

        # If columns defined in the graph, use them; otherwise add a default ID column
        columns = attrs.get("columns", [{"name": "id", "type": "int"}])

        # Capture dependencies (markov_blanket)
        dependencies = attrs.get("markov_blanket", [])
        # Capture top-level constraints for the table
        constraints = attrs.get("constraints", [])
        if "constraints" in G.graph and G.graph["constraints"]:
            constraints.extend([c for c in G.graph["constraints"] if c.get("table") == table_name])

        table_entry = {
            "name": table_name,
            "columns": columns,
            "rows": row_count,
            "dependencies": dependencies,
        }
        if locale:
            table_entry["locale"] = locale
        if time_series_spec:
            table_entry["time_series_spec"] = time_series_spec

        tables.append(table_entry)

    for u, v, edge_data in G.edges(data=True):
        edge_entry = {
            "from": G.nodes[u].get("name", str(u)),
            "to": G.nodes[v].get("name", str(v)),
            "relation_type": edge_data.get("relation_type", "dependency"),
            "constraints": edge_data.get("constraints", {}),
        }
        edges.append(edge_entry)

    working_schema = {
        "tables": tables,
        "edges": edges
    }

    return working_schema





if __name__ == "__main__":
    graph_path = "../data/input_graphs/example_graph.json"
    parsed = parse_graph(graph_path)
    print(json.dumps(parsed, indent=2))
