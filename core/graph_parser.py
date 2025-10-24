import os
import json
import pickle
import networkx as nx


def _load_from_json(path):
    with open(path, "r") as f:
        data = json.load(f)
        edge_key = "edges" if "edges" in data else "links"
        return nx.node_link_graph(data, edges=edge_key)


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


def parse_graph(graph_path):
    """
    Parse a scenario graph into a working schema compatible with the generation pipeline.
    Produces working_schema with 'tables' and 'edges'.
    """
    G = load_graph(graph_path)

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

def parse_graph_from_dict(graph_dict: dict) -> dict:
    """
    Parse a scenario graph from a dictionary into a working schema.
    This is used for API-based generation where the graph is in the request body.
    """
    edge_key = "edges" if "edges" in graph_dict else "links"
    # Use networkx to interpret the node-link structure, supporting 'edges' or 'links'
    # The 'attrs' argument is not supported by node_link_graph, attributes are read directly from node/edge dicts.
    G = nx.node_link_graph(graph_dict, directed=True, multigraph=False, edges=edge_key)

    tables = []
    edges = []

    for node_id, attrs in G.nodes(data=True):
        table_name = attrs.get("name", str(node_id))
        row_count = attrs.get("row_count", 100)
        locale = attrs.get("locale")
        time_series_spec = attrs.get("time_series_spec")

        columns = attrs.get("columns", [{"name": "id", "type": "int"}])
        dependencies = attrs.get("markov_blanket", [])
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

    return {"tables": tables, "edges": edges}





if __name__ == "__main__":
    graph_path = "../data/input_graphs/example_graph.json"
    parsed = parse_graph(graph_path)
    print(json.dumps(parsed, indent=2))
