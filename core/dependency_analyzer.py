from typing import Dict, List, Tuple
import networkx as nx

def analyze_dependencies(spec: Dict) -> Tuple[List[str], nx.DiGraph]:
    """
    Build a dependency graph based on foreign_key references in spec.
    Returns (topologically sorted list of table names, dependency graph).
    """
    G = nx.DiGraph()
    tables = [t['name'] for t in spec.get('tables', [])]
    for t in tables:
        G.add_node(t)

    # Add directed edges parent -> child based on foreign keys
    for table in spec.get('tables', []):
        tname = table.get('name')
        for col in table.get('columns', []):
            fk = col.get('foreign_key')
            if isinstance(fk, dict):
                parent = fk.get('table')
                if parent and parent in tables:
                    G.add_edge(parent, tname)

    # Topological sort ensures no cycles
    try:
        order = list(nx.topological_sort(G))
        print(f"[DEBUG] Dependency order: {order}")
        return order, G
    except nx.NetworkXUnfeasible:
        raise ValueError("Cyclic dependency detected among tables")
