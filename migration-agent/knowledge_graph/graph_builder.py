"""
Knowledge graph builder for transformation lineage
"""
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
try:
    import networkx as nx
except ImportError:
    nx = None


class KnowledgeGraphBuilder:
    """Build and maintain knowledge graph of transformations"""
    
    def __init__(self):
        """Initialize graph builder"""
        if nx is None:
            raise ImportError("NetworkX is required for knowledge graph building")
        
        self.graph = nx.DiGraph()
    
    def add_transformation_node(
        self,
        node_id: str,
        node_type: str,
        name: str,
        description: str = "",
        properties: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add transformation node to graph"""
        self.graph.add_node(
            node_id,
            type=node_type,
            name=name,
            description=description,
            properties=properties or {}
        )
    
    def add_data_flow_edge(
        self,
        source_node: str,
        target_node: str,
        field_mapping: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Add data flow edge between nodes"""
        self.graph.add_edge(
            source_node,
            target_node,
            field_mapping=field_mapping or {},
            metadata=metadata or {}
        )
    
    def build_from_canonical(self, canonical_mappings) -> None:
        """Build graph from canonical models"""
        for canonical in canonical_mappings:
            # Add source nodes
            for source_name in canonical.sources:
                self.add_transformation_node(
                    f"source_{source_name}",
                    "source",
                    source_name,
                    "Source system"
                )
            
            # Add transformation nodes
            for step in canonical.steps:
                self.add_transformation_node(
                    step.id,
                    step.type,
                    step.name,
                    step.description,
                    {
                        'input_fields': step.input_fields,
                        'output_fields': step.output_fields,
                    }
                )
            
            # Add target nodes
            for target_name in canonical.targets:
                self.add_transformation_node(
                    f"target_{target_name}",
                    "target",
                    target_name,
                    "Target system"
                )
            
            # Add data flows
            for flow in canonical.flows:
                field_mapping = {
                    flow.from_field: flow.to_field
                }
                self.add_data_flow_edge(
                    flow.from_instance,
                    flow.to_instance,
                    field_mapping
                )
    
    def get_node_info(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get information about a node"""
        if node_id in self.graph:
            return dict(self.graph.nodes[node_id])
        return None
    
    def get_predecessors(self, node_id: str) -> List[str]:
        """Get predecessors of a node"""
        if node_id in self.graph:
            return list(self.graph.predecessors(node_id))
        return []
    
    def get_successors(self, node_id: str) -> List[str]:
        """Get successors of a node"""
        if node_id in self.graph:
            return list(self.graph.successors(node_id))
        return []
    
    def get_path(self, source: str, target: str) -> Optional[List[str]]:
        """Get path between two nodes"""
        try:
            return nx.shortest_path(self.graph, source, target)
        except (nx.NodeNotFound, nx.NetworkXNoPath):
            return None
    
    def get_all_paths(self, source: str, target: str) -> List[List[str]]:
        """Get all paths between two nodes"""
        try:
            return list(nx.all_simple_paths(self.graph, source, target))
        except (nx.NodeNotFound, nx.NetworkXNoPath):
            return []
    
    def get_lineage(self, node_id: str) -> Dict[str, Any]:
        """Get complete lineage for a node"""
        if node_id not in self.graph:
            return {}
        
        return {
            'node': node_id,
            'ancestors': list(nx.ancestors(self.graph, node_id)),
            'descendants': list(nx.descendants(self.graph, node_id)),
            'predecessors': self.get_predecessors(node_id),
            'successors': self.get_successors(node_id),
        }
    
    def save_graph(self, output_path: str, format: str = "graphml") -> None:
        """Save graph to file"""
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        if format == "graphml":
            nx.write_graphml(self.graph, output_path)
        elif format == "gexf":
            nx.write_gexf(self.graph, output_path)
        elif format == "graphviz":
            # For graphviz, we'd need additional setup
            pass
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert graph to dictionary representation"""
        return {
            'nodes': [
                {
                    'id': node,
                    'data': dict(self.graph.nodes[node])
                }
                for node in self.graph.nodes()
            ],
            'edges': [
                {
                    'source': source,
                    'target': target,
                    'data': dict(data)
                }
                for source, target, data in self.graph.edges(data=True)
            ],
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get graph statistics"""
        return {
            'num_nodes': self.graph.number_of_nodes(),
            'num_edges': self.graph.number_of_edges(),
            'density': nx.density(self.graph),
            'is_dag': nx.is_directed_acyclic_graph(self.graph),
        }
