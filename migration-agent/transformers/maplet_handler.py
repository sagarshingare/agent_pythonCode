"""
Maplet handler - handles reusable maplet transformations
"""
from typing import Dict, List, Any, Optional


class MapletHandler:
    """Handle maplet (reusable transformation) expansion"""
    
    def __init__(self, maplets: Dict[str, Dict[str, Any]]):
        """Initialize with maplets"""
        self.maplets = maplets
    
    def expand_maplet(self, maplet_name: str, inputs: Dict[str, str]) -> str:
        """Expand a maplet into inline code"""
        
        if maplet_name not in self.maplets:
            raise ValueError(f"Maplet not found: {maplet_name}")
        
        maplet = self.maplets[maplet_name]
        
        # Generate maplet expansion code
        code = f"# Maplet: {maplet_name}\n"
        code += f"# Description: {maplet.get('description', '')}\n"
        code += f"# Inputs: {', '.join(inputs.keys())}\n\n"
        
        # Process maplet transformations
        transformations = maplet.get('transformations', {})
        instances = maplet.get('instances', [])
        connectors = maplet.get('connectors', [])
        
        code += "# Maplet transformations would be expanded here\n"
        
        return code
    
    def is_maplet_reference(self, instance_name: str) -> bool:
        """Check if an instance is a maplet reference"""
        return instance_name in self.maplets
    
    def get_maplet_inputs(self, maplet_name: str) -> List[str]:
        """Get input port names for a maplet"""
        if maplet_name not in self.maplets:
            return []
        
        maplet = self.maplets[maplet_name]
        instances = maplet.get('instances', [])
        
        input_ports = []
        for instance in instances:
            if instance['type'] == 'TRANSFORMATION':
                trans_data = maplet.get('transformations', {}).get(instance['name'], {})
                fields = trans_data.get('fields', [])
                for field in fields:
                    if field.get('porttype') == 'INPUT':
                        input_ports.append(field['name'])
        
        return input_ports
    
    def get_maplet_outputs(self, maplet_name: str) -> List[str]:
        """Get output port names for a maplet"""
        if maplet_name not in self.maplets:
            return []
        
        maplet = self.maplets[maplet_name]
        instances = maplet.get('instances', [])
        
        output_ports = []
        for instance in instances:
            if instance['type'] == 'TRANSFORMATION':
                trans_data = maplet.get('transformations', {}).get(instance['name'], {})
                fields = trans_data.get('fields', [])
                for field in fields:
                    if field.get('porttype') == 'OUTPUT':
                        output_ports.append(field['name'])
        
        return output_ports
