"""
Canonical model builder - converts JSON to normalized canonical format
"""
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, asdict, field
import json
from utils.logger import get_logger


logger = get_logger(__name__)


@dataclass
class FieldDefinition:
    """Field definition in canonical form"""
    name: str
    datatype: str
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: bool = True
    description: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class TransformationStep:
    """Single transformation step"""
    id: str
    name: str
    type: str  # Expression, Filter, Aggregator, Lookup, Join, etc.
    description: str = ""
    input_fields: List[str] = field(default_factory=list)
    output_fields: List[str] = field(default_factory=list)
    expression_logic: Dict[str, str] = field(default_factory=dict)  # field -> expression mapping
    filter_condition: Optional[str] = None
    join_keys: Optional[List[Tuple[str, str]]] = None
    group_by_fields: Optional[List[str]] = None
    aggregations: Dict[str, str] = field(default_factory=dict)  # field -> aggregation function
    lookup_table: Optional[str] = None
    lookup_condition: Optional[str] = None
    additional_params: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v}


@dataclass
class DataFlow:
    """Connection between transformation steps"""
    from_instance: str
    from_field: str
    to_instance: str
    to_field: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class CanonicalMapping:
    """Canonical representation of Informatica mapping"""
    mapping_name: str
    mapping_description: str = ""
    sources: List[str] = field(default_factory=list)  # source names
    targets: List[str] = field(default_factory=list)  # target names
    steps: List[TransformationStep] = field(default_factory=list)
    flows: List[DataFlow] = field(default_factory=list)
    dependencies: Dict[str, List[str]] = field(default_factory=dict)  # step_id -> [dependencies]
    source_schemas: Dict[str, List[FieldDefinition]] = field(default_factory=dict)
    target_schemas: Dict[str, List[FieldDefinition]] = field(default_factory=dict)
    variables: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'mapping_name': self.mapping_name,
            'mapping_description': self.mapping_description,
            'sources': self.sources,
            'targets': self.targets,
            'steps': [s.to_dict() for s in self.steps],
            'flows': [f.to_dict() for f in self.flows],
            'dependencies': self.dependencies,
            'source_schemas': {
                k: [f.to_dict() for f in v]
                for k, v in self.source_schemas.items()
            },
            'target_schemas': {
                k: [f.to_dict() for f in v]
                for k, v in self.target_schemas.items()
            },
            'variables': self.variables,
        }


class CanonicalModelBuilder:
    """Build canonical model from parsed XML/JSON"""
    
    def __init__(self, xml_data: Dict[str, Any]):
        """Initialize with parsed XML data"""
        self.xml_data = xml_data
        self.sources = xml_data.get('sources', {})
        self.targets = xml_data.get('targets', {})
        self.mappings = xml_data.get('mappings', {})
        self.transformations_global = xml_data.get('transformations', {})
        
    def build_canonical_mappings(self) -> List[CanonicalMapping]:
        """Build canonical models for all mappings"""
        canonical_mappings = []
        for mapping_name, mapping_data in self.mappings.items():
            logger.info(f"Building canonical model for mapping: {mapping_name}")
            canonical = self._build_single_mapping(mapping_name, mapping_data)
            canonical_mappings.append(canonical)
        logger.info(f"Built {len(canonical_mappings)} canonical mappings")
        return canonical_mappings
    
    def _build_single_mapping(self, mapping_name: str, mapping_data: Dict[str, Any]) -> CanonicalMapping:
        """Build canonical model for single mapping"""
        canonical = CanonicalMapping(
            mapping_name=mapping_name,
            mapping_description=mapping_data.get('description', ''),
        )
        
        # Parse instances to identify sources and targets
        instances = mapping_data.get('instances', [])
        instance_map = {inst['name']: inst for inst in instances}
        
        # Extract source and target names
        for instance in instances:
            inst_type = instance['type']
            inst_name = instance['name']
            
            if inst_type == 'SOURCE':
                canonical.sources.append(inst_name)
            elif inst_type == 'TARGET':
                canonical.targets.append(inst_name)
        
        # Load source and target schemas
        for source_name in canonical.sources:
            if source_name in self.sources:
                source_def = self.sources[source_name]
                fields = [
                    FieldDefinition(
                        name=f['name'],
                        datatype=f['datatype'],
                        precision=int(f.get('precision', 0)) or None,
                        scale=int(f.get('scale', 0)) or None,
                        nullable=f.get('nullable') != 'NOTNULL',
                        description=f.get('description', '')
                    )
                    for f in source_def.get('fields', [])
                ]
                canonical.source_schemas[source_name] = fields
        
        for target_name in canonical.targets:
            if target_name in self.targets:
                target_def = self.targets[target_name]
                fields = [
                    FieldDefinition(
                        name=f['name'],
                        datatype=f['datatype'],
                        precision=int(f.get('precision', 0)) or None,
                        scale=int(f.get('scale', 0)) or None,
                        nullable=f.get('nullable') != 'NOTNULL',
                        description=f.get('description', '')
                    )
                    for f in target_def.get('fields', [])
                ]
                canonical.target_schemas[target_name] = fields
        
        # Build transformation steps
        transformations = mapping_data.get('transformations', {})
        for trans_name, trans_data in transformations.items():
            step = self._build_transformation_step(trans_name, trans_data)
            canonical.steps.append(step)
        
        # Build data flows
        connectors = mapping_data.get('connectors', [])
        for connector in connectors:
            flow = DataFlow(
                from_instance=connector['frominstance'],
                from_field=connector['fromfield'],
                to_instance=connector['toinstance'],
                to_field=connector['tofield'],
            )
            canonical.flows.append(flow)
        
        # Build dependencies
        canonical.dependencies = self._build_dependencies(canonical)
        
        # Store variables
        canonical.variables = mapping_data.get('variables', {})
        
        logger.debug(f"Built canonical mapping with {len(canonical.steps)} steps and {len(canonical.flows)} flows")
        return canonical
    
    def _build_transformation_step(self, step_name: str, trans_data: Dict[str, Any]) -> TransformationStep:
        """Build transformation step"""
        step = TransformationStep(
            id=step_name,
            name=step_name,
            type=trans_data.get('type', ''),
            description=trans_data.get('description', ''),
        )
        
        # Extract input/output fields
        fields = trans_data.get('fields', [])
        for field_info in fields:
            port_type = field_info.get('porttype', '')
            field_name = field_info.get('name', '')
            
            if 'INPUT' in port_type or 'LOOKUP' in port_type:
                if field_name not in step.input_fields:
                    step.input_fields.append(field_name)
            
            if 'OUTPUT' in port_type or 'LOOKUP' in port_type:
                if field_name not in step.output_fields:
                    step.output_fields.append(field_name)
        
        # Extract expressions
        for expr_logic in trans_data.get('expression_logic', []):
            field_name = expr_logic['field']
            expression = expr_logic['expression']
            step.expression_logic[field_name] = expression
        
        # Extract filter condition
        step.filter_condition = trans_data.get('filter_condition')
        
        # Extract aggregations if present
        if trans_data.get('type') == 'Aggregator':
            for field_info in fields:
                expr = field_info.get('expression', '')
                if expr:
                    field_name = field_info['name']
                    step.aggregations[field_name] = expr
        
        # Extract lookup info
        if trans_data.get('type') == 'Lookup Procedure':
            attrs = trans_data.get('attributes', {})
            step.lookup_table = attrs.get('Lookup table name')
            step.lookup_condition = attrs.get('Lookup condition')
        
        # Extract Router groups
        if trans_data.get('type') == 'Router':
            groups = trans_data.get('groups', [])
            step.additional_params['router_groups'] = []
            for group in groups:
                step.additional_params['router_groups'].append({
                    'name': group.get('name', ''),
                    'expression': group.get('expression', ''),
                    'type': group.get('type', '')
                })
        
        # Extract Joiner info
        if trans_data.get('type') == 'Joiner':
            attrs = trans_data.get('attributes', {})
            step.additional_params['join_type'] = attrs.get('Join Type', 'Normal')
            step.additional_params['join_condition'] = attrs.get('Join Condition', '')
            step.additional_params['master_sort_order'] = attrs.get('Master Sort Order', '')
            step.additional_params['detail_sort_order'] = attrs.get('Detail Sort Order', '')
        
        # Extract Sequence info
        if trans_data.get('type') == 'Sequence':
            attrs = trans_data.get('attributes', {})
            step.additional_params['start_value'] = attrs.get('Start Value', '1')
            step.additional_params['increment_by'] = attrs.get('Increment By', '1')
            step.additional_params['end_value'] = attrs.get('End Value', '999999999')
            step.additional_params['current_value'] = attrs.get('Current Value', '1')
            step.additional_params['cycle'] = attrs.get('Cycle', 'NO')
            step.additional_params['reset'] = attrs.get('Reset', 'NO')
        
        # Extract Update Strategy info
        if trans_data.get('type') == 'Update Strategy':
            attrs = trans_data.get('attributes', {})
            step.additional_params['update_strategy_expression'] = attrs.get('Update Strategy Expression', '')
        
        return step
    
    def _build_dependencies(self, canonical: CanonicalMapping) -> Dict[str, List[str]]:
        """Build dependencies between steps"""
        dependencies = {step.id: [] for step in canonical.steps}
        
        for flow in canonical.flows:
            from_inst = flow.from_instance
            to_inst = flow.to_instance
            
            # Check if both are transformation steps
            from_step_ids = [s.id for s in canonical.steps if s.name == from_inst or s.id == from_inst]
            to_step_ids = [s.id for s in canonical.steps if s.name == to_inst or s.id == to_inst]
            
            for to_step_id in to_step_ids:
                for from_step_id in from_step_ids:
                    if to_step_id != from_step_id and from_step_id not in dependencies[to_step_id]:
                        dependencies[to_step_id].append(from_step_id)
        
        return dependencies
    
    def get_execution_order(self, canonical: CanonicalMapping) -> List[str]:
        """Get topological order for execution"""
        dependencies = canonical.dependencies
        executed = set()
        order = []
        
        while len(executed) < len(canonical.steps):
            progress = False
            for step in canonical.steps:
                if step.id not in executed:
                    step_deps = dependencies.get(step.id, [])
                    if all(dep in executed for dep in step_deps):
                        order.append(step.id)
                        executed.add(step.id)
                        progress = True
            
            if not progress and len(executed) < len(canonical.steps):
                raise RuntimeError("Circular dependency detected in mapping")
        
        return order


def build_canonical_model(xml_data: Dict[str, Any]) -> Tuple[List[CanonicalMapping], Dict[str, Any]]:
    """Build canonical models from parsed XML data"""
    builder = CanonicalModelBuilder(xml_data)
    canonical_mappings = builder.build_canonical_mappings()
    
    # Create summary
    summary = {
        'total_mappings': len(canonical_mappings),
        'mappings': [m.mapping_name for m in canonical_mappings],
        'total_sources': len(builder.sources),
        'total_targets': len(builder.targets),
        'sources': list(builder.sources.keys()),
        'targets': list(builder.targets.keys()),
    }
    
    return canonical_mappings, summary
