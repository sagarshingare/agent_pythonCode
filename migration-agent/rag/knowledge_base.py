"""
Knowledge base for storing transformation rules and patterns
"""
from typing import Dict, List, Any, Optional
from utils.helpers import save_json, load_json
from pathlib import Path
import json


class KnowledgeBase:
    """Knowledge base for storing transformation rules"""
    
    def __init__(self, kb_path: str = "rag/knowledge_base.json"):
        """Initialize knowledge base"""
        self.kb_path = kb_path
        self.data = self._load_or_create()
    
    def _load_or_create(self) -> Dict[str, Any]:
        """Load existing KB or create new one"""
        if Path(self.kb_path).exists():
            return load_json(self.kb_path)
        
        return self._create_default_kb()
    
    def _create_default_kb(self) -> Dict[str, Any]:
        """Create default knowledge base"""
        return {
            'transformation_rules': self._get_default_transformation_rules(),
            'function_mappings': self._get_default_function_mappings(),
            'patterns': self._get_default_patterns(),
            'best_practices': self._get_default_best_practices(),
        }
    
    def _get_default_transformation_rules(self) -> Dict[str, Dict[str, str]]:
        """Default transformation rules"""
        return {
            'expression': {
                'IIF': 'Use F.when().otherwise() for conditional logic',
                'NVL': 'Use F.coalesce() for null handling',
                'DECODE': 'Use F.when().otherwise() for switch logic',
            },
            'filter': {
                'simple': 'Use df.filter() for single conditions',
                'complex': 'Chain multiple filter() calls for AND conditions',
            },
            'aggregation': {
                'simple': 'Use agg() for ungrouped aggregations',
                'grouped': 'Use groupBy().agg() for grouped aggregations',
            },
        }
    
    def _get_default_function_mappings(self) -> Dict[str, str]:
        """Default function mappings"""
        return {
            'IIF': 'F.when',
            'NVL': 'F.coalesce',
            'DECODE': 'F.when',
            'TO_CHAR': 'F.cast',
            'TO_DATE': 'F.to_date',
            'UPPER': 'F.upper',
            'LOWER': 'F.lower',
            'SUBSTR': 'F.substring',
            'LENGTH': 'F.length',
            'ROUND': 'F.round',
            'COUNT': 'F.count',
            'SUM': 'F.sum',
            'AVG': 'F.avg',
        }
    
    def _get_default_patterns(self) -> List[Dict[str, str]]:
        """Default transformation patterns"""
        return [
            {
                'name': 'row_count_check',
                'description': 'Validate row counts match between source and target',
                'implementation': 'source.count() == target.count()',
            },
            {
                'name': 'null_handling',
                'description': 'Handle null values in transformations',
                'implementation': 'F.coalesce(col, default_value)',
            },
            {
                'name': 'data_type_casting',
                'description': 'Cast between data types',
                'implementation': 'F.cast(col, desired_type)',
            },
            {
                'name': 'string_operations',
                'description': 'Perform string operations',
                'implementation': 'F.upper, F.lower, F.trim, F.substring',
            },
        ]
    
    def _get_default_best_practices(self) -> List[str]:
        """Default best practices"""
        return [
            'Cache large DataFrames that are used multiple times',
            'Repartition data for join operations',
            'Use broadcast joins for small DataFrames',
            'Avoid wide transformations when possible',
            'Use columnar storage for large datasets',
            'Monitor job execution and optimize bottlenecks',
        ]
    
    def add_transformation_rule(self, category: str, rule_name: str, description: str) -> None:
        """Add new transformation rule"""
        if 'transformation_rules' not in self.data:
            self.data['transformation_rules'] = {}
        
        if category not in self.data['transformation_rules']:
            self.data['transformation_rules'][category] = {}
        
        self.data['transformation_rules'][category][rule_name] = description
        self.save()
    
    def add_function_mapping(self, informatica_func: str, pyspark_func: str) -> None:
        """Add new function mapping"""
        if 'function_mappings' not in self.data:
            self.data['function_mappings'] = {}
        
        self.data['function_mappings'][informatica_func] = pyspark_func
        self.save()
    
    def add_pattern(self, name: str, description: str, implementation: str) -> None:
        """Add new pattern"""
        if 'patterns' not in self.data:
            self.data['patterns'] = []
        
        self.data['patterns'].append({
            'name': name,
            'description': description,
            'implementation': implementation,
        })
        self.save()
    
    def get_transformation_rule(self, category: str, rule_name: str) -> Optional[str]:
        """Get transformation rule"""
        if category in self.data.get('transformation_rules', {}):
            return self.data['transformation_rules'][category].get(rule_name)
        return None
    
    def get_function_mapping(self, informatica_func: str) -> Optional[str]:
        """Get function mapping"""
        return self.data.get('function_mappings', {}).get(informatica_func)
    
    def search_patterns(self, keyword: str) -> List[Dict[str, str]]:
        """Search patterns by keyword"""
        results = []
        for pattern in self.data.get('patterns', []):
            if keyword.lower() in pattern['name'].lower() or \
               keyword.lower() in pattern['description'].lower():
                results.append(pattern)
        return results
    
    def save(self) -> None:
        """Save knowledge base"""
        Path(self.kb_path).parent.mkdir(parents=True, exist_ok=True)
        save_json(self.data, self.kb_path)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.data
