"""
Memory store for persisting agent state and learning
"""
from typing import Dict, List, Any, Optional
from utils.helpers import save_json, load_json
from pathlib import Path
from datetime import datetime


class MemoryStore:
    """Store for agent memory and learning"""
    
    def __init__(self, store_path: str = "rag/memory_store.json"):
        """Initialize memory store"""
        self.store_path = store_path
        self.data = self._load_or_create()
    
    def _load_or_create(self) -> Dict[str, Any]:
        """Load existing store or create new one"""
        if Path(self.store_path).exists():
            return load_json(self.store_path)
        
        return {
            'execution_history': [],
            'learned_patterns': [],
            'error_log': [],
            'optimization_suggestions': [],
            'mapping_cache': {},
        }
    
    def add_execution_record(self, mapping_name: str, execution_details: Dict[str, Any]) -> None:
        """Add execution record"""
        record = {
            'timestamp': datetime.now().isoformat(),
            'mapping_name': mapping_name,
            'details': execution_details,
        }
        self.data['execution_history'].append(record)
        self.save()
    
    def add_learned_pattern(self, pattern_name: str, pattern_details: Dict[str, Any]) -> None:
        """Add learned pattern"""
        pattern = {
            'timestamp': datetime.now().isoformat(),
            'pattern_name': pattern_name,
            'details': pattern_details,
        }
        self.data['learned_patterns'].append(pattern)
        self.save()
    
    def add_error_record(self, error_msg: str, context: Dict[str, Any]) -> None:
        """Add error record"""
        error = {
            'timestamp': datetime.now().isoformat(),
            'error': error_msg,
            'context': context,
        }
        self.data['error_log'].append(error)
        self.save()
    
    def add_optimization_suggestion(self, suggestion: str, reason: str, impact: str) -> None:
        """Add optimization suggestion"""
        suggestion_record = {
            'timestamp': datetime.now().isoformat(),
            'suggestion': suggestion,
            'reason': reason,
            'impact': impact,
        }
        self.data['optimization_suggestions'].append(suggestion_record)
        self.save()
    
    def cache_mapping(self, mapping_name: str, mapping_data: Dict[str, Any]) -> None:
        """Cache mapping data for reuse"""
        self.data['mapping_cache'][mapping_name] = {
            'timestamp': datetime.now().isoformat(),
            'data': mapping_data,
        }
        self.save()
    
    def get_cached_mapping(self, mapping_name: str) -> Optional[Dict[str, Any]]:
        """Get cached mapping"""
        if mapping_name in self.data['mapping_cache']:
            return self.data['mapping_cache'][mapping_name]['data']
        return None
    
    def get_execution_history(self, mapping_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get execution history"""
        history = self.data['execution_history']
        if mapping_name:
            history = [h for h in history if h['mapping_name'] == mapping_name]
        return history
    
    def get_learned_patterns(self) -> List[Dict[str, Any]]:
        """Get all learned patterns"""
        return self.data['learned_patterns']
    
    def get_error_log(self) -> List[Dict[str, Any]]:
        """Get error log"""
        return self.data['error_log']
    
    def get_optimization_suggestions(self) -> List[Dict[str, Any]]:
        """Get optimization suggestions"""
        return self.data['optimization_suggestions']
    
    def save(self) -> None:
        """Save memory store"""
        Path(self.store_path).parent.mkdir(parents=True, exist_ok=True)
        save_json(self.data, self.store_path)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return self.data
