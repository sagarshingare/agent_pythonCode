"""
Helper utilities for the Informatica Migration Agent
"""
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
import json
from pathlib import Path


def ensure_dir(path: str) -> str:
    """Ensure directory exists and return path"""
    Path(path).mkdir(parents=True, exist_ok=True)
    return path


def save_json(data: Any, output_path: str) -> None:
    """Save data to JSON file"""
    ensure_dir(Path(output_path).parent)
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)


def load_json(path: str) -> Any:
    """Load data from JSON file"""
    with open(path, 'r') as f:
        return json.load(f)


def save_text(content: str, output_path: str) -> None:
    """Save text to file"""
    ensure_dir(Path(output_path).parent)
    with open(output_path, 'w') as f:
        f.write(content)


def load_text(path: str) -> str:
    """Load text from file"""
    with open(path, 'r') as f:
        return f.read()


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
    """Flatten nested dictionary"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dictionaries"""
    result = {}
    for d in dicts:
        result.update(d)
    return result


def get_nested(d: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """Get nested dictionary value using dot notation"""
    keys = key_path.split('.')
    value = d
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key, default)
        else:
            return default
    return value


def timestamp_str() -> str:
    """Get current timestamp as string"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def format_sql(sql: str, indent: int = 0) -> str:
    """Format SQL query with indentation"""
    lines = sql.strip().split('\n')
    formatted = []
    for line in lines:
        formatted.append('  ' * indent + line.strip())
    return '\n'.join(formatted)


def generate_class_name(name: str) -> str:
    """Convert name to PascalCase for class name"""
    return ''.join(word.capitalize() for word in name.split('_'))


def generate_var_name(name: str) -> str:
    """Convert name to snake_case for variable name"""
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def sanitize_name(name: str) -> str:
    """Sanitize name for use in code"""
    import re
    # Replace non-alphanumeric with underscore
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Ensure doesn't start with number
    if name and name[0].isdigit():
        name = '_' + name
    # Ensure it's not empty
    return name or 'item'


def batch_items(items: List[Any], batch_size: int) -> List[List[Any]]:
    """Batch items into groups"""
    batches = []
    for i in range(0, len(items), batch_size):
        batches.append(items[i:i + batch_size])
    return batches
