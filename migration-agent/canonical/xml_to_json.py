"""
XML to JSON converter
Converts Informatica XML to JSON format maintaining hierarchy
"""
import json
from typing import Dict, Any
from pathlib import Path
from utils.logger import get_logger


logger = get_logger(__name__)


def xml_to_json(xml_data: Dict[str, Any]) -> str:
    """Convert parsed XML data to formatted JSON string"""
    return json.dumps(xml_data, indent=2, default=str)


def save_json(data: Dict[str, Any], output_path: str) -> None:
    """Save data as JSON file"""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2, default=str)
    logger.info(f"Saved JSON to {output_path}")


def load_json(input_path: str) -> Dict[str, Any]:
    """Load JSON file"""
    with open(input_path, 'r') as f:
        data = json.load(f)
    logger.info(f"Loaded JSON from {input_path}")
    return data
