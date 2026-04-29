"""
SpecAgent - Converts XML -> JSON -> Canonical Model
"""
from typing import Dict, Any, Optional
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from ingestion.xml_parser import parse_informatica_xml
from canonical.model_builder import build_canonical_model
from canonical.xml_to_json import xml_to_json, save_json
from utils.helpers import ensure_dir
import os


class SpecAgent(BaseAgent):
    """
    SpecAgent handles the specification phase:
    - Parse Informatica XML
    - Convert to JSON
    - Build canonical model
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("SpecAgent", config)
    
    def execute(self, context: AgentContext) -> AgentResult:
        """Execute specification phase"""
        try:
            self.log_step("Starting SpecAgent")
            
            # Get XML file path
            xml_path = context.metadata.get('xml_path', 'data/sample_informatic_export.xml')
            self.log_step("Parsing XML", f"File: {xml_path}")
            
            # Parse XML
            xml_data = parse_informatica_xml(xml_path)
            context.xml_data = xml_data
            
            # Convert to JSON
            self.log_step("Converting to JSON")
            json_str = xml_to_json(xml_data)
            
            # Save JSON intermediate
            output_dir = context.metadata.get('output_dir', 'output')
            ensure_dir(output_dir)
            json_path = os.path.join(output_dir, 'informatica_export.json')
            save_json(xml_data, json_path)
            self.log_step("Saved JSON", f"Path: {json_path}")
            
            # Build canonical model
            self.log_step("Building canonical model")
            canonical_models, summary = build_canonical_model(xml_data)
            context.canonical_models = canonical_models
            
            # Log summary
            self.log_step("Canonical model built", f"Mappings: {summary['total_mappings']}")
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'xml_data_summary': {
                        'sources': len(xml_data.get('sources', {})),
                        'targets': len(xml_data.get('targets', {})),
                        'mappings': len(xml_data.get('mappings', {})),
                    },
                    'canonical_model_summary': summary,
                    'mappings': [m.mapping_name for m in canonical_models],
                    'json_file': json_path,
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in SpecAgent: {str(e)}", exc_info=True)
            raise
