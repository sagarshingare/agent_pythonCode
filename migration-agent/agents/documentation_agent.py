"""
DocumentationAgent - Generates comprehensive documentation
"""
from typing import Dict, Any, Optional, List
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from utils.helpers import ensure_dir, save_text
import os


class DocumentationAgent(BaseAgent):
    """
    DocumentationAgent generates comprehensive documentation from XML and code:
    - Mapping overview
    - Source -> Target lineage
    - Transformation logic explanation
    - Column-level mapping
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("DocumentationAgent", config)
    
    def execute(self, context: AgentContext) -> AgentResult:
        """Generate documentation"""
        try:
            self.log_step("Starting documentation generation")
            
            if not context.canonical_models:
                raise ValueError("No canonical models in context")
            
            # Generate documentation for each mapping
            all_docs = []
            output_files = []
            
            for canonical in context.canonical_models:
                self.log_step(f"Documenting mapping", canonical.mapping_name)
                doc = self._generate_mapping_documentation(canonical)
                all_docs.append(doc)
                
                # Save individual documentation
                output_dir = os.path.join(context.metadata.get('output_dir', 'output'), 'docs')
                ensure_dir(output_dir)
                doc_file = os.path.join(
                    output_dir,
                    f"{canonical.mapping_name.lower()}_doc.md"
                )
                save_text(doc, doc_file)
                output_files.append(doc_file)
            
            # Generate index documentation
            index_doc = self._generate_index_documentation(context.canonical_models, all_docs)
            
            # Save index
            output_dir = os.path.join(context.metadata.get('output_dir', 'output'), 'docs')
            index_file = os.path.join(output_dir, 'README.md')
            save_text(index_doc, index_file)
            output_files.append(index_file)
            
            context.documentation = index_doc
            
            self.log_step("Documentation generated", f"Files: {len(output_files)}")
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'documentation_files': output_files,
                    'index_file': index_file,
                    'total_mappings_documented': len(context.canonical_models),
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in DocumentationAgent: {str(e)}", exc_info=True)
            raise
    
    def _generate_mapping_documentation(self, canonical) -> str:
        """Generate documentation for a single mapping"""
        
        doc_lines = [
            f'# Mapping: {canonical.mapping_name}',
            '',
            f'**Description:** {canonical.mapping_description}',
            '',
            '## Overview',
            '',
            'This mapping is auto-generated from Informatica XML export.',
            '',
            '## Data Flow',
            '',
        ]
        
        # Source information
        doc_lines.append('### Sources')
        doc_lines.append('')
        for source in canonical.sources:
            doc_lines.append(f'- **{source}**')
            source_schema = canonical.source_schemas.get(source, [])
            doc_lines.append('  - Fields:')
            for field in source_schema:
                doc_lines.append(
                    f'    - {field.name} ({field.datatype})'
                    f'{" - " + field.description if field.description else ""}'
                )
            doc_lines.append('')
        
        # Target information
        doc_lines.append('### Targets')
        doc_lines.append('')
        for target in canonical.targets:
            doc_lines.append(f'- **{target}**')
            target_schema = canonical.target_schemas.get(target, [])
            doc_lines.append('  - Fields:')
            for field in target_schema:
                doc_lines.append(
                    f'    - {field.name} ({field.datatype})'
                    f'{" - " + field.description if field.description else ""}'
                )
            doc_lines.append('')
        
        # Transformation steps
        doc_lines.append('## Transformation Steps')
        doc_lines.append('')
        
        execution_order = self._get_execution_order(canonical)
        for i, step_id in enumerate(execution_order, 1):
            step = next((s for s in canonical.steps if s.id == step_id), None)
            if step:
                doc_lines.append(f'### Step {i}: {step.name}')
                doc_lines.append(f'**Type:** {step.type}')
                if step.description:
                    doc_lines.append(f'**Description:** {step.description}')
                
                if step.input_fields:
                    doc_lines.append('**Inputs:** ' + ', '.join(step.input_fields))
                
                if step.output_fields:
                    doc_lines.append('**Outputs:** ' + ', '.join(step.output_fields))
                
                if step.expression_logic:
                    doc_lines.append('**Expressions:**')
                    for field, expr in step.expression_logic.items():
                        doc_lines.append(f'- {field}: `{expr}`')
                
                if step.filter_condition:
                    doc_lines.append(f'**Filter:** `{step.filter_condition}`')
                
                doc_lines.append('')
        
        # Data lineage
        doc_lines.append('## Data Lineage')
        doc_lines.append('')
        doc_lines.append('```mermaid')
        doc_lines.append('graph LR')
        
        for source in canonical.sources:
            doc_lines.append(f'    {source}["Source: {source}"]')
        
        for step in canonical.steps:
            doc_lines.append(f'    {step.id}["{step.name}<br/>({step.type})"]')
        
        for target in canonical.targets:
            doc_lines.append(f'    {target}["Target: {target}"]')
        
        for flow in canonical.flows:
            doc_lines.append(f'    {flow.from_instance} --> {flow.to_instance}')
        
        doc_lines.append('```')
        doc_lines.append('')
        
        return '\n'.join(doc_lines)
    
    def _generate_index_documentation(self, canonical_models, all_docs: List[str]) -> str:
        """Generate index documentation"""
        
        doc_lines = [
            '# Informatica Migration Documentation',
            '',
            'Auto-generated documentation for Informatica to PySpark migration.',
            '',
            f'## Summary',
            '',
            f'- **Total Mappings:** {len(canonical_models)}',
            f'- **Generated Date:** 2024-01-01',
            f'- **Status:** Complete',
            '',
            '## Mappings',
            '',
        ]
        
        for i, canonical in enumerate(canonical_models, 1):
            doc_lines.append(
                f'{i}. [{canonical.mapping_name}]({canonical.mapping_name.lower()}_doc.md) '
                f'- {canonical.mapping_description}'
            )
        
        doc_lines.extend([
            '',
            '## Generated Artifacts',
            '',
            '- PySpark Code: `output/pyspark/main_job.py`',
            '- Validation SQL: `output/sql/validation.sql`',
            '- Airflow DAG: `output/dags/informatica_migration_dag.py`',
            '- Sample Data: `data/generated_*.csv`',
            '',
            '## Architecture',
            '',
            'The migration uses a multi-agent architecture:',
            '',
            '1. **SpecAgent** - Parses XML and builds canonical model',
            '2. **CodeGenAgent** - Generates PySpark code',
            '3. **ValidationAgent** - Creates validation SQL',
            '4. **OptimizationAgent** - Optimizes code performance',
            '5. **OrchestrationAgent** - Generates Airflow DAG',
            '6. **DocumentationAgent** - Creates documentation',
            '',
        ])
        
        return '\n'.join(doc_lines)
    
    def _get_execution_order(self, canonical) -> List[str]:
        """Get execution order for steps"""
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
            
            if not progress:
                break
        
        return order
