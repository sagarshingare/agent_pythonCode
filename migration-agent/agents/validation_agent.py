"""
ValidationAgent - Generates validation SQL queries
"""
from typing import Dict, Any, Optional, List
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from utils.helpers import ensure_dir, save_text
import os


class ValidationAgent(BaseAgent):
    """
    ValidationAgent generates SQL validation queries for:
    - Row count checks
    - EXCEPT queries
    - Hash comparisons
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("ValidationAgent", config)
    
    def execute(self, context: AgentContext) -> AgentResult:
        """Generate validation SQL"""
        try:
            self.log_step("Starting validation query generation")
            
            if not context.canonical_models:
                raise ValueError("No canonical models in context")
            
            validation_queries = []
            
            for canonical in context.canonical_models:
                self.log_step(f"Generating validation for", canonical.mapping_name)
                
                queries = self._generate_validation_queries(canonical)
                validation_queries.append(queries)
            
            # Combine all queries
            all_queries = '\n\n'.join(validation_queries)
            
            # Save to file
            output_dir = context.metadata.get('output_dir', 'output/sql')
            ensure_dir(output_dir)
            validation_file = os.path.join(output_dir, 'validation.sql')
            save_text(all_queries, validation_file)
            
            context.validation_queries = all_queries
            
            self.log_step("Validation queries generated", f"File: {validation_file}")
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'validation_file': validation_file,
                    'total_queries': len(validation_queries),
                    'query_types': ['row_count', 'except', 'hash'],
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in ValidationAgent: {str(e)}", exc_info=True)
            raise
    
    def _generate_validation_queries(self, canonical) -> str:
        """Generate validation queries for a mapping"""
        
        queries = []
        
        # Header
        queries.append(f'-- Validation Queries for: {canonical.mapping_name}')
        queries.append(f'-- Description: {canonical.mapping_description}')
        queries.append('')
        
        # Row count checks
        queries.extend(self._generate_row_count_checks(canonical))
        
        # Source to target except queries
        queries.extend(self._generate_except_queries(canonical))
        
        # Hash comparison queries
        queries.extend(self._generate_hash_comparisons(canonical))
        
        return '\n'.join(queries)
    
    def _generate_row_count_checks(self, canonical) -> List[str]:
        """Generate row count validation queries"""
        queries = [
            '-- Row Count Checks',
            '',
        ]
        
        for source in canonical.sources:
            queries.append(f'SELECT COUNT(*) as row_count FROM source.{source};')
        
        for target in canonical.targets:
            queries.append(f'SELECT COUNT(*) as row_count FROM target.{target};')
        
        queries.append('')
        return queries
    
    def _generate_except_queries(self, canonical) -> List[str]:
        """Generate EXCEPT queries for validation"""
        queries = [
            '-- Data Reconciliation (Source vs Target)',
            '',
        ]
        
        if canonical.sources and canonical.targets:
            source = canonical.sources[0]
            target = canonical.targets[0]
            
            # Build column lists
            source_schema = canonical.source_schemas.get(source, [])
            target_schema = canonical.target_schemas.get(target, [])
            
            source_cols = ', '.join([f.name for f in source_schema])
            target_cols = ', '.join([f.name for f in target_schema])
            
            queries.append(f'SELECT {source_cols} FROM source.{source}')
            queries.append('EXCEPT')
            queries.append(f'SELECT {target_cols} FROM target.{target};')
            queries.append('')
        
        return queries
    
    def _generate_hash_comparisons(self, canonical) -> List[str]:
        """Generate hash comparison queries"""
        queries = [
            '-- Hash Validation for Data Integrity',
            '',
        ]
        
        for target in canonical.targets:
            target_schema = canonical.target_schemas.get(target, [])
            
            if target_schema:
                cols = ', '.join([f.name for f in target_schema])
                queries.append(f'SELECT')
                queries.append(f'    MD5(CONCAT({cols})) as data_hash,')
                queries.append(f'    COUNT(*) as record_count')
                queries.append(f'FROM target.{target}')
                queries.append(f'GROUP BY data_hash;')
                queries.append('')
        
        return queries
