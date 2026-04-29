#!/usr/bin/env python3
"""
Main orchestrator for Informatica to PySpark migration
Multi-agent system orchestration with RAG and knowledge graph support
"""

import sys
import os
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from agents.base_agent import AgentContext, AgentStatus
from agents.planner_agent import PlannerAgent
from agents.spec_agent import SpecAgent
from agents.codegen_agent import CodeGenAgent
from agents.validation_agent import ValidationAgent
from agents.optimization_agent import OptimizationAgent
from agents.orchestration_agent import OrchestrationAgent
from agents.documentation_agent import DocumentationAgent
from rag.knowledge_base import KnowledgeBase
from rag.memory_store import MemoryStore
from knowledge_graph.graph_builder import KnowledgeGraphBuilder
from utils.logger import get_logger
from utils.helpers import save_json, ensure_dir, timestamp_str


class InformaticaMigrationOrchestrator:
    """
    Main orchestrator for Informatica to PySpark migration
    Manages multi-agent execution flow
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize orchestrator"""
        self.config = config or {}
        self.logger = get_logger("Orchestrator")
        self.agents = {}
        self.execution_results = {}
        self.context = AgentContext()
        
        # Initialize RAG systems
        self.knowledge_base = KnowledgeBase()
        self.memory_store = MemoryStore()
        self.graph_builder = KnowledgeGraphBuilder()
        
        # Setup directories
        self.output_dir = self.config.get('output_dir', 'output')
        self.data_dir = self.config.get('data_dir', 'data')
        ensure_dir(self.output_dir)
        ensure_dir(self.data_dir)
        
        # Setup logger
        log_file = os.path.join(self.output_dir, f'migration_{timestamp_str()}.log')
        self.logger = get_logger("Orchestrator", log_file)
    
    def initialize_agents(self) -> None:
        """Initialize all agents"""
        self.logger.info("Initializing agents...")
        
        self.agents = {
            'planner': PlannerAgent(self.config),
            'spec': SpecAgent(self.config),
            'codegen': CodeGenAgent(self.config),
            'validation': ValidationAgent(self.config),
            'optimization': OptimizationAgent(self.config),
            'orchestration': OrchestrationAgent(self.config),
            'documentation': DocumentationAgent(self.config),
        }
        
        self.logger.info(f"Initialized {len(self.agents)} agents")
    
    def setup_context(self, xml_path: str) -> None:
        """Setup initial context"""
        self.context.metadata = {
            'xml_path': xml_path,
            'output_dir': self.output_dir,
            'data_dir': self.data_dir,
            'enable_data_generation': True,
            'start_time': datetime.now().isoformat(),
        }
    
    def run_planning_phase(self) -> bool:
        """Run planning phase"""
        self.logger.info("=" * 80)
        self.logger.info("PLANNING PHASE")
        self.logger.info("=" * 80)
        
        planner = self.agents['planner']
        result = planner.run(self.context)
        self.execution_results['planner'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Planning phase completed successfully")
            self.logger.info(f"Execution plan: {len(result.output['execution_plan'])} steps")
            return True
        else:
            self.logger.error(f"Planning phase failed: {result.error}")
            return False
    
    def run_spec_phase(self) -> bool:
        """Run specification phase"""
        self.logger.info("=" * 80)
        self.logger.info("SPECIFICATION PHASE")
        self.logger.info("=" * 80)
        
        spec = self.agents['spec']
        result = spec.run(self.context)
        self.execution_results['spec'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Specification phase completed successfully")
            self.logger.info(f"Summary: {result.output}")
            return True
        else:
            self.logger.error(f"Specification phase failed: {result.error}")
            return False
    
    def run_codegen_phase(self) -> bool:
        """Run code generation phase"""
        self.logger.info("=" * 80)
        self.logger.info("CODE GENERATION PHASE")
        self.logger.info("=" * 80)
        
        codegen = self.agents['codegen']
        result = codegen.run(self.context)
        self.execution_results['codegen'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Code generation phase completed successfully")
            self.logger.info(f"Generated files: {result.output}")
            return True
        else:
            self.logger.error(f"Code generation phase failed: {result.error}")
            return False
    
    def run_validation_phase(self) -> bool:
        """Run validation phase"""
        self.logger.info("=" * 80)
        self.logger.info("VALIDATION PHASE")
        self.logger.info("=" * 80)
        
        validation = self.agents['validation']
        result = validation.run(self.context)
        self.execution_results['validation'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Validation phase completed successfully")
            self.logger.info(f"Output: {result.output}")
            return True
        else:
            self.logger.error(f"Validation phase failed: {result.error}")
            return False
    
    def run_optimization_phase(self) -> bool:
        """Run optimization phase"""
        self.logger.info("=" * 80)
        self.logger.info("OPTIMIZATION PHASE")
        self.logger.info("=" * 80)
        
        optimization = self.agents['optimization']
        result = optimization.run(self.context)
        self.execution_results['optimization'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Optimization phase completed successfully")
            self.logger.info(f"Output: {result.output}")
            return True
        else:
            self.logger.warning(f"Optimization phase skipped: {result.error}")
            return True  # Don't fail on optimization
    
    def run_orchestration_phase(self) -> bool:
        """Run orchestration phase"""
        self.logger.info("=" * 80)
        self.logger.info("ORCHESTRATION PHASE")
        self.logger.info("=" * 80)
        
        orchestration = self.agents['orchestration']
        result = orchestration.run(self.context)
        self.execution_results['orchestration'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Orchestration phase completed successfully")
            self.logger.info(f"Output: {result.output}")
            return True
        else:
            self.logger.warning(f"Orchestration phase skipped: {result.error}")
            return True  # Don't fail on orchestration
    
    def run_documentation_phase(self) -> bool:
        """Run documentation phase"""
        self.logger.info("=" * 80)
        self.logger.info("DOCUMENTATION PHASE")
        self.logger.info("=" * 80)
        
        documentation = self.agents['documentation']
        result = documentation.run(self.context)
        self.execution_results['documentation'] = result
        
        if result.status == AgentStatus.COMPLETED:
            self.logger.info("Documentation phase completed successfully")
            self.logger.info(f"Output: {result.output}")
            return True
        else:
            self.logger.warning(f"Documentation phase skipped: {result.error}")
            return True  # Don't fail on documentation
    
    def build_knowledge_graph(self) -> None:
        """Build knowledge graph from canonical models"""
        self.logger.info("Building knowledge graph...")
        
        if self.context.canonical_models:
            try:
                self.graph_builder.build_from_canonical(self.context.canonical_models)
                self.logger.info(f"Knowledge graph built: {self.graph_builder.get_statistics()}")
                
                # Save graph
                graph_path = os.path.join(self.output_dir, 'transformation_graph.graphml')
                self.graph_builder.save_graph(graph_path)
                self.logger.info(f"Graph saved to {graph_path}")
            except Exception as e:
                self.logger.warning(f"Knowledge graph building failed: {str(e)}")
    
    def generate_sample_data(self) -> None:
        """Generate sample input and output datasets"""
        self.logger.info("Generating sample data...")
        
        if self.context.canonical_models:
            for canonical in self.context.canonical_models:
                # Generate input data based on source schema
                input_data = []
                if canonical.source_schemas:
                    for source_name, fields in canonical.source_schemas.items():
                        row = {}
                        for field in fields:
                            # Generate sample data based on type
                            if 'string' in field.datatype.lower():
                                row[field.name] = f"sample_{field.name}"
                            elif 'number' in field.datatype.lower() or 'int' in field.datatype.lower():
                                row[field.name] = 123
                            elif 'date' in field.datatype.lower():
                                row[field.name] = "2024-01-01"
                            else:
                                row[field.name] = None
                        input_data.append(row)
                
                # Save sample data
                input_file = os.path.join(self.data_dir, 'generated_input.csv')
                if input_data:
                    import csv
                    with open(input_file, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=input_data[0].keys())
                        writer.writeheader()
                        writer.writerows(input_data)
                    self.logger.info(f"Sample input data saved to {input_file}")
    
    def print_execution_summary(self) -> None:
        """Print execution summary"""
        self.logger.info("=" * 80)
        self.logger.info("EXECUTION SUMMARY")
        self.logger.info("=" * 80)
        
        for agent_name, result in self.execution_results.items():
            status = result.status.value
            exec_time = result.execution_time_ms
            self.logger.info(f"{agent_name:20s} | Status: {status:10s} | Time: {exec_time}ms")
        
        # Overall status
        failed_agents = [
            name for name, result in self.execution_results.items()
            if result.status != AgentStatus.COMPLETED
        ]
        
        if failed_agents:
            self.logger.warning(f"Failed agents: {', '.join(failed_agents)}")
        else:
            self.logger.info("All agents completed successfully!")
        
        # Save summary
        summary = {
            'start_time': self.context.metadata.get('start_time'),
            'end_time': datetime.now().isoformat(),
            'agents': {
                name: result.to_dict()
                for name, result in self.execution_results.items()
            },
            'outputs': {
                'canonical_models': len(self.context.canonical_models or []),
                'generated_code_lines': len(self.context.generated_code.split('\n')) if self.context.generated_code else 0,
                'validation_queries': len(self.context.validation_queries.split('\n')) if self.context.validation_queries else 0,
            },
        }
        
        summary_file = os.path.join(self.output_dir, 'execution_summary.json')
        save_json(summary, summary_file)
        self.logger.info(f"Summary saved to {summary_file}")
    
    def run(self, xml_path: str) -> bool:
        """
        Run the complete migration pipeline
        
        Args:
            xml_path: Path to Informatica XML export file
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.info("=" * 80)
        self.logger.info("INFORMATICA TO PYSPARK MIGRATION SYSTEM")
        self.logger.info(f"Start Time: {datetime.now()}")
        self.logger.info("=" * 80)
        
        try:
            # Initialize
            self.initialize_agents()
            self.setup_context(xml_path)
            
            # Run phases
            phases = [
                ('Planning', self.run_planning_phase),
                ('Specification', self.run_spec_phase),
                ('Code Generation', self.run_codegen_phase),
                ('Validation', self.run_validation_phase),
                ('Optimization', self.run_optimization_phase),
                ('Orchestration', self.run_orchestration_phase),
                ('Documentation', self.run_documentation_phase),
            ]
            
            all_success = True
            for phase_name, phase_func in phases:
                try:
                    if not phase_func():
                        all_success = False
                        if phase_name in ['Planning', 'Specification', 'Code Generation']:
                            # Critical phases
                            self.logger.error(f"Critical phase {phase_name} failed. Aborting.")
                            return False
                except Exception as e:
                    self.logger.error(f"Exception in {phase_name}: {str(e)}", exc_info=True)
                    if phase_name in ['Planning', 'Specification', 'Code Generation']:
                        return False
            
            # Post-processing
            self.build_knowledge_graph()
            self.generate_sample_data()
            
            # Print summary
            self.print_execution_summary()
            
            self.logger.info("=" * 80)
            self.logger.info("MIGRATION COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 80)
            
            return all_success
            
        except Exception as e:
            self.logger.error(f"Fatal error during migration: {str(e)}", exc_info=True)
            return False


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Informatica to PySpark Migration System")
    parser.add_argument(
        '--xml',
        type=str,
        default='data/sample_informatic_export.xml',
        help='Path to Informatica XML export file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='output',
        help='Output directory for generated artifacts'
    )
    parser.add_argument(
        '--data',
        type=str,
        default='data',
        help='Data directory for generated datasets'
    )
    
    args = parser.parse_args()
    
    config = {
        'output_dir': args.output,
        'data_dir': args.data,
    }
    
    orchestrator = InformaticaMigrationOrchestrator(config)
    success = orchestrator.run(args.xml)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
