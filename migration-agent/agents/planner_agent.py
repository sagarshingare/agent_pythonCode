"""
Planner Agent - orchestrates the entire migration flow
"""
from typing import Dict, Any, List, Optional
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from dataclasses import dataclass
import json


@dataclass
class ExecutionPlan:
    """Execution plan for the migration"""
    steps: List[Dict[str, Any]]
    
    def to_list(self) -> List[Dict[str, Any]]:
        return self.steps
    
    def __str__(self) -> str:
        lines = []
        for i, step in enumerate(self.steps, 1):
            lines.append(f"Step {i}: {step['agent']} - {step['description']}")
        return "\n".join(lines)


class PlannerAgent(BaseAgent):
    """
    PlannerAgent orchestrates the entire migration flow.
    Decides which agents to call and in what order.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("PlannerAgent", config)
    
    def execute(self, context: AgentContext) -> AgentResult:
        """
        Create execution plan for Informatica migration
        """
        try:
            self.log_step("Creating execution plan for Informatica migration")
            
            # Define the execution plan
            execution_plan = self._create_execution_plan(context)
            
            self.log_step("Execution plan created")
            self.logger.info("\nExecution Plan:")
            self.logger.info(str(execution_plan))
            
            # Return plan as output
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'execution_plan': execution_plan.to_list(),
                    'total_steps': len(execution_plan.steps),
                    'agents_to_execute': [step['agent'] for step in execution_plan.steps],
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in PlannerAgent: {str(e)}", exc_info=True)
            raise
    
    def _create_execution_plan(self, context: AgentContext) -> ExecutionPlan:
        """
        Create a sequential execution plan
        
        The standard flow for Informatica migration is:
        1. SpecAgent: Parse XML -> JSON -> Canonical
        2. CodeGenAgent: Generate PySpark code
        3. ValidationAgent: Generate validation SQL
        4. OptimizationAgent: Apply optimizations
        5. OrchestrationAgent: Generate Airflow DAG
        6. DocumentationAgent: Generate documentation
        """
        steps = [
            {
                'step': 1,
                'agent': 'SpecAgent',
                'description': 'Parse Informatica XML export -> JSON -> Canonical Model',
                'inputs': ['xml_data'],
                'outputs': ['canonical_models', 'json_data'],
                'timeout': 120,
                'optional': False,
                'retry': 3,
            },
            {
                'step': 2,
                'agent': 'CodeGenAgent',
                'description': 'Generate PySpark code from canonical model',
                'inputs': ['canonical_models'],
                'outputs': ['generated_code'],
                'timeout': 180,
                'optional': False,
                'retry': 2,
            },
            {
                'step': 3,
                'agent': 'ValidationAgent',
                'description': 'Generate SQL validation queries (row counts, hashes, EXCEPT)',
                'inputs': ['canonical_models'],
                'outputs': ['validation_queries'],
                'timeout': 120,
                'optional': False,
                'retry': 2,
            },
            {
                'step': 4,
                'agent': 'OptimizationAgent',
                'description': 'Apply performance optimizations (repartition, cache, broadcast)',
                'inputs': ['generated_code', 'canonical_models'],
                'outputs': ['optimized_code'],
                'timeout': 120,
                'optional': True,
                'retry': 1,
            },
            {
                'step': 5,
                'agent': 'OrchestrationAgent',
                'description': 'Generate Airflow DAG for job orchestration',
                'inputs': ['canonical_models', 'generated_code'],
                'outputs': ['airflow_dag'],
                'timeout': 120,
                'optional': True,
                'retry': 1,
            },
            {
                'step': 6,
                'agent': 'DocumentationAgent',
                'description': 'Generate comprehensive documentation from XML and code',
                'inputs': ['canonical_models', 'generated_code'],
                'outputs': ['documentation'],
                'timeout': 120,
                'optional': True,
                'retry': 1,
            },
        ]
        
        # Conditionally add data generation agent if enabled
        if context.metadata.get('enable_data_generation', True):
            steps.append({
                'step': 7,
                'agent': 'DataGenerationAgent',
                'description': 'Generate sample input and output datasets',
                'inputs': ['canonical_models'],
                'outputs': ['input_dataset', 'output_dataset'],
                'timeout': 120,
                'optional': True,
                'retry': 1,
            })
        
        return ExecutionPlan(steps=steps)
    
    def get_next_agent(self, executed_agents: List[str], execution_plan: ExecutionPlan) -> Optional[str]:
        """Get the next agent to execute based on completed agents"""
        for step in execution_plan.steps:
            agent = step['agent']
            if agent not in executed_agents:
                # Check if dependencies are met
                dependencies_met = True
                for input_type in step.get('inputs', []):
                    # For now, assume sequential execution meets dependencies
                    pass
                
                if dependencies_met:
                    return agent
        
        return None
    
    def validate_execution_plan(self, execution_plan: ExecutionPlan) -> bool:
        """Validate that the execution plan is sound"""
        executed = set()
        
        for step in execution_plan.steps:
            agent = step['agent']
            inputs = step.get('inputs', [])
            outputs = step.get('outputs', [])
            
            # Check that all inputs are either initial or outputs of previous steps
            for input_type in inputs:
                if input_type != 'xml_data' and input_type not in executed:
                    self.logger.warning(f"Step {step['step']}: Input {input_type} not available")
            
            executed.update(outputs)
        
        return True
