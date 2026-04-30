"""
OrchestrationAgent - Generates Airflow DAG
"""
from typing import Dict, Any, Optional
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from utils.helpers import ensure_dir, save_text, generate_class_name
import os


class OrchestrationAgent(BaseAgent):
    """
    OrchestrationAgent generates Apache Airflow DAG for job orchestration
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("OrchestrationAgent", config)
    
    def execute(self, context: AgentContext) -> AgentResult:
        """Generate Airflow DAG"""
        try:
            self.log_step("Starting DAG generation")
            
            if not context.canonical_models:
                raise ValueError("No canonical models in context")
            
            # Generate DAG code
            dag_code = self._generate_airflow_dag(context.canonical_models)
            context.airflow_dag = dag_code
            
            # Save DAG file
            output_dir = os.path.join(context.metadata.get('output_dir', 'output'), 'dags')
            ensure_dir(output_dir)
            dag_file = os.path.join(output_dir, 'informatica_migration_dag.py')
            save_text(dag_code, dag_file)
            
            self.log_step("DAG generated", f"File: {dag_file}")
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'dag_file': dag_file,
                    'mappings_count': len(context.canonical_models),
                    'dag_id': 'informatica_migration_dag',
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in OrchestrationAgent: {str(e)}", exc_info=True)
            raise
    
    def _generate_airflow_dag(self, canonical_models) -> str:
        """Generate Airflow DAG code"""
        
        code_lines = [
            '"""',
            'Auto-generated Apache Airflow DAG for Informatica Migration',
            '"""',
            '',
            'from datetime import datetime, timedelta',
            'from airflow import DAG',
            'from airflow.operators.bash import BashOperator',
            'from airflow.operators.python import PythonOperator',
            'from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator',
            'from airflow.utils.trigger_rule import TriggerRule',
            '',
            'default_args = {',
            '    "owner": "data_engineering",',
            '    "retries": 3,',
            '    "retry_delay": timedelta(minutes=5),',
            '    "start_date": datetime(2024, 1, 1),',
            '}',
            '',
            'dag = DAG(',
            '    "informatica_migration_dag",',
            '    default_args=default_args,',
            '    schedule_interval="@daily",',
            '    catchup=False,',
            '    tags=["informatica", "migration", "pyspark"],',
            ')',
            '',
            '# Tasks',
            '',
        ]
        
        # Create tasks for each mapping
        for i, canonical in enumerate(canonical_models):
            task_id = f"task_{canonical.mapping_name.lower()}"
            class_name = generate_class_name(canonical.mapping_name)
            
            code_lines.append(f'{task_id} = SparkSubmitOperator(')
            code_lines.append(f'    task_id="{task_id}",')
            code_lines.append(f'    application="pyspark/job.py",')
            code_lines.append(f'    conf={{"spark.app.name": "{canonical.mapping_name}"}},')
            code_lines.append(f'    dag=dag,')
            code_lines.append(f')')
            code_lines.append('')
            
            # Add dependencies between tasks
            if i > 0:
                prev_task_id = f"task_{canonical_models[i-1].mapping_name.lower()}"
                code_lines.append(f'{prev_task_id} >> {task_id}')
                code_lines.append('')
        
        code_lines.extend([
            '# SLA and Alerts',
            'task_success_alert = PythonOperator(',
            '    task_id="success_alert",',
            '    python_callable=lambda: print("Migration job completed successfully"),',
            '    dag=dag,',
            '    trigger_rule=TriggerRule.ALL_SUCCESS,',
            ')',
            '',
            'task_failure_alert = PythonOperator(',
            '    task_id="failure_alert",',
            '    python_callable=lambda: print("Migration job failed"),',
            '    dag=dag,',
            '    trigger_rule=TriggerRule.ONE_FAILED,',
            ')',
        ])
        
        return '\n'.join(code_lines)
