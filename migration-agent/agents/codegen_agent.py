"""
CodeGenAgent - Generates PySpark code from canonical model
"""
from typing import Dict, Any, Optional, List
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from transformers.expression import ExpressionTransformer
from transformers.join import JoinTransformer
from transformers.aggregator import AggregatorTransformer
from transformers.maplet_handler import MapletHandler
from utils.helpers import ensure_dir, save_text, generate_class_name
import os


class CodeGenAgent(BaseAgent):
    """
    CodeGenAgent generates production-grade PySpark code
    from canonical transformation model
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("CodeGenAgent", config)
        self.expr_transformer = ExpressionTransformer()
        self.join_transformer = JoinTransformer()
        self.agg_transformer = AggregatorTransformer()
    
    def execute(self, context: AgentContext) -> AgentResult:
        """Generate PySpark code"""
        try:
            self.log_step("Starting code generation")
            
            if not context.canonical_models:
                raise ValueError("No canonical models in context")
            
            # Generate code for each mapping
            all_code = []
            output_files = []
            
            for i, canonical in enumerate(context.canonical_models):
                self.log_step(f"Generating code for mapping", canonical.mapping_name)
                code = self._generate_mapping_code(canonical)
                all_code.append(code)
                
                # Save individual mapping code
                output_dir = os.path.join(context.metadata.get('output_dir', 'output'), 'pyspark')
                ensure_dir(output_dir)
                mapping_file = os.path.join(
                    output_dir,
                    f"{canonical.mapping_name.lower()}.py"
                )
                save_text(code, mapping_file)
                output_files.append(mapping_file)
            
            # Generate main orchestrator code
            main_code = self._generate_main_code(context.canonical_models, all_code)
            
            # Save main code
            output_dir = os.path.join(context.metadata.get('output_dir', 'output'), 'pyspark')
            main_file = os.path.join(output_dir, 'main_job.py')
            save_text(main_code, main_file)
            output_files.append(main_file)
            
            context.generated_code = main_code
            
            self.log_step("Code generation completed", f"Files: {len(output_files)}")
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'generated_files': output_files,
                    'main_job_file': main_file,
                    'individual_mapping_files': output_files[:-1],
                    'total_lines': sum(len(code.split('\n')) for code in all_code),
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in CodeGenAgent: {str(e)}", exc_info=True)
            raise
    
    def _generate_mapping_code(self, canonical) -> str:
        """Generate PySpark code for a single mapping"""
        
        code_lines = [
            '"""',
            f'Auto-generated PySpark code for mapping: {canonical.mapping_name}',
            f'Description: {canonical.mapping_description}',
            '"""',
            '',
            'from pyspark.sql import SparkSession, DataFrame',
            'from pyspark.sql import functions as F',
            'from pyspark.sql.types import *',
            '',
        ]
        
        # Add mapping class
        class_name = generate_class_name(canonical.mapping_name)
        code_lines.append(f'class {class_name}:')
        code_lines.append(f'    """Mapping: {canonical.mapping_name}"""')
        code_lines.append('')
        code_lines.append('    def __init__(self, spark: SparkSession):')
        code_lines.append('        self.spark = spark')
        code_lines.append('        self.logger = self._setup_logger()')
        code_lines.append('')
        
        # Add execute method
        code_lines.append('    def execute(self):')
        code_lines.append(f'        """Execute {canonical.mapping_name} mapping"""')
        code_lines.append('        try:')
        code_lines.append(f'            self.logger.info("Starting {canonical.mapping_name}")')
        code_lines.append('')
        
        # Load sources
        for source_name in canonical.sources:
            code_lines.extend(self._generate_source_load(source_name))
        
        # Execute transformations in order
        execution_order = self._get_execution_order(canonical)
        
        for step_id in execution_order:
            step = next((s for s in canonical.steps if s.id == step_id), None)
            if step:
                code_lines.extend(self._generate_transformation_code(step, canonical))
        
        # Save to targets
        for target_name in canonical.targets:
            code_lines.extend(self._generate_target_write(target_name))
        
        code_lines.append('            self.logger.info("Mapping completed successfully")')
        code_lines.append('        except Exception as e:')
        code_lines.append('            self.logger.error(f"Mapping failed: {str(e)}", exc_info=True)')
        code_lines.append('            raise')
        code_lines.append('')
        
        # Add helper methods
        code_lines.extend(self._generate_helper_methods())
        
        return '\n'.join(code_lines)
    
    def _generate_source_load(self, source_name: str) -> List[str]:
        """Generate code to load source data"""
        return [
            f'            # Load source: {source_name}',
            f'            df = self.spark.read.csv("/data/sources/{source_name.lower()}.csv", header=True, inferSchema=True)',
            f'            self.logger.info("Loaded source {source_name}: {{}} records".format(df.count()))',
            '',
        ]
    
    def _generate_transformation_code(self, step, canonical) -> List[str]:
        """Generate code for a transformation step"""
        code_lines = []
        
        # Determine input DataFrame based on dependencies
        input_df = self._get_input_dataframe(step, canonical)
        
        if step.type == 'Expression':
            code_lines.extend(self._generate_expression_step(step, input_df))
        elif step.type == 'Filter':
            code_lines.extend(self._generate_filter_step(step, input_df))
        elif step.type == 'Aggregator':
            code_lines.extend(self._generate_aggregator_step(step, input_df))
        elif step.type == 'Lookup Procedure':
            code_lines.extend(self._generate_lookup_step(step, input_df))
        elif step.type == 'Router':
            code_lines.extend(self._generate_router_step(step, input_df))
        elif step.type == 'Joiner':
            code_lines.extend(self._generate_joiner_step(step, input_df))
        elif step.type == 'Sequence':
            code_lines.extend(self._generate_sequence_step(step, input_df))
        elif step.type == 'Update Strategy':
            code_lines.extend(self._generate_update_strategy_step(step, input_df))
        elif step.type == 'Source Qualifier':
            code_lines.extend(self._generate_source_qualifier_step(step))
            code_lines.append('')
        
        return code_lines
    
    def _get_input_dataframe(self, step, canonical) -> str:
        """Get the input DataFrame variable name for a step"""
        dependencies = canonical.dependencies.get(step.id, [])
        if dependencies:
            # Use the output of the last dependency
            last_dep = dependencies[-1]
            return f"df_{last_dep.lower()}"
        else:
            # No dependencies, use the main df (for source qualifier)
            return "df"
    
    def _generate_expression_step(self, step, input_df: str) -> List[str]:
        """Generate code for expression transformation"""
        code_lines = [
            f'            # Expression: {step.name}',
        ]
        
        if step.expression_logic:
            code_lines.append(f'            df_{step.name.lower()} = {input_df}.selectExpr(')
            
            selects = []
            for field, expr in step.expression_logic.items():
                transformed_expr = self.expr_transformer.transform(expr)
                selects.append(f'                "{transformed_expr} as {field}"')
            
            code_lines.append(',\n'.join(selects))
            code_lines.append('            )')
        else:
            code_lines.append(f'            df_{step.name.lower()} = {input_df}')
        
        code_lines.append('')
        return code_lines
    
    def _generate_filter_step(self, step, input_df: str) -> List[str]:
        """Generate code for filter transformation"""
        code_lines = [
            f'            # Filter: {step.name}',
        ]
        
        if step.filter_condition:
            condition = self.expr_transformer.transform(step.filter_condition)
            code_lines.append(f'            df_{step.name.lower()} = {input_df}.filter("{condition}")')
        else:
            code_lines.append(f'            df_{step.name.lower()} = {input_df}')
        
        code_lines.append('')
        return code_lines
    
    def _generate_aggregator_step(self, step, input_df: str) -> List[str]:
        """Generate code for aggregator transformation"""
        code_lines = [
            f'            # Aggregator: {step.name}',
        ]
        
        if step.group_by_fields or step.aggregations:
            code = self.agg_transformer.create_aggregation_code(
                input_df,
                step.group_by_fields or [],
                step.aggregations or {}
            )
            code_lines.append('            ' + code.replace('\n', '\n            '))
        
        code_lines.append('')
        return code_lines
    
    def _generate_lookup_step(self, step, input_df: str) -> List[str]:
        """Generate code for lookup transformation"""
        code_lines = [
            f'            # Lookup: {step.name}',
            f'            # Lookup table: {step.lookup_table}',
            f'            # Condition: {step.lookup_condition}',
            f'            df_{step.name.lower()} = {input_df}  # Lookup logic here',
            '',
        ]
        return code_lines
    
    def _generate_router_step(self, step, input_df: str) -> List[str]:
        """Generate code for router transformation"""
        code_lines = [
            f'            # Router: {step.name}',
        ]
        
        router_groups = step.additional_params.get('router_groups', [])
        if router_groups:
            for group in router_groups:
                group_name = group.get('name', 'DEFAULT')
                expression = group.get('expression', 'TRUE')
                group_type = group.get('type', 'OUTPUT')
                
                if group_type == 'OUTPUT':
                    condition = self.expr_transformer.transform(expression)
                    code_lines.append(f'            df_{step.name.lower()}_{group_name.lower()} = {input_df}.filter("{condition}")')
        
        code_lines.append('')
        return code_lines
    
    def _generate_joiner_step(self, step, input_df: str) -> List[str]:
        """Generate code for joiner transformation"""
        code_lines = [
            f'            # Joiner: {step.name}',
        ]
        
        join_type = step.additional_params.get('join_type', 'inner')
        join_condition = step.additional_params.get('join_condition', '')
        
        # Map Informatica join types to PySpark
        join_type_mapping = {
            'Normal': 'inner',
            'Master Outer': 'left_outer',
            'Detail Outer': 'right_outer',
            'Full Outer': 'outer'
        }
        
        pyspark_join_type = join_type_mapping.get(join_type, 'inner')
        
        if join_condition:
            # This is a simplified join - in practice, we'd need to identify the two input DataFrames
            code_lines.append(f'            # Join type: {pyspark_join_type}, Condition: {join_condition}')
            code_lines.append(f'            df_{step.name.lower()} = {input_df}.join(df_other, "{join_condition}", "{pyspark_join_type}")')
        else:
            code_lines.append(f'            df_{step.name.lower()} = {input_df}  # Join logic here')
        
        code_lines.append('')
        return code_lines
    
    def _generate_sequence_step(self, step, input_df: str) -> List[str]:
        """Generate code for sequence transformation"""
        code_lines = [
            f'            # Sequence: {step.name}',
        ]
        
        start_value = step.additional_params.get('start_value', '1')
        increment_by = step.additional_params.get('increment_by', '1')
        
        # Add a monotonically increasing ID column
        code_lines.append(f'            df_{step.name.lower()} = {input_df}.withColumn("surrogate_key", F.monotonically_increasing_id() + {start_value})')
        
        code_lines.append('')
        return code_lines
    
    def _generate_update_strategy_step(self, step, input_df: str) -> List[str]:
        """Generate code for update strategy transformation"""
        code_lines = [
            f'            # Update Strategy: {step.name}',
        ]
        
        update_expression = step.additional_params.get('update_strategy_expression', '')
        
        if update_expression:
            # Map Informatica update strategies to PySpark operations
            if 'DD_UPDATE' in update_expression:
                code_lines.append(f'            # Update strategy: {update_expression}')
                code_lines.append(f'            df_{step.name.lower()} = {input_df}  # Update logic here')
            elif 'DD_INSERT' in update_expression:
                code_lines.append(f'            # Insert strategy: {update_expression}')
                code_lines.append(f'            df_{step.name.lower()} = {input_df}  # Insert logic here')
            else:
                code_lines.append(f'            df_{step.name.lower()} = {input_df}  # Update strategy: {update_expression}')
        else:
            code_lines.append(f'            df_{step.name.lower()} = {input_df}')
        
        code_lines.append('')
        return code_lines
    
    def _generate_source_qualifier_step(self, step) -> List[str]:
        """Generate code for source qualifier transformation"""
        code_lines = [
            f'            # Source Qualifier: {step.name}',
        ]
        
        # Load the source data
        source_name = "CUSTOMER_DATA"  # This should be extracted from the transformation
        code_lines.append(f'            df_{step.name.lower()} = self.spark.read.csv("/data/sources/{source_name.lower()}.csv", header=True, inferSchema=True)')
        code_lines.append(f'            self.logger.info("Loaded source {source_name}: {{}} records".format(df_{step.name.lower()}.count()))')
        
        code_lines.append('')
        return code_lines
    
    def _generate_target_write(self, target_name: str) -> List[str]:
        """Generate code to write to target"""
        return [
            f'            # Write to target: {target_name}',
            f'            df_target.write.mode("overwrite").parquet("/data/targets/{target_name.lower()}")',
            f'            self.logger.info("Written to target {target_name}")',
            '',
        ]
    
    def _generate_helper_methods(self) -> List[str]:
        """Generate helper methods"""
        return [
            '    def _setup_logger(self):',
            '        import logging',
            '        logger = logging.getLogger(self.__class__.__name__)',
            '        return logger',
        ]
    
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
    
    def _generate_main_code(self, canonical_models, all_code: List[str]) -> str:
        """Generate main orchestration code"""
        code_lines = [
            '"""',
            'Main PySpark Job Orchestrator',
            'Auto-generated from Informatica mappings',
            '"""',
            '',
            'from pyspark.sql import SparkSession',
            'import logging',
            '',
            'def setup_spark_session() -> SparkSession:',
            '    """Setup Spark session"""',
            '    spark = SparkSession.builder \\',
            '        .appName("Informatica_Migration") \\',
            '        .config("spark.sql.adaptive.enabled", "true") \\',
            '        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\',
            '        .getOrCreate()',
            '    return spark',
            '',
            'def main():',
            '    """Main entry point"""',
            '    logger = logging.getLogger(__name__)',
            '    logger.info("Starting Informatica migration job")',
            '    ',
            '    spark = setup_spark_session()',
            '    ',
            f'    # Execute {len(canonical_models)} mapping(s)',
        ]
        
        for canonical in canonical_models:
            class_name = generate_class_name(canonical.mapping_name)
            code_lines.append(f'    # Mapping: {canonical.mapping_name}')
            code_lines.append(f'    # {class_name}(spark).execute()')
            code_lines.append('')
        
        code_lines.extend([
            '    logger.info("Job completed successfully")',
            '    spark.stop()',
            '',
            'if __name__ == "__main__":',
            '    main()',
        ])
        
        return '\n'.join(code_lines)
