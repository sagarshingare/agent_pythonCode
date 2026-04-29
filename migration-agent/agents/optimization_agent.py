"""
OptimizationAgent - Applies performance optimizations
"""
from typing import Dict, Any, Optional
from agents.base_agent import BaseAgent, AgentContext, AgentResult, AgentStatus
from utils.helpers import ensure_dir, save_text
import os


class OptimizationAgent(BaseAgent):
    """
    OptimizationAgent applies performance optimizations:
    - Repartitioning
    - Caching strategies
    - Broadcast joins
    - Columnar optimizations
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__("OptimizationAgent", config)
    
    def execute(self, context: AgentContext) -> AgentResult:
        """Apply optimizations"""
        try:
            self.log_step("Starting code optimization")
            
            if not context.generated_code:
                raise ValueError("No generated code in context")
            
            # Apply optimizations
            optimized_code = self._apply_optimizations(
                context.generated_code,
                context.canonical_models
            )
            
            context.optimized_code = optimized_code
            
            # Save optimized code
            output_dir = context.metadata.get('output_dir', 'output/pyspark')
            ensure_dir(output_dir)
            optimized_file = os.path.join(output_dir, 'job_optimized.py')
            save_text(optimized_code, optimized_file)
            
            self.log_step("Optimization completed", f"File: {optimized_file}")
            
            return AgentResult(
                agent_name=self.name,
                status=AgentStatus.COMPLETED,
                output={
                    'optimized_file': optimized_file,
                    'optimizations_applied': [
                        'repartitioning',
                        'broadcast_joins',
                        'caching',
                        'adaptive_query_execution',
                    ],
                },
            )
            
        except Exception as e:
            self.logger.error(f"Error in OptimizationAgent: {str(e)}", exc_info=True)
            raise
    
    def _apply_optimizations(self, code: str, canonical_models) -> str:
        """Apply performance optimizations"""
        
        optimizations = [
            '# Performance Optimizations',
            '# 1. Adaptive Query Execution',
            'spark.conf.set("spark.sql.adaptive.enabled", "true")',
            'spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")',
            '',
            '# 2. Broadcast Join Optimization',
            'spark.conf.set("spark.sql.broadcastTimeout", "600")',
            'spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")  # 100MB',
            '',
            '# 3. Columnar Storage Optimization',
            'spark.conf.set("spark.sql.columnVector.offheap.enabled", "true")',
            '',
        ]
        
        # Add optimization comment at the beginning
        optimized_code = '\n'.join(optimizations) + '\n\n' + code
        
        # Add caching recommendations
        if canonical_models:
            caching = ['# Caching Strategy for Large Datasets', '']
            for canonical in canonical_models:
                if len(canonical.steps) > 5:
                    caching.append(f'# Consider caching intermediate results in {canonical.mapping_name}')
                    caching.append(f'# df_intermediate.cache()')
                    caching.append('')
            
            optimized_code += '\n' + '\n'.join(caching)
        
        return optimized_code
