"""
Aggregator transformer - handles aggregation transformations
"""
from typing import Dict, List


class AggregatorTransformer:
    """Convert Informatica aggregations to PySpark"""
    
    def __init__(self):
        pass
    
    def create_aggregation_code(
        self,
        df: str,
        group_by_fields: List[str],
        aggregations: Dict[str, str]
    ) -> str:
        """Generate PySpark aggregation code"""
        
        if not group_by_fields:
            # No grouping - simple aggregation
            return self._create_simple_aggregation(df, aggregations)
        else:
            # With grouping
            return self._create_group_aggregation(df, group_by_fields, aggregations)
    
    def _create_simple_aggregation(self, df: str, aggregations: Dict[str, str]) -> str:
        """Generate code for aggregation without grouping"""
        agg_exprs = []
        
        for output_field, agg_func in aggregations.items():
            # Parse aggregation function (e.g., "COUNT(SSN)" -> COUNT, SSN)
            if '(' in agg_func:
                func_name = agg_func[:agg_func.index('(')].upper()
                input_field = agg_func[agg_func.index('(') + 1:agg_func.rindex(')')].strip()
                
                pyspark_func = {
                    'COUNT': 'count',
                    'SUM': 'sum',
                    'AVG': 'avg',
                    'MIN': 'min',
                    'MAX': 'max',
                    'STDDEV': 'stddev',
                    'VARIANCE': 'variance',
                }.get(func_name, 'count')
                
                agg_exprs.append(f'F.{pyspark_func}("{input_field}").alias("{output_field}")')
        
        code = f"""# Simple Aggregation
{df}_agg = {df}.agg(
    {', '.join(agg_exprs)}
)
"""
        return code
    
    def _create_group_aggregation(
        self,
        df: str,
        group_by_fields: List[str],
        aggregations: Dict[str, str]
    ) -> str:
        """Generate code for aggregation with grouping"""
        
        group_fields_str = ", ".join([f'"{f}"' for f in group_by_fields])
        
        agg_exprs = []
        for output_field, agg_func in aggregations.items():
            if '(' in agg_func:
                func_name = agg_func[:agg_func.index('(')].upper()
                input_field = agg_func[agg_func.index('(') + 1:agg_func.rindex(')')].strip()
                
                pyspark_func = {
                    'COUNT': 'count',
                    'SUM': 'sum',
                    'AVG': 'avg',
                    'MIN': 'min',
                    'MAX': 'max',
                    'STDDEV': 'stddev',
                    'VARIANCE': 'variance',
                }.get(func_name, 'count')
                
                agg_exprs.append(f'F.{pyspark_func}("{input_field}").alias("{output_field}")')
        
        code = f"""# Aggregation with Grouping
{df}_agg = {df}.groupBy({group_fields_str}).agg(
    {', '.join(agg_exprs)}
)
"""
        return code
