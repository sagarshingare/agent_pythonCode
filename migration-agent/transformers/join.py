"""
Join transformer - handles join transformations
"""
from typing import Dict, List, Any, Optional


class JoinTransformer:
    """Convert Informatica joins to PySpark joins"""
    
    def __init__(self):
        pass
    
    def create_join_code(
        self,
        left_df: str,
        right_df: str,
        join_type: str,
        on_conditions: List[tuple],
        select_fields: Optional[List[str]] = None
    ) -> str:
        """Generate PySpark join code"""
        
        join_type_map = {
            'inner': 'inner',
            'left': 'left',
            'right': 'right',
            'outer': 'outer',
            'full': 'outer',
            'left_outer': 'left',
            'right_outer': 'right',
            'full_outer': 'outer',
        }
        
        pyspark_join_type = join_type_map.get(join_type.lower(), 'inner')
        
        # Build join condition
        if len(on_conditions) == 1:
            left_col, right_col = on_conditions[0]
            condition = f"({left_df}.{left_col} == {right_df}.{right_col})"
        else:
            conditions = [
                f"({left_df}.{lc} == {right_df}.{rc})"
                for lc, rc in on_conditions
            ]
            condition = " & ".join(conditions)
        
        # Generate code
        code = f"""# Join: {left_df} with {right_df}
{left_df}_joined = {left_df}.join(
    {right_df},
    on={condition},
    how='{pyspark_join_type}'
)
"""
        
        # Add select if specified
        if select_fields:
            select_list = ", ".join([f'"{f}"' for f in select_fields])
            code += f"{left_df}_joined = {left_df}_joined.select({select_list})\n"
        
        return code
